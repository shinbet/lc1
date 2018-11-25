import random
import threading
from chess import uci, Board
from chess.engine import EngineTerminatedException
from copy import deepcopy
import logging

from server import Server, ResultSaver

logging.basicConfig(level=logging.INFO)
log = logging.getLogger('client')

class EngineConf:
    __slots__ = 'eid, cmd options nodes get_train_data'.split()

    def __init__(self, eid, cmd, nodes, options=None, get_train_data=False):
        self.eid = eid
        self.cmd = cmd
        self.nodes = nodes
        self.options = options
        self.get_train_data = get_train_data


class TrainEngine(uci.Engine):
    '''add traindata uci command to engine'''

    def __init__(self, **kwargs):
        super(TrainEngine, self).__init__(**kwargs)

        self.training_data_received = threading.Condition()
        self.traindata_data = []

    def traindata(self, version='3', async_callback=None):
        def command():
            with self.semaphore:
                with self.training_data_received:
                    self.send_line("traindata version {}".format(version))
                    self.training_data_received.wait()

                if self.terminated.is_set():
                    raise EngineTerminatedException()

        return self._queue_command(command, async_callback)

    def _traindata(self, arg):
        #print 'got arg:', arg
        self.traindata_data.append(arg)
        with self.training_data_received:
            self.training_data_received.notify_all()


    def on_line_received(self, buf):
        command_and_args = buf.split(None, 1)
        #print '***', command_and_args

        if len(command_and_args) >= 1:
            if command_and_args[0] == "traindata":
                return self._traindata(command_and_args[1])

        return super(TrainEngine, self).on_line_received(buf)

    def on_terminated(self):
        with self.training_data_received:
            self.training_data_received.notify_all()
        super(TrainEngine, self).on_terminated()

def get_engine(engine_conf):
    e = uci.popen_engine(engine_conf.cmd, engine_cls=TrainEngine)
    e.uci()
    if engine_conf.options:
        e.setoption(engine_conf.options)
    e.isready()

    info_handler = uci.InfoHandler()
    e.info_handlers.append(info_handler)
    return e

def do_match(conf1, conf2, start_position=None):
    e1 = e2 = None
    try:
        e1 = get_engine(conf1)
        e2 = get_engine(conf2)

        e1.ucinewgame()
        e2.ucinewgame()

        board = Board()

        while not board.is_game_over():
            e,c = (e1,conf1) if board.turn else (e2, conf2)

            e.isready()
            e.position(board)
            r = e.go(nodes=c.nodes)
            if c.get_train_data:
                e.traindata()

            info = e.info_handlers[0].info
            log.info('%s score: %s, nps: %s, nodes: %s, time: %s', c.eid, info.get('score'), info.get('nps'), info.get('nodes'), info.get('time'))
            board.push(r.bestmove)
        #print board
        log.info("%s vs %s: %s", conf1.eid, conf2.eid, board.result())

        res = board.result()
        if res == '1-0':
            res = 1
        elif res == '0-1':
            res = -1
        else:
            res = 0
    finally:
        e1.quit()
        e2.quit()

    traindata = fix_traindata(e1.traindata_data, res)
    traindata.extend(fix_traindata(e2.traindata_data, -res))

    return {'result': res, 'traindata': traindata}

def fix_traindata(l, res, version='3'):
    ret = []
    if res == -1:
        res = 255 # 2's complemet
    for data in l:
        # hex encoded
        ba = bytearray.fromhex(data)
        if len(ba) != 8276:
            raise Exception('wrong size for training data: ' + str(len(ba)))
        # wdl is last byte
        ba[-1] = res
        ret.append(ba)
    return ret

loptions = {
    'Threads': 1,
    'Ponder': False,
    'Temperature': '0.8',
    'SmartPruningFactor': '0.0',
    'OutOfOrderEval': False,
    'TempDecayMoves': 20,
    # 'CPuct': '3.4',
    'MaxCollisionEvents': 1,
    # 'FpuReduction': '1.2',
    # 'TempVisitOffset': '0.0',
    #'WeightsFile': '/Users/sbentov/lc0/10593.w',
    # 'PolicyTemperature': '2.2',
    'DirichletNoise': True,

}

static_config = {
    'engines': {
        'sf9': dict(cmd="/Users/sbentov/stockfish-9-mac/Mac/stockfish-9-bmi2", options={'Threads':2}),
        'sf9_s1': dict(cmd="/Users/sbentov/stockfish-9-mac/Mac/stockfish-9-bmi2", options={'Threads': 2, 'Skill': 1}),
        'lc0': dict(cmd='/Users/sbentov/lc0/build/release/lc0', options=loptions, get_train_data=False),
    },
    'net_path': '/Users/sbentov/PycharmProjects/lczero-training/'
}

class Client(threading.Thread):
    def __init__(self, server):
        self.server = server
        super(Client, self).__init__()

    def run(self):
        self.transient_config = self.server.get_configs()
        lc0 = self.get_engine('lc0')
        n = 0
        try:
            while 1:
                eid = self.get_next_opponent()
                e = self.get_engine(eid)
                bw = random.choice([1, -1])
                e1, e2 = (lc0, e) if bw == 1 else (e, lc0)
                res = do_match(e1, e2)
                self.server.add_results([{'result': res['result'] * bw,
                                          'nodes': e.nodes, 'eid': eid,
                                          'netid': lc0.options['WeightsFile'].rsplit('/', 1)[-1],
                                          'traindata': res['traindata']}])
                # FIXME: too often
                self.transient_config = self.server.get_configs()
                n += 1
        except KeyboardInterrupt:
            pass

    def get_engine(self, eid):
        conf = deepcopy(static_config['engines'][eid])
        t_c = self.transient_config[eid]
        conf['nodes'] = t_c['nodes']
        conf['options'].update(t_c.get('options', {}))
        net = t_c.get('net')
        if net:
            conf['options'].update(WeightsFile=static_config['net_path'] + net)
        return EngineConf(eid=eid, **conf)

    def get_next_opponent(self):
        # sigh
        options = [eid
                   for eid,conf in self.transient_config.items() if eid in static_config['engines']
                   for _ in range(conf['rate'])]
        return random.choice(options)

def main():
    s = Server(ResultSaver('run_1'))

    threads = [Client(s).start() for _ in range(4)]
    threads[0].join()

if __name__ == '__main__':
    main()
