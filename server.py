import gzip
import json
import math

import sqlite3


transient_config = {
    'sf9': {'nodes':3200, 'rate':1},
    'sf9_s1': {'nodes':20000, 'rate':1},
    #'lc0': {'nodes': 400, 'weight':0.0, 'options': {'WeightsFile': '/Users/sbentov/lc0/10593.w'}}
    'lc0': {'nodes': 800, 'rate': 0, 'net': 'scs-64x8-run1-550000.pb.gz', 'options': {}}
}

class ResultSaver:
    def __init__(self, name):
        self.name = name
        self.conn = sqlite3.connect(name + '.db', check_same_thread=False)
        self.conn.isolation_level = None
        self.create_schema()

    def create_schema(self):
        self.conn.execute("""
CREATE TABLE IF NOT EXISTS results (
  gameid INTEGER PRIMARY KEY ASC,
  runid TEXT,
  netid TEXT,
  opponent_id TEXT,
  opponent_nodes INTEGER,
  result INTEGER)
""")

        self.conn.execute("""
    CREATE TABLE IF NOT EXISTS run_conf (
      runid TEXT UNIQUE,
      config TEXT,
      version INTEGER
      )
    """)

    def add(self, result):
        cur = self.conn.cursor()
        cur.execute("INSERT INTO results (runid, netid,opponent_id,opponent_nodes,result) VALUES (?, ?, ?, ?, ?)", (self.name, result['netid'], result['eid'], result['nodes'], result['result']))
        game_id = cur.lastrowid
        cur.close()
        data = result.get('traindata')
        if data:
            with gzip.open('{}_{}.gz'.format(self.name, game_id), 'wb') as f:
                for chunk in data:
                    f.write(buffer(chunk))

        return game_id

    def get_last(self, n):
        cur = self.conn.cursor()
        cur.execute('SELECT opponent_id, result, opponent_nodes FROM results where runid=? ORDER BY gameid DESC LIMIT {}'.format(n), (self.name,))
        res = cur.fetchall()
        cur.close()
        return reversed(res)

    def save_config(self, conf):
        cur = self.conn.cursor()
        data = json.dumps(conf)
        cur.execute('INSERT INTO run_conf VALUES(?, ?, 0) ON CONFLICT(runid) DO UPDATE SET config=?, version=version+1', (self.name, data, data))
        cur.close()

    def load_config(self):
        conf = None
        cur = self.conn.cursor()
        cur.execute('SELECT config from run_conf where runid=?', (self.name,))
        res = cur.fetchall()
        if res:
            conf = json.loads(res[0][0])
        cur.close()
        return conf


class Server:
    def __init__(self, saver):
        self.saver = saver
        self.load_prev_res()
        self.last_rebalance = 0

        conf = saver.load_config()
        if conf:
            global transient_config
            transient_config = conf

    def load_prev_res(self):
        self.running_avg = {}
        rows = self.saver.get_last(100)
        for r in rows:
            self.add_to_ra(*r)

    def add_to_ra(self, eid, result, nodes):
        # need lock
        cur = self.running_avg.get(eid, 0.0)
        newcur = cur * 0.95 + result * 0.05
        self.running_avg[eid] = newcur
        print eid, result, cur, newcur

    def rebalance(self):
        self.last_rebalance = 0
        changed = False
        for k, v in self.running_avg.items():
            if math.fabs(v) > 0.2:
                direction = math.copysign(1.1, v) # either 1.1 or -1.1
                transient_config[k]['nodes'] = int(transient_config[k]['nodes']*direction)
                print 'changed', k, 'nodes to', transient_config[k]['nodes'], v, direction
                changed = True
        if changed:
            self.saver.save_config(transient_config)

    def get_configs(self):
        return transient_config.copy()

    def add_results(self, results):
        for r in results:
            gid = self.saver.add(r)
            self.add_to_ra(r['eid'], r['result'], r['nodes'])
        self.last_rebalance += len(results)
        if self.last_rebalance >= 1:
            self.rebalance()

def main():
    pass
    # make http server

if __name__ == '__main__':
    main()
