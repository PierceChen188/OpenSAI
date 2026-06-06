"""Quantitative placement experiment (T6.2) on the 7-node perception DAG, 3 devices.

Metric = DAG makespan (server-reported finishedTime). We also report cross-device
transfers and the bytes they move. CPU compute is a real busy-spin, so co-located
CPU tasks serialize (GIL) while spread tasks run truly parallel — this is what makes
"spread" pay off for compute and "cluster" pay off for network.

Phase A — effect: smart vs random across 3 regimes (compute-heavy / balanced /
          network-heavy).
Phase B — tuning: smart with W_LOAD=0 (cluster) vs W_LOAD=50 (spread), at the two
          extreme regimes, to locate the spread<->cluster crossover.

Run:  .venv/Scripts/python.exe scripts/exp_placement_quant.py [trials]
"""
import os
import sys
import time

try:
    sys.stdout.reconfigure(encoding='utf-8')
except Exception:
    pass

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import test_suite as ts
import inject_collab

TRIALS = int(sys.argv[1]) if len(sys.argv) > 1 else 3
EDGES = [(1, 5), (2, 5), (3, 6), (4, 6), (5, 7), (6, 7)]
SENSORS = [1, 2, 3, 4]

# regime -> (blob_mb, compute_ms)
REGIMES = {
    'compute-heavy': (1, 800),    # 传输便宜、计算重 -> 分散(并行)应更优
    'balanced':      (8, 400),
    'network-heavy': (32, 100),   # 传输贵、计算轻 -> 聚合(少传输)应更优
}


def build_dag(idx, blob_mb, compute_ms):
    n = inject_collab._node
    p = {'size_mb': blob_mb, 'compute_ms': compute_ms}
    nodes = [
        n(1, 'Sensor1', 'Algorithm_ProduceBlob', [], [5], -1, dict(p)),
        n(2, 'Sensor2', 'Algorithm_ProduceBlob', [], [5], -1, dict(p)),
        n(3, 'Sensor3', 'Algorithm_ProduceBlob', [], [6], -1, dict(p)),
        n(4, 'Sensor4', 'Algorithm_ProduceBlob', [], [6], -1, dict(p)),
        n(5, 'Fuse12', 'Algorithm_Relay', [1, 2], [7], -1, dict(p)),
        n(6, 'Fuse34', 'Algorithm_Relay', [3, 4], [7], -1, dict(p)),
        n(7, 'Decide', 'Algorithm_ConsumeBlob', [5, 6], [], -1, {'compute_ms': compute_ms}),
    ]
    return {'dagData': {'taskName': f'q{idx}', 'taskId': '1', 'taskDesc': '',
                        'taskCreateTime': time.time(), 'taskPolicy': 'p', 'nodes': nodes}}


def analyze(res, blob_mb):
    nodes = res.get('nodes', [])
    dev = {nd.get('id'): nd.get('result', {}).get('deviceId') for nd in nodes}
    errs = sum(1 for nd in nodes
               if (not str(nd.get('ResultInfo', ''))) or ('ERROR' in str(nd.get('ResultInfo', '')))
               or ('MISSING' in str(nd.get('ResultInfo', ''))))
    xfers = sum(1 for (a, b) in EDGES if dev.get(a) != dev.get(b))
    spread = len({dev.get(s) for s in SENSORS})
    makespan = float(res.get('finishedTime') or 0.0)
    return makespan, xfers, xfers * blob_mb, spread, errs


def run_stack(server_env, conditions):
    """Start a 3-device stack with given server env, run each condition x TRIALS,
    return {cond_name: [(makespan, xfers, cross_MB, spread, errs), ...]}."""
    ts.kill_stale()
    t0 = time.time()
    while ts.port_in_use() and time.time() - t0 < 12:
        ts.kill_stale(); time.sleep(1)
    import redis
    r = redis.Redis(host='127.0.0.1', port=6379, db=0); r.flushall()
    procs = [ts.start('server.py', server_env, 'server')]
    time.sleep(3)
    procs.append(ts.start('device_client.py', {'OPENSAI_DATA_PORT': '9999'}, 'device1'))
    procs.append(ts.start('device_client.py', {'OPENSAI_DATA_PORT': '9998'}, 'device2'))
    procs.append(ts.start('device_client.py', {'OPENSAI_DATA_PORT': '9997'}, 'device3'))
    out = {}
    try:
        t0 = time.time()
        while int(r.scard('DEVICE_ONLINE_SET')) < 3 and time.time() - t0 < 30:
            time.sleep(1)
        time.sleep(3)
        if int(r.scard('DEVICE_ONLINE_SET')) < 3:
            print("  FAIL: need 3 devices"); return out
        idx = 0
        for cname in conditions:
            blob_mb, compute_ms = REGIMES[cname]
            rows = []
            for _t in range(TRIALS):
                res = inject_collab.submit(build_dag(idx, blob_mb, compute_ms), timeout=120)
                idx += 1
                if res:
                    rows.append(analyze(res, blob_mb))
            out[cname] = rows
    finally:
        for p in procs:
            ts.kill_tree(p)
        ts.kill_stale()
    return out


def avg(rows, i):
    return sum(x[i] for x in rows) / len(rows) if rows else float('nan')


def main():
    modes = ('random', 'smart', 'plan')   # smart=贪心自适应; plan=整图反向就近(前瞻)
    print(f"\n#### Phase A — effect: random vs smart vs plan ({TRIALS} trials/cond) ####", flush=True)
    a = {}
    for mode in modes:
        print(f"\n  [stack] OPENSAI_PLACEMENT={mode}", flush=True)
        a[mode] = run_stack({'OPENSAI_PLACEMENT': mode}, list(REGIMES.keys()))
    print(f"\n  {'regime':<14}{'mode':<8}{'makespan(s)':>12}{'transfers':>11}{'cross_MB':>10}{'spread':>8}{'errs':>6}", flush=True)
    for cname in REGIMES:
        for mode in modes:
            rows = a[mode].get(cname, [])
            print(f"  {cname:<14}{mode:<8}{avg(rows,0):>12.2f}{avg(rows,1):>11.1f}{avg(rows,2):>10.0f}{avg(rows,3):>8.1f}{sum(x[4] for x in rows):>6}", flush=True)
        rs = a['random'].get(cname, [])
        for mode in ('smart', 'plan'):
            ms = a[mode].get(cname, [])
            if rs and ms and avg(rs, 0) > 0:
                print(f"  {'-> ' + mode + ' makespan vs random:':<30}{(avg(ms,0)/avg(rs,0)-1)*100:+.0f}%", flush=True)

    print(f"\n#### Phase B — tuning: cluster (W_LOAD=0) vs spread (W_LOAD=50) ####", flush=True)
    conds_b = ['compute-heavy', 'network-heavy']
    b = {}
    for wl in ('0', '50'):
        tag = 'cluster' if wl == '0' else 'spread'
        print(f"\n  [stack] smart, OPENSAI_W_LOAD={wl} ({tag})", flush=True)
        b[tag] = run_stack({'OPENSAI_PLACEMENT': 'smart', 'OPENSAI_W_LOAD': wl}, conds_b)
    print(f"\n  {'regime':<14}{'tuning':<9}{'makespan(s)':>12}{'transfers':>11}{'spread':>8}", flush=True)
    for cname in conds_b:
        for tag in ('cluster', 'spread'):
            rows = b[tag].get(cname, [])
            print(f"  {cname:<14}{tag:<9}{avg(rows,0):>12.2f}{avg(rows,1):>11.1f}{avg(rows,3):>8.1f}", flush=True)
    print("\n(makespan lower=better. Expect: compute-heavy -> spread wins; network-heavy -> cluster wins.)", flush=True)
    return 0


if __name__ == '__main__':
    sys.exit(main())
