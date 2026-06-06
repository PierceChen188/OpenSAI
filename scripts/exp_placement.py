"""Qualitative placement experiment on a complex DAG that genuinely needs smart
placement. Shape (collaborative-perception-like), all nodes UNPINNED:

    Sensor1 ┐                 ┌ (5,6 -> 7)
    Sensor2 ┴─> Fuse12 (5) ─┐ │
    Sensor3 ┐               ├─┴─> Decide (7)
    Sensor4 ┴─> Fuse34 (6) ─┘

Edges (pred->node) = potential transfers. A cross-device edge = a real data-plane
transfer. We run the SAME DAG under smart vs random placement on 3 devices and
compare: cross-device transfers (lower=better), fan-in locality, sensor spread.
Also a correctness/bug check (every node must finish with a valid result).

Run:  .venv/Scripts/python.exe scripts/exp_placement.py [runs_per_mode]
"""
import collections
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

RUNS = int(sys.argv[1]) if len(sys.argv) > 1 else 5
EDGES = [(1, 5), (2, 5), (3, 6), (4, 6), (5, 7), (6, 7)]   # pred -> node
SENSORS = [1, 2, 3, 4]
FANIN_INPUTS = {5: [1, 2], 6: [3, 4], 7: [5, 6]}           # node -> its input nodes


def build_dag(idx):
    n = inject_collab._node
    nodes = [
        n(1, 'Sensor1', 'Algorithm_ProduceBlob', [], [5], -1, {'size_mb': 1}),
        n(2, 'Sensor2', 'Algorithm_ProduceBlob', [], [5], -1, {'size_mb': 1}),
        n(3, 'Sensor3', 'Algorithm_ProduceBlob', [], [6], -1, {'size_mb': 1}),
        n(4, 'Sensor4', 'Algorithm_ProduceBlob', [], [6], -1, {'size_mb': 1}),
        n(5, 'Fuse12', 'Algorithm_Relay', [1, 2], [7], -1, {'size_mb': 1}),
        n(6, 'Fuse34', 'Algorithm_Relay', [3, 4], [7], -1, {'size_mb': 1}),
        n(7, 'Decide', 'Algorithm_ConsumeBlob', [5, 6], [], -1, {}),
    ]
    return {'dagData': {'taskName': f'exp{idx}', 'taskId': '1', 'taskDesc': '',
                        'taskCreateTime': time.time(), 'taskPolicy': 'p', 'nodes': nodes}}


def analyze(res, labels):
    nodes = res.get('nodes', [])
    dev = {}
    errors = []
    for nd in nodes:
        nid = nd.get('id')
        d = nd.get('result', {}).get('deviceId')
        if d is not None and d not in labels:
            labels[d] = f"D{len(labels) + 1}"
        dev[nid] = labels.get(d, '?')
        info = str(nd.get('ResultInfo', ''))
        if (not info) or ('ERROR' in info) or ('MISSING' in info):
            errors.append((nd.get('name'), info[:50]))
    xfers = sum(1 for (p, c) in EDGES if dev.get(p) != dev.get(c))
    loc_hits = sum(1 for node, ins in FANIN_INPUTS.items()
                   if dev.get(node) in [dev.get(i) for i in ins])
    spread = len({dev.get(s) for s in SENSORS})
    return dev, xfers, loc_hits, len(FANIN_INPUTS), spread, errors


def run_mode(mode, r):
    print(f"\n################  PLACEMENT = {mode.upper()}  ################", flush=True)
    ts.kill_stale()
    t0 = time.time()
    while ts.port_in_use() and time.time() - t0 < 12:
        ts.kill_stale(); time.sleep(1)
    r.flushall()
    procs = [ts.start('server.py', {'OPENSAI_PLACEMENT': mode}, 'server')]
    time.sleep(3)
    procs.append(ts.start('device_client.py', {'OPENSAI_DATA_PORT': '9999'}, 'device1'))
    procs.append(ts.start('device_client.py', {'OPENSAI_DATA_PORT': '9998'}, 'device2'))
    procs.append(ts.start('device_client.py', {'OPENSAI_DATA_PORT': '9997'}, 'device3'))
    labels = {}
    stats = []
    try:
        t0 = time.time()
        while int(r.scard('DEVICE_ONLINE_SET')) < 3 and time.time() - t0 < 30:
            time.sleep(1)
        online = int(r.scard('DEVICE_ONLINE_SET'))
        time.sleep(3)
        print(f"[{mode}] devices online: {online}", flush=True)
        if online < 3:
            print(f"[{mode}] FAIL: need 3 devices"); return None
        for i in range(RUNS):
            res = inject_collab.submit(build_dag(i), timeout=60)
            if not res:
                print(f"  run {i+1}: (no result / timeout)"); continue
            dev, xfers, lh, lt, spread, errs = analyze(res, labels)
            stats.append((xfers, lh, spread, len(errs)))
            plac = " ".join(f"{nid}@{dev[nid]}" for nid in sorted(dev))
            flag = f" ERRORS={errs}" if errs else ""
            print(f"  run {i+1}: xfers={xfers}/6  fanin_local={lh}/{lt}  sensor_spread={spread}/3  | {plac}{flag}", flush=True)
    finally:
        for p in procs:
            ts.kill_tree(p)
        ts.kill_stale()
    return stats


def summarize(mode, stats):
    if not stats:
        print(f"[{mode}] no data"); return
    n = len(stats)
    avg = lambda i: sum(s[i] for s in stats) / n
    print(f"[{mode}]  avg cross-device transfers = {avg(0):.1f}/6   "
          f"avg fan-in locality = {avg(1):.1f}/3   "
          f"avg sensor spread = {avg(2):.1f}/3   "
          f"total errors = {sum(s[3] for s in stats)}", flush=True)


def main():
    import redis
    r = redis.Redis(host='127.0.0.1', port=6379, db=0)
    out = {}
    for mode in ('random', 'smart'):
        out[mode] = run_mode(mode, r)
    print("\n================  SUMMARY  ================", flush=True)
    for mode in ('random', 'smart'):
        summarize(mode, out.get(mode))
    print("\n(lower transfers = better; higher locality & spread = better; errors must be 0)", flush=True)
    return 0


if __name__ == '__main__':
    sys.exit(main())
