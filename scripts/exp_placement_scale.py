"""Scaled placement experiment: ~24-node hierarchical reduction DAG on 10 devices.

Topology (sensor-fusion tree): 12 sensors -> 6 -> 3 -> 2 -> 1  (24 nodes, 23 edges).
No-predecessor nodes = ProduceBlob; intermediate = Relay (consume+produce);
root = ConsumeBlob. All UNPINNED. Metric = makespan (finishedTime); also cross-device
transfers, distinct devices used, and per-device node spread. Bug check = errors.

Probing is set to a long interval (10 devices x 4MB probes would add noise).

Run:  .venv/Scripts/python.exe scripts/exp_placement_scale.py [trials]
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

TRIALS = int(sys.argv[1]) if len(sys.argv) > 1 else 2
N_DEVICES = 10
BASE_PORT = 9990          # devices use 9990..9999
N_SENSORS = 12
FAN = 2
REGIMES = {'balanced': (4, 100), 'network-heavy': (16, 50)}   # (blob_mb, compute_ms)


def topology():
    """Return (specs, edges). specs = [(id, kind)] kind in produce/relay/consume."""
    specs = {}
    edges = []
    nid = 0
    cur = []
    for _ in range(N_SENSORS):
        nid += 1
        specs[nid] = 'produce'
        cur.append(nid)
    before = {i: [] for i in specs}
    while len(cur) > 1:
        nxt = []
        for i in range(0, len(cur), FAN):
            group = cur[i:i + FAN]
            nid += 1
            specs[nid] = 'relay'
            before[nid] = list(group)
            for g in group:
                edges.append((g, nid))
            nxt.append(nid)
        cur = nxt
    specs[cur[0]] = 'consume'   # root is terminal
    after = {i: [] for i in specs}
    for (p, c) in edges:
        after[p].append(c)
    return specs, before, after, edges


SPECS, BEFORE, AFTER, EDGES = topology()
ALGO = {'produce': 'Algorithm_ProduceBlob', 'relay': 'Algorithm_Relay', 'consume': 'Algorithm_ConsumeBlob'}


def build_dag(idx, blob_mb, compute_ms):
    nodes = []
    for nid in sorted(SPECS):
        kind = SPECS[nid]
        p = {'compute_ms': compute_ms}
        if kind in ('produce', 'relay'):
            p['size_mb'] = blob_mb
        nodes.append(inject_collab._node(nid, f'N{nid}', ALGO[kind],
                                         BEFORE[nid], AFTER[nid], -1, p))
    return {'dagData': {'taskName': f's{idx}', 'taskId': '1', 'taskDesc': '',
                        'taskCreateTime': time.time(), 'taskPolicy': 'p', 'nodes': nodes}}


def analyze(res, blob_mb):
    nodes = res.get('nodes', [])
    dev = {nd.get('id'): nd.get('result', {}).get('deviceId') for nd in nodes}
    got = sum(1 for nd in nodes if str(nd.get('ResultInfo', '')))
    errs = sum(1 for nd in nodes
               if ('ERROR' in str(nd.get('ResultInfo', ''))) or ('MISSING' in str(nd.get('ResultInfo', ''))))
    errs += (len(SPECS) - got)   # missing results also count as problems
    xfers = sum(1 for (a, b) in EDGES if dev.get(a) != dev.get(b))
    distinct = len({d for d in dev.values() if d is not None})
    makespan = float(res.get('finishedTime') or 0.0)
    return makespan, xfers, distinct, errs


def run_stack(server_env, conditions):
    ts.kill_stale()
    t0 = time.time()
    while ts.port_in_use() and time.time() - t0 < 12:
        ts.kill_stale(); time.sleep(1)
    import redis
    r = redis.Redis(host='127.0.0.1', port=6379, db=0); r.flushall()
    procs = [ts.start('server.py', server_env, 'server')]
    time.sleep(3)
    for i in range(N_DEVICES):
        procs.append(ts.start('device_client.py',
                              {'OPENSAI_DATA_PORT': str(BASE_PORT + i),
                               'OPENSAI_PROBE_INTERVAL': '600'}, f'device{i + 1}'))
    out = {}
    try:
        t0 = time.time()
        while int(r.scard('DEVICE_ONLINE_SET')) < N_DEVICES and time.time() - t0 < 120:
            time.sleep(2)
        online = int(r.scard('DEVICE_ONLINE_SET'))
        time.sleep(5)
        print(f"  devices online: {online}/{N_DEVICES}", flush=True)
        if online < N_DEVICES:
            print("  FAIL: not all devices registered"); return out
        idx = 0
        for cname in conditions:
            blob_mb, compute_ms = REGIMES[cname]
            rows = []
            for _t in range(TRIALS):
                res = inject_collab.submit(build_dag(idx, blob_mb, compute_ms), timeout=240)
                idx += 1
                if res:
                    rows.append(analyze(res, blob_mb))
                else:
                    print(f"    {cname} trial {_t+1}: TIMEOUT/no result", flush=True)
            out[cname] = rows
    finally:
        for p in procs:
            ts.kill_tree(p)
        ts.kill_stale()
    return out


def avg(rows, i):
    return sum(x[i] for x in rows) / len(rows) if rows else float('nan')


def main():
    print(f"\nDAG: {len(SPECS)} nodes, {len(EDGES)} edges, {N_SENSORS} sensors, fan={FAN}; "
          f"{N_DEVICES} devices; {TRIALS} trials/cond", flush=True)
    modes = ('random', 'smart', 'plan')
    print(f"\n#### Phase A - random vs smart vs plan ####", flush=True)
    a = {}
    for mode in modes:
        print(f"\n  [stack] OPENSAI_PLACEMENT={mode}", flush=True)
        a[mode] = run_stack({'OPENSAI_PLACEMENT': mode}, list(REGIMES.keys()))
    print(f"\n  {'regime':<14}{'mode':<8}{'makespan(s)':>12}{'xfers/'+str(len(EDGES)):>11}{'devices_used':>13}{'errs':>6}", flush=True)
    for cname in REGIMES:
        for mode in modes:
            rows = a[mode].get(cname, [])
            print(f"  {cname:<14}{mode:<8}{avg(rows,0):>12.2f}{avg(rows,1):>11.1f}{avg(rows,2):>13.1f}{sum(x[3] for x in rows):>6}", flush=True)
        rs = a['random'].get(cname, [])
        for mode in ('smart', 'plan'):
            ms = a[mode].get(cname, [])
            if rs and ms and avg(rs, 0) > 0:
                print(f"  {'-> ' + mode + ' makespan vs random:':<30}{(avg(ms,0)/avg(rs,0)-1)*100:+.0f}%", flush=True)

    print(f"\n#### Phase B - cluster (W_LOAD=0) vs spread (W_LOAD=50), network-heavy ####", flush=True)
    b = {}
    for wl in ('0', '50'):
        tag = 'cluster' if wl == '0' else 'spread'
        print(f"\n  [stack] smart, OPENSAI_W_LOAD={wl} ({tag})", flush=True)
        b[tag] = run_stack({'OPENSAI_PLACEMENT': 'smart', 'OPENSAI_W_LOAD': wl}, ['network-heavy'])
    print(f"\n  {'tuning':<9}{'makespan(s)':>12}{'xfers/'+str(len(EDGES)):>11}{'devices_used':>13}", flush=True)
    for tag in ('cluster', 'spread'):
        rows = b[tag].get('network-heavy', [])
        print(f"  {tag:<9}{avg(rows,0):>12.2f}{avg(rows,1):>11.1f}{avg(rows,2):>13.1f}", flush=True)
    print("\n(makespan lower=better; errors must be 0)", flush=True)
    return 0


if __name__ == '__main__':
    sys.exit(main())
