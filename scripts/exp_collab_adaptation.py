"""Big collaborative-perception experiment: does the ADAPTATION-OPTIMIZATION component help?

A large, INTERTWINED lattice DAG (not a clean tree): 16 sensors -> 16 per-sensor extracts
-> 8 regional fusions (overlapping windows of 3, stride 2) -> 4 cross-region fusions
(overlapping) -> 1 global head (all cross + skip links from regionals). ~45 nodes, ~58
edges, with shared/overlapping dependencies and skip connections -> dense and intertwined.

We compare, end to end, on the SAME DAG:
  OFF  : OPENSAI_PLACEMENT=random         (no adaptation -- scatter tasks at random)
  smart: OPENSAI_PLACEMENT=smart          (greedy locality+resource-aware)
  ON   : OPENSAI_PLACEMENT=plan + AUTO_MEM (whole-DAG lookahead placement + resource-aware)

Two regimes stress different levers:
  network-heavy : big feature blobs (8MB), light compute -> LOCALITY (avoid transfers) matters
  compute-heavy : tiny blobs (1MB), heavy compute (600ms) -> SPREADING across devices matters

Metrics per run: makespan (end-to-end finishedTime, lower=better), cross-device transfers
(of ~58 edges), distinct devices used, max nodes on any one device (balance), errors
(must be 0). All nodes UNPINNED so placement is fully the brain's decision.

Run:  .venv/Scripts/python.exe scripts/exp_collab_adaptation.py [trials]
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
import test_suite as ts          # noqa: E402
import inject_collab             # noqa: E402

TRIALS = int(sys.argv[1]) if len(sys.argv) > 1 else 2
N_DEVICES = 10
BASE_PORT = 9990
N_SENSORS = 16
REGIMES = {'network-heavy': (8, 60), 'compute-heavy': (1, 600)}   # (blob_mb, compute_ms)


def topology():
    """Intertwined lattice: sensors -> extracts -> regional(overlap) -> cross(overlap) -> global(+skip)."""
    specs, before = {}, {}
    nid = 0
    S = []
    for _ in range(N_SENSORS):
        nid += 1; specs[nid] = 'produce'; before[nid] = []; S.append(nid)
    E = []
    for s in S:
        nid += 1; specs[nid] = 'relay'; before[nid] = [s]; E.append(nid)
    R = []                                    # regional fusion: overlapping windows of 3, stride 2
    i = 0
    while i < len(E):
        win = E[i:i + 3] if i + 3 <= len(E) else E[i:]
        if len(win) < 2 and R:
            before[R[-1]] = list(dict.fromkeys(before[R[-1]] + win)); break
        nid += 1; specs[nid] = 'relay'; before[nid] = list(win); R.append(nid)
        i += 2
    C = []                                    # cross-region fusion: overlapping windows of 3, stride 2
    i = 0
    while i < len(R):
        win = R[i:i + 3] if i + 3 <= len(R) else R[i:]
        if len(win) < 2 and C:
            before[C[-1]] = list(dict.fromkeys(before[C[-1]] + win)); break
        nid += 1; specs[nid] = 'relay'; before[nid] = list(win); C.append(nid)
        i += 2
    nid += 1; specs[nid] = 'consume'          # global head: all cross + skip links from regionals
    skip = [R[0], R[len(R) // 2]] if len(R) >= 2 else R[:1]
    before[nid] = list(dict.fromkeys(C + skip))
    edges, after = [], {k: [] for k in specs}
    for c, ps in before.items():
        for p in ps:
            edges.append((p, c)); after[p].append(c)
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
        nodes.append(inject_collab._node(nid, f'N{nid}', ALGO[kind], BEFORE[nid], AFTER[nid], -1, p))
    return {'dagData': {'taskName': f'c{idx}', 'taskId': '1', 'taskDesc': '',
                        'taskCreateTime': time.time(), 'taskPolicy': 'p', 'nodes': nodes}}


def analyze(res):
    nodes = res.get('nodes', [])
    dev = {nd.get('id'): nd.get('result', {}).get('deviceId') for nd in nodes}
    got = sum(1 for nd in nodes if str(nd.get('ResultInfo', '')))
    errs = sum(1 for nd in nodes if ('ERROR' in str(nd.get('ResultInfo', '')))
               or ('MISSING' in str(nd.get('ResultInfo', ''))))
    errs += (len(SPECS) - got)
    xfers = sum(1 for (a, b) in EDGES if dev.get(a) != dev.get(b))
    used = [d for d in dev.values() if d is not None]
    distinct = len(set(used))
    max_load = max((used.count(d) for d in set(used)), default=0)
    makespan = float(res.get('finishedTime') or 0.0)
    return makespan, xfers, distinct, errs, max_load


def run_stack(server_env):
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
                               'OPENSAI_PROBE_INTERVAL': '600',
                               'OPENSAI_TASK_WORKERS': '3',
                               'OPENSAI_POOL_WARMUP_S': '45'}, f'device{i + 1}'))
    out = {}
    try:
        t0 = time.time()
        while int(r.scard('DEVICE_ONLINE_SET')) < N_DEVICES and time.time() - t0 < 150:
            time.sleep(2)
        online = int(r.scard('DEVICE_ONLINE_SET'))
        time.sleep(5)
        print(f"  devices online: {online}/{N_DEVICES}", flush=True)
        if online < N_DEVICES:
            print("  FAIL: not all devices registered"); return out
        idx = 0
        for cname, (blob_mb, compute_ms) in REGIMES.items():
            rows = []
            for _t in range(TRIALS):
                res = inject_collab.submit(build_dag(idx, blob_mb, compute_ms), timeout=300)
                idx += 1
                if res:
                    rows.append(analyze(res))
                else:
                    print(f"    {cname} trial {_t + 1}: TIMEOUT/no result", flush=True)
            out[cname] = rows
    finally:
        for p in procs:
            ts.kill_tree(p)
        ts.kill_stale()
    return out


def avg(rows, i):
    return sum(x[i] for x in rows) / len(rows) if rows else float('nan')


def main():
    print(f"\nDAG: {len(SPECS)} nodes, {len(EDGES)} edges (intertwined lattice), {N_SENSORS} sensors; "
          f"{N_DEVICES} devices x3 slots; {TRIALS} trials/regime", flush=True)
    stacks = {
        'OFF(random)':  {'OPENSAI_PLACEMENT': 'random'},
        'smart':        {'OPENSAI_PLACEMENT': 'smart'},
        'ON(plan)':     {'OPENSAI_PLACEMENT': 'plan', 'OPENSAI_AUTO_MEM': '1'},
    }
    data = {}
    for label, env in stacks.items():
        print(f"\n  [stack] {label}  env={env}", flush=True)
        data[label] = run_stack(env)

    print(f"\n{'='*78}\n  RESULTS  (makespan lower=better; xfers/{len(EDGES)}; errs must be 0)\n{'='*78}", flush=True)
    hdr = f"  {'regime':<15}{'config':<14}{'makespan(s)':>12}{'xfers':>7}{'devs':>6}{'maxload':>8}{'errs':>6}"
    for cname in REGIMES:
        print("\n" + hdr, flush=True)
        for label in stacks:
            rows = data[label].get(cname, [])
            print(f"  {cname:<15}{label:<14}{avg(rows,0):>12.2f}{avg(rows,1):>7.1f}"
                  f"{avg(rows,2):>6.1f}{avg(rows,4):>8.1f}{sum(x[3] for x in rows):>6}", flush=True)
        base = data['OFF(random)'].get(cname, [])
        for label in ('smart', 'ON(plan)'):
            ms = data[label].get(cname, [])
            if base and ms and avg(base, 0) > 0:
                d = (avg(ms, 0) / avg(base, 0) - 1) * 100
                print(f"     -> {label} makespan vs OFF(random): {d:+.0f}%", flush=True)
    print("\n  适配优化 ON(plan) 相对 OFF(random) 的 makespan 下降 = 适配优化组件的端到端收益。", flush=True)
    return 0


if __name__ == '__main__':
    sys.exit(main())
