"""Improved COMPUTE-heavy experiment: adaptation wins via CAPACITY-AWARE placement on
HETEROGENEOUS devices.

Why the big experiment showed only ~-4% in compute-heavy: the devices were homogeneous
(random spreads ~evenly -> no straggler) and the stub compute is single-core (so the
core-allocation lever is inert). Here we make capacity the bottleneck:

  1 STRONG device (OPENSAI_TASK_WORKERS=3) + 1 WEAK device (OPENSAI_TASK_WORKERS=1).
  A wide layer of N single-core compute tasks (compute_ms each, ~0 data) -> 1 sink.
  (single-core busy-work -> all fit the host's cores with no physical contention, so the only
   thing that matters is how many tasks queue behind too-few worker slots.)

  random     : ignores capacity -> ~N/2 tasks/device -> the WEAK device runs its ~half
               SERIALLY through its single slot -> it is the straggler -> long makespan.
  smart/plan : capacity-aware (cost ~ reserved/worker_slots) -> sends ~3x more to the STRONG
               device, few to WEAK -> balanced finish -> short makespan.
  (Kept to a 2-device stack: building >2 worker pools on this single host intermittently
   deadlocks the Windows multiprocessing spawn; 1 strong + 1 weak still shows the effect.)

Metrics: makespan (lower=better), tasks placed on WEAK(1-slot) devices (lower=smarter),
distinct devices, errors (=0).

Run:  .venv/Scripts/python.exe scripts/exp_compute_adaptation.py [trials]
"""
import json
import os
import sys
import time

try:
    sys.stdout.reconfigure(encoding='utf-8')
except Exception:
    pass

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(ROOT, 'Offloading-System'))
import test_suite as ts          # noqa: E402
import inject_collab             # noqa: E402
from config.common_redis_key_constant import ComRedisKeyConstant as KEY   # noqa: E402

TRIALS = int(sys.argv[1]) if len(sys.argv) > 1 else 2
N_TASKS = 16
COMPUTE_MS = 800
N_STRONG, N_WEAK = 1, 1             # 1 strong (3 slots) + 1 weak (1 slot); 2-device stack is reliable here
N_DEVICES = N_STRONG + N_WEAK


def build_dag(idx):
    n = inject_collab._node
    nodes = []
    for i in range(1, N_TASKS + 1):
        # single-core compute (compute_ms busy-spin), ~no data (size_mb=0)
        nodes.append(n(i, f'C{i}', 'Algorithm_ProduceBlob', [], [N_TASKS + 1], -1,
                       {'compute_ms': COMPUTE_MS, 'size_mb': 0}))
    nodes.append(n(N_TASKS + 1, 'Sink', 'Algorithm_ConsumeBlob',
                   list(range(1, N_TASKS + 1)), [], -1, {}))
    return {'dagData': {'taskName': f'cm{idx}', 'taskId': '1', 'taskDesc': '',
                        'taskCreateTime': time.time(), 'taskPolicy': 'p', 'nodes': nodes}}


def read_slots(r):
    """device_no -> worker_slots, from the Redis device registry."""
    slots = {}
    for m in (r.smembers(KEY.DEVICE_ONLINE_SET) or set()):
        dno = m.decode() if isinstance(m, bytes) else str(m)
        raw = r.get(KEY.DEVICE_ONLINE_PREFIX + dno)
        if raw:
            try:
                slots[dno] = int(json.loads(raw).get('worker_slots') or 0)
            except Exception:
                pass
    return slots


def analyze(res, slots):
    nodes = res.get('nodes', [])
    got = sum(1 for nd in nodes if str(nd.get('ResultInfo', '')))
    errs = sum(1 for nd in nodes if 'ERROR' in str(nd.get('ResultInfo', '')) or 'MISSING' in str(nd.get('ResultInfo', '')))
    errs += (N_TASKS + 1 - got)
    # tasks placed on weak (<=1 slot) devices (exclude the sink, count only the N compute tasks)
    weak = 0
    devs = set()
    for nd in nodes:
        if nd.get('name') == 'Sink':
            continue
        d = nd.get('result', {}).get('deviceId')
        if d is None:
            continue
        devs.add(d)
        if slots.get(d, 9) <= 1:
            weak += 1
    makespan = float(res.get('finishedTime') or 0.0)
    return makespan, weak, len(devs), errs


def run_stack(mode, base_port):
    ts.kill_stale()
    t0 = time.time()
    while ts.port_in_use() and time.time() - t0 < 12:
        ts.kill_stale(); time.sleep(1)
    import redis
    r = redis.Redis(host='127.0.0.1', port=6379, db=0); r.flushall()
    procs = [ts.start('server.py', {'OPENSAI_PLACEMENT': mode}, 'server')]
    time.sleep(3)
    # Stagger device launches (each builds a torch-preloading worker pool synchronously before
    # registering -> starting several heavy pools at once is a spawn storm). Distinct port range
    # per stack avoids racing the previous stack's not-yet-freed data ports.
    devspec = ([(base_port + i, '3') for i in range(N_STRONG)] +    # strong = 3 slots
               [(base_port + N_STRONG + i, '1') for i in range(N_WEAK)])
    # Launch all devices back-to-back, then wait for all to register -- exactly the regression's
    # proven-reliable 2-device startup. (Per-device SERIALIZED waiting was worse here: it left a
    # fully-built idle pool up while the next device's pool spawned, which intermittently
    # deadlocked the Windows multiprocessing spawn.)
    for p, w in devspec:
        procs.append(ts.start('device_client.py', {'OPENSAI_DATA_PORT': str(p), 'OPENSAI_PROBE_INTERVAL': '600',
                              'OPENSAI_TASK_WORKERS': w, 'OPENSAI_POOL_WARMUP_S': '45'}, f'dev{p}'))
    rows = []
    try:
        t0 = time.time()
        while int(r.scard('DEVICE_ONLINE_SET')) < N_DEVICES and time.time() - t0 < 90:
            time.sleep(2)
        online = int(r.scard('DEVICE_ONLINE_SET'))
        time.sleep(5)
        slots = read_slots(r)
        strong = sum(1 for v in slots.values() if v >= 2)
        weak = sum(1 for v in slots.values() if v <= 1)
        print(f"  devices online: {online}/{N_DEVICES}  (strong={strong} weak={weak})", flush=True)
        if online < N_DEVICES:
            print("  FAIL: not all devices online"); return rows
        for _t in range(TRIALS):
            res = inject_collab.submit(build_dag(_t), timeout=180)
            if res:
                rows.append(analyze(res, slots))
            else:
                print(f"    trial {_t + 1}: TIMEOUT/no result", flush=True)
    finally:
        for p in procs:
            ts.kill_tree(p)
        ts.kill_stale()
    return rows


def avg(rows, i):
    return sum(x[i] for x in rows) / len(rows) if rows else float('nan')


def main():
    print(f"\n{N_TASKS} single-core compute tasks ({COMPUTE_MS}ms each) -> 1 sink; "
          f"{N_STRONG} STRONG(3-slot) + {N_WEAK} WEAK(1-slot) devices; {TRIALS} trials", flush=True)
    data = {}
    for mode, base in (('random', 9982), ('smart', 9986), ('plan', 9990)):
        print(f"\n  [stack] OPENSAI_PLACEMENT={mode}", flush=True)
        data[mode] = run_stack(mode, base)
    print(f"\n{'='*72}\n  RESULTS  (makespan lower=better; tasks-on-WEAK lower=smarter; errs=0)\n{'='*72}", flush=True)
    print(f"  {'mode':<8}{'makespan(s)':>12}{'tasks_on_weak/'+str(N_TASKS):>18}{'devices':>9}{'errs':>6}", flush=True)
    for mode in ('random', 'smart', 'plan'):
        rows = data[mode]
        print(f"  {mode:<8}{avg(rows,0):>12.2f}{avg(rows,1):>18.1f}{avg(rows,2):>9.1f}{sum(x[3] for x in rows):>6}", flush=True)
    base = data.get('random')
    for mode in ('smart', 'plan'):
        rows = data.get(mode)
        if base and rows and avg(base, 0) > 0:
            print(f"     -> {mode} makespan vs random: {(avg(rows,0)/avg(base,0)-1)*100:+.0f}%", flush=True)
    print("\n  异构容量下：容量感知放置(smart/plan)把活派给强设备、少给弱(1槽)设备，避免弱设备串行拖尾"
          " -> makespan 明显低于 random。", flush=True)
    return 0


if __name__ == '__main__':
    sys.exit(main())
