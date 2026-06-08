r"""DUAL-LEG experiment: a scenario where BOTH halves of the adaptation-optimization
component sit on the critical path at the same time, with a 2x2 factorial ablation that
attributes the makespan gain to each half INDEPENDENTLY.

The component has two halves:
  * PLACEMENT (WHERE)      -- offloading_strategy: random / smart / plan (capacity-aware).
  * RESOURCE ALLOC (HOW MUCH) -- _alloc_compute (B2): per-task CPU cores, capped to device
                              capacity and enforced via cpu_affinity in the worker pool.

Why a single workload can exercise BOTH at once without them physically fighting:
the two levers act on ORTHOGONAL resources --
  - placement balances how many tasks queue behind a device's WORKER SLOTS (concurrency),
  - core-alloc sets how many CPU CORES each task's matmul runs on (speed).
The host has plenty of cores, so a wide matmul on K cores and the queue behind a 1-slot
device do not contend for the same thing.

Workload: N_MM independent CPU-heavy matmul tasks (Algorithm_MatMul, scales with cores) ->
1 sink, on a HETEROGENEOUS 2-device cluster: 1 STRONG (3 worker slots) + 1 WEAK (1 slot).

  makespan ~= (max #tasks queued on one device's slot) x (per-task matmul time).
              \_______________ PLACEMENT lever ______________/   \__ CORES lever __/

2x2 ablation (placement {random, plan} x declared cores {1, HI}):
              cores=1 (resource OFF)         cores=HI (resource ON)
  random      A: piled on weak + slow  -> WORST
  plan        B: spread, but each slow      C(random,HI): fast, but piled on weak
  plan,HI     D: spread + fast         -> BEST
Measured (1 host, 32 logical cores): A=26.6s, B(plan,1)=15.9s, C(random,4)=9.2s, D=9.2s.
  * resource leg (cores): A->C -65%, B->D -42%  -- effective in BOTH rows.
  * placement leg:        A->B -40%, C->D  ~0%  -- effective only when tasks are SLOW (cores=1).
Both legs individually take effect. They do NOT fully compound here: once the resource leg makes
each task fast, makespan hits a floor set by the SHARED physical CPU's throughput, and placement
(which device) can't push below it (D~=C). The two are only truly independent across SEPARATE
physical hosts (placement moves work to a different CPU) -- the natural Phase-2 multi-host setting --
or with two independent bottleneck branches (a CPU-queue branch + a network branch).

Prereq this experiment surfaced & fixed: core allocation (cpu_affinity) alone did NOT speed up
numpy/torch compute -- BLAS spawned host-count threads that thrashed the pinned cores. run_task now
aligns BLAS/OMP threads to the leased core count (threadpoolctl); only then does the resource leg bite.

Run:  .venv/Scripts/python.exe scripts/exp_dual_adaptation.py [trials]
"""
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

TRIALS = int(sys.argv[1]) if len(sys.argv) > 1 else 2
N_MM = 8                              # wide layer of independent compute tasks
MATRIX = 1200                         # per-chunk matrix size
CHUNKS = 64                           # independent matmuls per task (compute dominates per-task overhead)
# CORES_HI=4 (not 8) on purpose: max concurrent = 3 strong-slots + 1 weak-slot = 4 tasks; 4x4=16 <= 24
# physical cores, so "resource ON" does NOT saturate the shared host -> the weak device's single SLOT stays
# a real queue bottleneck that PLACEMENT can still relieve. (At cores=8, 4x8=32 oversubscribes the CPU, so
# placement gets masked and the two legs stop compounding -- measured.)
CORES_LO, CORES_HI = 1, 4             # resource OFF vs ON (declared cores -> B2 alloc + BLAS-thread align)
N_STRONG, N_WEAK = 1, 1               # 1 strong (3 slots) + 1 weak (1 slot); 2-device stack is reliable
N_DEVICES = N_STRONG + N_WEAK


def build_dag(idx, cores):
    n = inject_collab._node
    nodes = []
    for i in range(1, N_MM + 1):
        nodes.append(n(i, f'MM{i}', 'Algorithm_ParallelMatMul', [], [N_MM + 1], -1,
                       {'matrix': MATRIX, 'chunks': CHUNKS, 'cores': cores}))
    # Join node: depends on all producers (so makespan = when the whole wide layer is done) but
    # IGNORES their data -> use ProduceBlob(0B) not ConsumeBlob (which would error on missing artifacts).
    nodes.append(n(N_MM + 1, 'Sink', 'Algorithm_ProduceBlob',
                   list(range(1, N_MM + 1)), [], -1, {'size_mb': 0, 'compute_ms': 0}))
    return {'dagData': {'taskName': f'c{cores}_{idx}', 'taskId': '1', 'taskDesc': '',
                        'taskCreateTime': time.time(), 'taskPolicy': 'p', 'nodes': nodes}}


def analyze(res):
    nodes = res.get('nodes', [])
    got = sum(1 for nd in nodes if str(nd.get('ResultInfo', '')))
    errs = sum(1 for nd in nodes if 'ERROR' in str(nd.get('ResultInfo', ''))
               or 'MISSING' in str(nd.get('ResultInfo', '')))
    errs += (N_MM + 1 - got)
    makespan = float(res.get('finishedTime') or 0.0)
    return makespan, errs


def run_stack(mode, base_port):
    ts.kill_stale()
    t0 = time.time()
    while ts.port_in_use() and time.time() - t0 < 12:
        ts.kill_stale(); time.sleep(1)
    import redis
    r = redis.Redis(host='127.0.0.1', port=6379, db=0); r.flushall()
    procs = [ts.start('server.py', {'OPENSAI_PLACEMENT': mode}, 'server')]
    time.sleep(3)
    # strong = 3 slots, weak = 1 slot. Launch back-to-back, then wait for all to register
    # (the regression's proven-reliable startup; serialized per-device waiting deadlocks spawn here).
    devspec = ([(base_port + i, '3') for i in range(N_STRONG)] +
               [(base_port + N_STRONG + i, '1') for i in range(N_WEAK)])
    for p, w in devspec:
        # Cap BLAS threads to CORES_HI so per-task matmul time scales CLEANLY with the brain's
        # core allocation. Without this, OpenBLAS spawns ~ncores(32) threads regardless of the
        # cpu_affinity pin, so a task pinned to few cores has 32 threads THRASHING those cores
        # (measured: 4 cores -> >200s, 8 -> 25s, 16 -> 0.9s -- a useless, noisy cliff). Capping
        # threads = the resource budget is exactly what core-aware control SHOULD do.
        procs.append(ts.start('device_client.py',
                              {'OPENSAI_DATA_PORT': str(p), 'OPENSAI_PROBE_INTERVAL': '600',
                               'OPENSAI_TASK_WORKERS': w, 'OPENSAI_POOL_WARMUP_S': '45',
                               'OPENBLAS_NUM_THREADS': str(CORES_HI), 'OMP_NUM_THREADS': str(CORES_HI),
                               'MKL_NUM_THREADS': str(CORES_HI)}, f'dev{p}'))
    out = {CORES_LO: [], CORES_HI: []}
    try:
        t0 = time.time()
        while int(r.scard('DEVICE_ONLINE_SET')) < N_DEVICES and time.time() - t0 < 90:
            time.sleep(2)
        online = int(r.scard('DEVICE_ONLINE_SET'))
        time.sleep(5)
        print(f"  devices online: {online}/{N_DEVICES}", flush=True)
        if online < N_DEVICES:
            print("  FAIL: not all devices online"); return out
        for cores in (CORES_LO, CORES_HI):
            for t in range(TRIALS):
                res = inject_collab.submit(build_dag(t, cores), timeout=300)
                if res:
                    ms, errs = analyze(res)
                    out[cores].append((ms, errs))
                    print(f"    cores={cores} trial{t + 1}: makespan={ms:.2f}s errs={errs}", flush=True)
                else:
                    print(f"    cores={cores} trial{t + 1}: TIMEOUT/no result", flush=True)
    finally:
        for p in procs:
            ts.kill_tree(p)
        ts.kill_stale()
    return out


def avg(rows, i=0):
    return sum(x[i] for x in rows) / len(rows) if rows else float('nan')


def main():
    print(f"\nDUAL-LEG adaptation experiment: {N_MM} parallel-compute tasks (n={MATRIX}, chunks={CHUNKS}) -> 1 sink;"
          f" 1 STRONG(3-slot)+1 WEAK(1-slot); {TRIALS} trials/cell", flush=True)
    data = {}
    for mode, base in (('random', 9970), ('plan', 9974)):
        print(f"\n  [stack] OPENSAI_PLACEMENT={mode}", flush=True)
        data[mode] = run_stack(mode, base)

    A = avg(data['random'][CORES_LO])      # random + cores=1   (neither leg)
    B = avg(data['plan'][CORES_LO])        # plan   + cores=1   (placement only)
    C = avg(data['random'][CORES_HI])      # random + cores=HI  (resource only)
    D = avg(data['plan'][CORES_HI])        # plan   + cores=HI  (both)
    errs = sum(x[1] for m in data.values() for rows in m.values() for x in rows)

    print(f"\n{'='*74}\n  RESULTS  2x2 makespan(s)  (lower=better; errs total={errs})\n{'='*74}", flush=True)
    print(f"  {'':<18}{'cores=1 (resource OFF)':>24}{'cores='+str(CORES_HI)+' (resource ON)':>24}", flush=True)
    print(f"  {'placement=random':<18}{A:>24.2f}{C:>24.2f}", flush=True)
    print(f"  {'placement=plan':<18}{B:>24.2f}{D:>24.2f}", flush=True)
    print("\n  Per-leg attribution:", flush=True)
    if A > 0 and B > 0 and C > 0:
        print(f"    resource leg (cores):  A->C {(C/A-1)*100:+.0f}%   B->D {(D/B-1)*100:+.0f}%   (helps in BOTH rows)", flush=True)
        print(f"    placement leg:         A->B {(B/A-1)*100:+.0f}%   C->D {(D/C-1)*100:+.0f}%   (helps when tasks are SLOW)", flush=True)
        print(f"    both vs neither:       A->D {(D/A-1)*100:+.0f}%", flush=True)
    print("\n  解读：两条腿各自都生效。资源(B2，每任务对齐核数+BLAS线程)在每一行都把 makespan 砍掉一大块；"
          "\n  放置(plan，按容量摊负载、少压 weak 单槽)在【任务慢(cores=1)】时收益最大(−40%)。"
          "\n  但在【单台物理主机】上二者不完全叠加：资源开到 cores=4 后 makespan 触及共享物理 CPU 吞吐"
          "\n  地板(~9s)，放置已无空间再压(D≈C)。真正独立可叠加需多台物理主机，或两条独立瓶颈支路。", flush=True)
    return 0


if __name__ == '__main__':
    sys.exit(main())
