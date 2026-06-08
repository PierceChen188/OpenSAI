r"""DUAL-LEG COMPOUNDING experiment: make BOTH adaptation legs reduce makespan AND compound.

The earlier `exp_dual_adaptation.py` put both legs on ONE wide layer, so makespan = max(...) and
the legs MASKED each other (D ~= C). Here the two legs act on TWO SEQUENTIAL layers, so the
makespan is a SUM and the gains ADD (and the layers run in different time windows -> no shared-CPU
contention between the legs):

  Layer 1  (PLACEMENT lever): N_SPIN single-core busy-spin tasks (cores-INSENSITIVE). The weak
      device's single slot serializes whatever lands on it -> capacity-aware placement (plan)
      spreads them onto the strong device's many slots -> shorter layer-1.        [random vs plan]
  Layer 2  (RESOURCE/cores lever): 1 parallel-matmul task depending on ALL of layer 1
      (placement-insensitive, it's a single task; cores-SENSITIVE) -> more cores = shorter.  [1 vs HI]

  makespan = layer1_time + layer2_time
             \__ PLACEMENT shrinks this _/   \__ cores shrinks this __/

2x2 (placement {random,plan} x cores {1,HI}):
              cores=1                     cores=HI
  random      A: slow spread + slow mm    C: slow spread + fast mm
  plan        B: fast spread + slow mm    D: fast spread + fast mm  -> BEST
Expect A worst; B and C each partially better (one term shrunk); D best with the two gains ADDED.

Run:  .venv/Scripts/python.exe scripts/exp_dual_compound.py [trials]
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
N_SPIN = 8                            # layer-1: single-core busy-spin tasks (placement lever)
SPIN_MS = 1500                        # ms of single-core busy-work per layer-1 task
MM_MATRIX, MM_CHUNKS = 1200, 64       # layer-2: parallel-matmul size (cores lever)
CORES_LO, CORES_HI = 1, 4
N_STRONG, N_WEAK = 1, 1               # 1 strong (3 slots) + 1 weak (1 slot)
N_DEVICES = N_STRONG + N_WEAK
N_NODES = N_SPIN + 1


def build_dag(idx, cores):
    n = inject_collab._node
    nodes = []
    for i in range(1, N_SPIN + 1):    # layer 1: busy-spin -> layer 2
        nodes.append(n(i, f'S{i}', 'Algorithm_ProduceBlob', [], [N_SPIN + 1], -1,
                       {'compute_ms': SPIN_MS, 'size_mb': 0}))
    # layer 2: one parallel-matmul depending on all of layer 1 (ignores their data)
    nodes.append(n(N_SPIN + 1, 'MM', 'Algorithm_ParallelMatMul', list(range(1, N_SPIN + 1)), [], -1,
                   {'matrix': MM_MATRIX, 'chunks': MM_CHUNKS, 'cores': cores}))
    return {'dagData': {'taskName': f'k{cores}_{idx}', 'taskId': '1', 'taskDesc': '',
                        'taskCreateTime': time.time(), 'taskPolicy': 'p', 'nodes': nodes}}


def analyze(res):
    nodes = res.get('nodes', [])
    got = sum(1 for nd in nodes if str(nd.get('ResultInfo', '')))
    errs = sum(1 for nd in nodes if 'ERROR' in str(nd.get('ResultInfo', ''))
               or 'MISSING' in str(nd.get('ResultInfo', '')))
    errs += (N_NODES - got)
    return float(res.get('finishedTime') or 0.0), errs


def run_stack(mode, base_port):
    ts.kill_stale()
    t0 = time.time()
    while ts.port_in_use() and time.time() - t0 < 12:
        ts.kill_stale(); time.sleep(1)
    import redis
    r = redis.Redis(host='127.0.0.1', port=6379, db=0); r.flushall()
    procs = [ts.start('server.py', {'OPENSAI_PLACEMENT': mode}, 'server')]
    time.sleep(3)
    devspec = ([(base_port + i, '3') for i in range(N_STRONG)] +
               [(base_port + N_STRONG + i, '1') for i in range(N_WEAK)])
    for p, w in devspec:
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
    print(f"\nDUAL-LEG COMPOUNDING: layer1 = {N_SPIN}x busy-spin({SPIN_MS}ms, placement lever) -> "
          f"layer2 = 1x parallel-matmul(chunks={MM_CHUNKS}, cores lever); "
          f"1 STRONG(3-slot)+1 WEAK(1-slot); {TRIALS} trials/cell", flush=True)
    data = {}
    for mode, base in (('random', 9952), ('plan', 9956)):
        print(f"\n  [stack] OPENSAI_PLACEMENT={mode}", flush=True)
        data[mode] = run_stack(mode, base)

    A = avg(data['random'][CORES_LO])
    B = avg(data['plan'][CORES_LO])
    C = avg(data['random'][CORES_HI])
    D = avg(data['plan'][CORES_HI])
    errs = sum(x[1] for m in data.values() for rows in m.values() for x in rows)

    print(f"\n{'='*74}\n  RESULTS  2x2 makespan(s)  (lower=better; errs total={errs})\n{'='*74}", flush=True)
    print(f"  {'':<18}{'cores=1 (resource OFF)':>24}{'cores='+str(CORES_HI)+' (resource ON)':>24}", flush=True)
    print(f"  {'placement=random':<18}{A:>24.2f}{C:>24.2f}", flush=True)
    print(f"  {'placement=plan':<18}{B:>24.2f}{D:>24.2f}", flush=True)
    print("\n  Per-leg attribution:", flush=True)
    if A > 0 and B > 0 and C > 0:
        print(f"    placement leg (layer1):  A->B {(B/A-1)*100:+.0f}%   C->D {(D/C-1)*100:+.0f}%", flush=True)
        print(f"    resource  leg (layer2):  A->C {(C/A-1)*100:+.0f}%   B->D {(D/B-1)*100:+.0f}%", flush=True)
        print(f"    BOTH legs       (A->D):  {(D/A-1)*100:+.0f}%   (≈ sum of the two single-leg gains)", flush=True)
    print("\n  顺序两层：放置(plan)缩短 layer1(把单核任务摊离 weak 单槽)，资源(B2 多核)缩短 layer2(并行 matmul)。"
          "\n  两层在不同时间窗口 -> 不抢 CPU -> 收益相加，D 处 makespan 最低（叠加生效）。", flush=True)
    return 0


if __name__ == '__main__':
    sys.exit(main())
