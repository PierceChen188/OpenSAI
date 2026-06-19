"""Full-stack 综合验证：在异构集群 + 复杂混合 DAG 上，量化核心功能的收益。

验证对象：
  - 设备选择（放置）：random vs plan
  - 资源分配：每任务 CPU 核(算力) + 自动内存/带宽
  - 数据面：跨设备传输（producer->consumer）端到端正确

DAG（混合两支，汇到一个 sink）：
  数据支路：N_PROD 个 ProduceBlob(大 blob) -> 各接 1 个 ConsumeBlob（跨设备传输：局部性 + 带宽）
  计算支路：N_COMP 个 ParallelMatMul(cores 可调：算力分配 + 容量感知放置)
  sink：ProduceBlob(0)，依赖全部消费者 + 全部计算 -> makespan = 整图完成

三档消融（每档=一套 server env，单独起栈，TRIALS 次取平均）：
  A baseline : OPENSAI_PLACEMENT=random, matmul cores=1, 无资源
  B placement: OPENSAI_PLACEMENT=plan,   matmul cores=1
  C plan+res : OPENSAI_PLACEMENT=plan,   matmul cores=HI + 自动内存/带宽
量化：A->B = 放置收益；B->C = 资源收益；A->C = 合计。errs 必须为 0（含数据面 sha 正确）。

中继多链路：localhost 无链路非对称、大脑遥测不会偏向中继，其吞吐聚合见 bench_dataplane.py
[E]/[F]（1.5x）；本实验跑通即验证数据面端到端正确。

Run:  .venv/Scripts/python.exe scripts/exp_fullstack_validation.py [trials]
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
N_PROD = 8                # 数据支路：生产者/消费者对数（跨设备传输）
N_COMP = 5                # 计算支路：并行 matmul 个数
BLOB_MB = 12              # 每个产物大小（传输 + 带宽）
PROD_MS = 300            # 生产者单核忙等
MM_MATRIX, MM_CHUNKS = 1200, 48
CORES_HI = 4
N_STRONG, N_WEAK = 2, 2   # 异构：2 强(3 槽) + 2 弱(1 槽)
N_DEVICES = N_STRONG + N_WEAK


def build_dag(idx, cores):
    n = inject_collab._node
    P = list(range(1, N_PROD + 1))                        # producers
    Q = list(range(N_PROD + 1, 2 * N_PROD + 1))           # consumers
    M = list(range(2 * N_PROD + 1, 2 * N_PROD + N_COMP + 1))  # compute
    sink = 2 * N_PROD + N_COMP + 1
    # 重要：节点必须按 id 升序排列 —— server 用 queue[id-1] 按 id 索引子任务（依赖/设备查询）。
    nodes = []
    for i in range(N_PROD):                                   # 生产者 id 1..N_PROD
        nodes.append(n(P[i], f'Prod{i}', 'Algorithm_ProduceBlob', [], [Q[i]], -1,
                       {'size_mb': BLOB_MB, 'compute_ms': PROD_MS}))
    for i in range(N_PROD):                                   # 消费者 id N_PROD+1..2N_PROD
        nodes.append(n(Q[i], f'Cons{i}', 'Algorithm_ConsumeBlob', [P[i]], [sink], -1, {}))
    for j in range(N_COMP):                                   # 计算 id 2N_PROD+1..
        nodes.append(n(M[j], f'MM{j}', 'Algorithm_ParallelMatMul', [], [sink], -1,
                       {'matrix': MM_MATRIX, 'chunks': MM_CHUNKS, 'cores': cores}))
    nodes.append(n(sink, 'Sink', 'Algorithm_ProduceBlob', Q + M, [], -1,    # sink id 2N_PROD+N_COMP+1
                   {'size_mb': 0, 'compute_ms': 0}))
    return {'dagData': {'taskName': f'fs{cores}_{idx}', 'taskId': '1', 'taskDesc': '',
                        'taskCreateTime': time.time(), 'taskPolicy': 'p', 'nodes': nodes}}


def analyze(res):
    nodes = res.get('nodes', [])
    n_nodes = 2 * N_PROD + N_COMP + 1
    got = sum(1 for nd in nodes if str(nd.get('ResultInfo', '')))
    errs = sum(1 for nd in nodes if 'ERROR' in str(nd.get('ResultInfo', ''))
               or 'MISSING' in str(nd.get('ResultInfo', '')))
    errs += (n_nodes - got)
    return float(res.get('finishedTime') or 0.0), errs


def run_stack(label, placement, cores, base_port, server_env_extra=None):
    ts.kill_stale()
    t0 = time.time()
    while ts.port_in_use() and time.time() - t0 < 12:
        ts.kill_stale(); time.sleep(1)
    import redis
    r = redis.Redis(host='127.0.0.1', port=6379, db=0); r.flushall()
    srv_env = {'OPENSAI_PLACEMENT': placement}
    srv_env.update(server_env_extra or {})
    procs = [ts.start('server.py', srv_env, 'server')]
    time.sleep(3)
    devspec = ([(base_port + i, '3') for i in range(N_STRONG)] +
               [(base_port + N_STRONG + i, '1') for i in range(N_WEAK)])
    for p, w in devspec:
        procs.append(ts.start('device_client.py',
                              {'OPENSAI_DATA_PORT': str(p), 'OPENSAI_PROBE_INTERVAL': '600',
                               'OPENSAI_TASK_WORKERS': w, 'OPENSAI_POOL_WARMUP_S': '45'}, f'dev{p}'))
    out = []
    try:
        t0 = time.time()
        while int(r.scard('DEVICE_ONLINE_SET')) < N_DEVICES and time.time() - t0 < 120:
            time.sleep(2)
        online = int(r.scard('DEVICE_ONLINE_SET'))
        time.sleep(5)
        print(f"  [{label}] devices online: {online}/{N_DEVICES}", flush=True)
        if online < N_DEVICES:
            print(f"  [{label}] FAIL: not all devices online"); return out
        for t in range(TRIALS):
            res = inject_collab.submit(build_dag(t, cores), timeout=300)
            if res:
                ms, errs = analyze(res)
                out.append((ms, errs))
                print(f"    [{label}] trial{t + 1}: makespan={ms:.2f}s errs={errs}", flush=True)
            else:
                print(f"    [{label}] trial{t + 1}: TIMEOUT/no result", flush=True)
    finally:
        for p in procs:
            ts.kill_tree(p)
        ts.kill_stale()
    return out


def avg(rows, i=0):
    return sum(x[i] for x in rows) / len(rows) if rows else float('nan')


def main():
    print(f"\nFULL-STACK 综合验证: {N_PROD}产+{N_PROD}消(blob {BLOB_MB}MB) + {N_COMP}xMatMul -> sink; "
          f"{N_STRONG}强(3槽)+{N_WEAK}弱(1槽); {TRIALS} trials/档", flush=True)
    A = run_stack('A random,cores1', 'random', 1, 9930)
    B = run_stack('B plan,cores1', 'plan', 1, 9935)
    C = run_stack('C plan,cores%d+res' % CORES_HI, 'plan', CORES_HI, 9940,
                  {'OPENSAI_AUTO_MEM': '1', 'OPENSAI_TARGET_XFER_S': '2'})
    mA, mB, mC = avg(A), avg(B), avg(C)
    eAll = sum(x[1] for grp in (A, B, C) for x in grp)
    print(f"\n{'='*70}\n  结果 (makespan 秒, 越低越好; errs 合计={eAll})\n{'='*70}", flush=True)
    print(f"  A baseline (random, cores=1)     : {mA:.2f}", flush=True)
    print(f"  B placement (plan, cores=1)      : {mB:.2f}", flush=True)
    print(f"  C plan+resource (plan, cores={CORES_HI})  : {mC:.2f}", flush=True)
    if mA and mA == mA:
        if mB == mB:
            print(f"    放置收益 A->B: {(mB/mA-1)*100:+.0f}%", flush=True)
        if mB == mB and mC == mC:
            print(f"    资源收益 B->C: {(mC/mB-1)*100:+.0f}%", flush=True)
            print(f"    合计   A->C: {(mC/mA-1)*100:+.0f}%", flush=True)
    return 0


if __name__ == '__main__':
    sys.exit(main())
