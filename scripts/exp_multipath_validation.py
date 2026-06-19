"""全栈验证中继多链路的作用：通过大脑真实选中继 + 消费者分条带聚合，量化收益。

为什么需要这个：bench_dataplane.py 只在数据面层面证明了聚合；本实验走【完整链路】——
大脑 _pick_relay 选中继 -> 下发 sources(直连+中继)+size -> 消费者 fetch_striped 分条带聚合。

集群：A(生产者,pin) -> B(消费者,pin) 跨设备传输；R 作为中继候选（在线、非源非汇）。
触发：单机各链路速率相近，故用 OPENSAI_MULTIPATH_GAIN=0.5 让大脑在“相近链路”下也选中继；
      OPENSAI_TARGET_XFER_S 给每条路径限速（A2 自动按 size/目标时延）=> 每路成为瓶颈 ->
      两条路径(直连 + 经中继)聚合 -> 传输更快。A 出口不限(不设 OPENSAI_DATA_RATE_MBPS)。

对比两档（同样限速 T 秒）：
  direct   : OPENSAI_MULTIPATH=0  -> 仅直连(1 路)
  multipath: OPENSAI_MULTIPATH=1  -> 直连 + 中继(2 路聚合)
metric: makespan(传输主导) + errs(必须 0, 含传输 sha 正确)。期望 multipath 更快。

Run:  .venv/Scripts/python.exe scripts/exp_multipath_validation.py [trials]
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
BIG_MB = 48           # 大产物：让传输主导 makespan
XFER_S = 6.0          # 每路目标传输时延 -> A2 限速 ≈ BIG/XFER MB/s 每路
N_DEVICES = 3         # A(源) + B(汇) + R(中继候选)


def build_dag(idx):
    n = inject_collab._node
    # 产物 size_mb 同时给 ProduceBlob 产出 + 供大脑 size 估（output_sizes 上报实际字节）。
    nodes = [
        n(1, 'Prod', 'Algorithm_ProduceBlob', [], [2], 1, {'size_mb': BIG_MB, 'compute_ms': 200}),
        n(2, 'Cons', 'Algorithm_ConsumeBlob', [1], [], 2, {}),
    ]
    return {'dagData': {'taskName': f'mp{idx}', 'taskId': '1', 'taskDesc': '',
                        'taskCreateTime': time.time(), 'taskPolicy': 'p', 'nodes': nodes}}


def analyze(res):
    nodes = res.get('nodes', [])
    got = sum(1 for nd in nodes if str(nd.get('ResultInfo', '')))
    errs = sum(1 for nd in nodes if 'ERROR' in str(nd.get('ResultInfo', ''))
               or 'MISSING' in str(nd.get('ResultInfo', '')))
    errs += (2 - got)
    return float(res.get('finishedTime') or 0.0), errs


def run_stack(label, base_port, server_env):
    ts.kill_stale()
    t0 = time.time()
    while ts.port_in_use() and time.time() - t0 < 12:
        ts.kill_stale(); time.sleep(1)
    import redis
    r = redis.Redis(host='127.0.0.1', port=6379, db=0); r.flushall()
    procs = [ts.start('server.py', server_env, 'server')]
    time.sleep(3)
    # 三台设备：launch 顺序 = directAllocationId 顺序（1=A 源, 2=B 汇, 3=R 中继候选）
    for i in range(N_DEVICES):
        procs.append(ts.start('device_client.py',
                              {'OPENSAI_DATA_PORT': str(base_port + i), 'OPENSAI_PROBE_INTERVAL': '600',
                               'OPENSAI_TASK_WORKERS': '2', 'OPENSAI_POOL_WARMUP_S': '45'}, f'dev{base_port + i}'))
    out = []
    try:
        t0 = time.time()
        while int(r.scard('DEVICE_ONLINE_SET')) < N_DEVICES and time.time() - t0 < 90:
            time.sleep(2)
        online = int(r.scard('DEVICE_ONLINE_SET'))
        time.sleep(5)
        print(f"  [{label}] devices online: {online}/{N_DEVICES}", flush=True)
        if online < N_DEVICES:
            print(f"  [{label}] FAIL: not all online"); return out
        for t in range(TRIALS):
            res = inject_collab.submit(build_dag(t), timeout=180)
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


def avg(rows):
    return sum(x[0] for x in rows) / len(rows) if rows else float('nan')


def main():
    print(f"\n中继多链路全栈验证: A->B 传 {BIG_MB}MB, 每路限速≈{BIG_MB/XFER_S:.0f}MB/s; R 作中继; {TRIALS} trials/档", flush=True)
    base = {'OPENSAI_TARGET_XFER_S': str(XFER_S)}          # 每路 A2 限速（两档相同）
    d = run_stack('direct(1路)', 9920, dict(base, OPENSAI_MULTIPATH='0'))
    m = run_stack('multipath(2路)', 9924,
                  dict(base, OPENSAI_MULTIPATH='1', OPENSAI_MULTIPATH_GAIN='0.5', OPENSAI_MULTIPATH_MIN_MB='1'))
    md, mm = avg(d), avg(m)
    eAll = sum(x[1] for grp in (d, m) for x in grp)
    print(f"\n{'='*64}\n  结果 (makespan 秒; errs 合计={eAll})\n{'='*64}", flush=True)
    print(f"  direct   仅直连(1路): {md:.2f}", flush=True)
    print(f"  multipath 直连+中继(2路): {mm:.2f}", flush=True)
    if md == md and mm == mm and md > 0:
        print(f"    多链路收益: {(mm/md-1)*100:+.0f}%  (越负越好; 传输聚合应更快)", flush=True)
    print("\n  注: 是否真的走了中继, 见 .run/server.err.log 的 [multipath] 日志。", flush=True)
    return 0


if __name__ == '__main__':
    sys.exit(main())
