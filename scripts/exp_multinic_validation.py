"""全栈验证 P0-1c 多网卡聚合的作用：源设备有多张网卡时，消费者对其多个 IP 并行分条带拉取，带宽叠加。

为什么需要这个：和中继多链路一样，必须走【完整链路】——
设备上报 nic_ips -> 大脑 _multinic_sources 把同一源设备的多个 NIC IP 填进 sources+size ->
消费者 fetch_striped 对不同目的 IP 建独立连接（路由表把各连接从匹配网卡发出）-> 聚合。

单机如何模拟“多网卡”：数据面 server 绑 0.0.0.0，故经 127.0.0.1 与本机 LAN IP 都能连到同一源。
用 OPENSAI_NIC_IPS="127.0.0.1,<LAN-IP>" 让源设备“声明”两张网卡（指向同一数据服务的两个目的 IP）。
单机两 IP 走同一物理栈、不会天然更快，故用 OPENSAI_TARGET_XFER_S 给每条路径(每块)限速 ->
单路成为瓶颈 -> 两条连接并行聚合 -> 更快。这验证的是【机制正确 + 确实并行】，真多网卡环境带宽天然叠加。

对比两档（同样限速 T 秒）：
  direct  : OPENSAI_MULTINIC=0 -> 仅 1 个直连源(1 路)
  multinic: OPENSAI_MULTINIC=1 -> 源设备 2 个 NIC IP(2 路聚合)
metric: makespan(传输主导) + errs(必须 0, 含传输 sha 正确)。期望 multinic 更快。

Run:  .venv/Scripts/python.exe scripts/exp_multinic_validation.py [trials]
"""
import os
import socket
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
N_DEVICES = 2         # A(源, 双网卡) + B(汇)


def lan_ip():
    """本机对外 LAN IPv4（用作源设备的“第二张网卡”目的 IP）；取不到返回 None。"""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(('8.8.8.8', 80))
        ip = s.getsockname()[0]
        return ip if ip and not ip.startswith('127.') else None
    except Exception:
        return None
    finally:
        s.close()


def build_dag(idx):
    n = inject_collab._node
    nodes = [
        n(1, 'Prod', 'Algorithm_ProduceBlob', [], [2], 1, {'size_mb': BIG_MB, 'compute_ms': 200}),
        n(2, 'Cons', 'Algorithm_ConsumeBlob', [1], [], 2, {}),
    ]
    return {'dagData': {'taskName': f'mn{idx}', 'taskId': '1', 'taskDesc': '',
                        'taskCreateTime': time.time(), 'taskPolicy': 'p', 'nodes': nodes}}


def analyze(res):
    nodes = res.get('nodes', [])
    got = sum(1 for nd in nodes if str(nd.get('ResultInfo', '')))
    errs = sum(1 for nd in nodes if 'ERROR' in str(nd.get('ResultInfo', ''))
               or 'MISSING' in str(nd.get('ResultInfo', '')))
    errs += (2 - got)
    return float(res.get('finishedTime') or 0.0), errs


def run_stack(label, base_port, nic_ips, server_env):
    ts.kill_stale()
    t0 = time.time()
    while ts.port_in_use() and time.time() - t0 < 12:
        ts.kill_stale(); time.sleep(1)
    import redis
    r = redis.Redis(host='127.0.0.1', port=6379, db=0); r.flushall()
    procs = [ts.start('server.py', server_env, 'server')]
    time.sleep(3)
    # 两台设备：launch 顺序 = directAllocationId 顺序（1=A 源, 2=B 汇）。
    # 所有设备都声明同一组 NIC IP（whichever 当源都能多网卡；汇声明无副作用）。
    for i in range(N_DEVICES):
        procs.append(ts.start('device_client.py',
                              {'OPENSAI_DATA_PORT': str(base_port + i), 'OPENSAI_PROBE_INTERVAL': '600',
                               'OPENSAI_TASK_WORKERS': '2', 'OPENSAI_POOL_WARMUP_S': '45',
                               'OPENSAI_NIC_IPS': nic_ips}, f'dev{base_port + i}'))
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
    ip2 = lan_ip()
    if not ip2:
        print("无法获取本机 LAN IP，单机无法模拟第二张网卡——请在多网卡/多主机环境跑此实验。", flush=True)
        return 1
    nic_ips = f"127.0.0.1,{ip2}"
    print(f"\n多网卡聚合全栈验证: A->B 传 {BIG_MB}MB; 源声明双网卡 [{nic_ips}]; "
          f"每路限速≈{BIG_MB/XFER_S:.0f}MB/s; {TRIALS} trials/档", flush=True)
    base = {'OPENSAI_TARGET_XFER_S': str(XFER_S)}          # 每路 A2 限速（两档相同）
    d = run_stack('direct(1路)', 9950, nic_ips, dict(base, OPENSAI_MULTINIC='0'))
    m = run_stack('multinic(2路)', 9954, nic_ips,
                  dict(base, OPENSAI_MULTINIC='1', OPENSAI_MULTINIC_MIN_MB='1'))
    md, mm = avg(d), avg(m)
    eAll = sum(x[1] for grp in (d, m) for x in grp)
    print(f"\n{'='*64}\n  结果 (makespan 秒; errs 合计={eAll})\n{'='*64}", flush=True)
    print(f"  direct   单网卡(1路): {md:.2f}", flush=True)
    print(f"  multinic 双网卡(2路): {mm:.2f}", flush=True)
    if md == md and mm == mm and md > 0:
        print(f"    多网卡收益: {(mm/md-1)*100:+.0f}%  (越负越好; 双路聚合应更快)", flush=True)
    print("\n  注: 是否真的下发了 2 个源, 见 .run/server.err.log 的 [multipath] ... 多网卡直连x2 日志。", flush=True)
    return 0


if __name__ == '__main__':
    sys.exit(main())
