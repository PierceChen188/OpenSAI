"""全栈验证 2c：多网卡聚合 + 中继多链路【同时开启】，确认两条机制叠加无冲突、收益可加。

集群：A(生产者, 声明双网卡, pin=1) -> B(消费者, pin=2)；R(中继候选, 在线, 非源非汇)。
单机模拟：数据面 server 绑 0.0.0.0，故 127.0.0.1 与本机 LAN IP 都连到同一源 -> 用 OPENSAI_NIC_IPS
让源“声明两张网卡”；每路用 OPENSAI_TARGET_XFER_S 限速使单路成瓶颈；中继用 GAIN=0.5 在相近链路下也选中。

4 档对照（其余相同，每路限速 T 秒）：
  direct   : MULTINIC=0 MULTIPATH=0            -> 1 路（仅直连）
  multinic : MULTINIC=1 MULTIPATH=0            -> 2 路（源 2 个 NIC IP）
  relay    : MULTINIC=0 MULTIPATH=1(GAIN=.5)   -> 2 路（直连 + 中继）
  combined : MULTINIC=1 MULTIPATH=1(GAIN=.5)   -> 3 路（2 NIC + 中继）  ★2c
metric: makespan(传输主导) + errs(必须 0)。期望 路数越多越快，combined 最快。

Run:  .venv/Scripts/python.exe scripts/exp_combined_validation.py [trials]
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
BIG_MB = 48
XFER_S = 6.0
N_DEVICES = 3         # A(源,双网卡) + B(汇) + R(中继候选)


def lan_ip():
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
    return {'dagData': {'taskName': f'cb{idx}', 'taskId': '1', 'taskDesc': '',
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
    print(f"\n2c 组合验证: A->B 传 {BIG_MB}MB; 源双网卡 [{nic_ips}] + R 中继; "
          f"每路限速≈{BIG_MB/XFER_S:.0f}MB/s; {TRIALS} trials/档", flush=True)
    base = {'OPENSAI_TARGET_XFER_S': str(XFER_S)}
    relay_on = {'OPENSAI_MULTIPATH': '1', 'OPENSAI_MULTIPATH_GAIN': '0.5', 'OPENSAI_MULTIPATH_MIN_MB': '1'}
    nic_on = {'OPENSAI_MULTINIC': '1', 'OPENSAI_MULTINIC_MIN_MB': '1'}
    d = run_stack('direct(1路)', 9960, nic_ips, dict(base, OPENSAI_MULTINIC='0', OPENSAI_MULTIPATH='0'))
    m = run_stack('multinic(2路)', 9964, nic_ips, dict(base, OPENSAI_MULTIPATH='0', **nic_on))
    rl = run_stack('relay(2路)', 9968, nic_ips, dict(base, OPENSAI_MULTINIC='0', **relay_on))
    cb = run_stack('combined(3路)', 9972, nic_ips, dict(base, **nic_on, **relay_on))
    md, mm, mr, mc = avg(d), avg(m), avg(rl), avg(cb)
    eAll = sum(x[1] for grp in (d, m, rl, cb) for x in grp)
    print(f"\n{'='*64}\n  结果 (makespan 秒, 越低越好; errs 合计={eAll})\n{'='*64}", flush=True)
    print(f"  direct   1 路(仅直连)      : {md:.2f}", flush=True)
    print(f"  multinic 2 路(双网卡)      : {mm:.2f}", flush=True)
    print(f"  relay    2 路(直连+中继)   : {mr:.2f}", flush=True)
    print(f"  combined 3 路(2NIC+中继)★  : {mc:.2f}", flush=True)
    if md == md and md > 0:
        if mm == mm:
            print(f"    多网卡 vs 直连 : {(mm/md-1)*100:+.0f}%", flush=True)
        if mr == mr:
            print(f"    中继   vs 直连 : {(mr/md-1)*100:+.0f}%", flush=True)
        if mc == mc:
            print(f"    组合   vs 直连 : {(mc/md-1)*100:+.0f}%", flush=True)
        if mc == mc and mm == mm:
            print(f"    组合   vs 2路  : {(mc/mm-1)*100:+.0f}%  (叠加增益)", flush=True)
    print("\n  注: 组合档应下发 3 个源, 见 .run/server.err.log 的 [multipath] ... 多网卡直连x2+中继。", flush=True)
    return 0


if __name__ == '__main__':
    sys.exit(main())
