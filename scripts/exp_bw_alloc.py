"""Demonstrate T3.2-A bandwidth allocation: one producer (32MB) feeds two consumers
that DECLARE different bandwidth needs (bw_mbps=2 vs 6). Run unlimited, then under a
per-device egress budget (4 MB/s) to show the budget caps + accounting.

Shows: (1) server [alloc] decisions, (2) measured per-link rates (observed_links).
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
import test_suite as ts
import inject_collab
import show_monitor


def dag(idx):
    n = inject_collab._node
    nodes = [
        n(1, 'Producer', 'Algorithm_ProduceBlob', [], [2, 3], 1, {'size_mb': 32}),
        n(2, 'ConsumerA', 'Algorithm_ConsumeBlob', [1], [], 2, {'bw_mbps': 2}),   # 声明 2 MB/s
        n(3, 'ConsumerB', 'Algorithm_ConsumeBlob', [1], [], 3, {'bw_mbps': 6}),   # 声明 6 MB/s
    ]
    return {'dagData': {'taskName': f'bw{idx}', 'taskId': '1', 'taskDesc': '',
                        'taskCreateTime': time.time(), 'taskPolicy': 'p', 'nodes': nodes}}


def run(label, server_env):
    print(f"\n################  {label}  ################", flush=True)
    ts.kill_stale()
    t0 = time.time()
    while ts.port_in_use() and time.time() - t0 < 12:
        ts.kill_stale(); time.sleep(1)
    import redis
    r = redis.Redis(host='127.0.0.1', port=6379, db=0); r.flushall()
    procs = [ts.start('server.py', server_env, 'server')]
    time.sleep(3)
    for i in range(3):
        procs.append(ts.start('device_client.py',
                              {'OPENSAI_DATA_PORT': str(9999 - i),
                               'OPENSAI_PROBE_INTERVAL': '0',     # 关闭主动探测，避免污染测量
                               'OPENSAI_TASK_WORKERS': '1'}, f'device{i + 1}'))
    try:
        t0 = time.time()
        while int(r.scard('DEVICE_ONLINE_SET')) < 3 and time.time() - t0 < 40:
            time.sleep(2)
        time.sleep(3)
        if int(r.scard('DEVICE_ONLINE_SET')) < 3:
            print("  FAIL: need 3 devices"); return
        res = inject_collab.submit(dag(0), timeout=90)
        print(f"  DAG result: {'ok' if res else 'TIMEOUT'}", flush=True)
        time.sleep(13)   # 等一个心跳把 observed_links 上报
        # 服务器的分配决策
        alloc = []
        try:
            with open(os.path.join(ROOT, '.run', 'server.err.log'), encoding='utf-8', errors='replace') as f:
                alloc = [ln.strip() for ln in f if '[alloc]' in ln]
        except Exception:
            pass
        print("  --- server 分配决策 ---", flush=True)
        for ln in alloc[-4:]:
            print("   ", ln.split(' - ')[-1], flush=True)
        print("  --- 实测每条链路速率 ---", flush=True)
        show_monitor.snapshot()
    finally:
        for p in procs:
            ts.kill_tree(p)
        ts.kill_stale()


if __name__ == '__main__':
    run("UNLIMITED (no device budget)", {})
    run("DEVICE BUDGET = 4 MB/s", {'OPENSAI_DEVICE_BW_MBPS': '4'})
