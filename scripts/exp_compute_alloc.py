"""Demonstrate T3.2-B per-task CPU allocation: two identical matmul tasks on the SAME
device, one declaring cores=1, the other cores=16. The 16-core task should finish much
faster -- proving the brain's per-task core allocation is enforced end-to-end."""
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


def dag():
    n = inject_collab._node
    # 两个独立 matmul 任务, 都钉在设备1(directAllocationId=1), 并行跑; 只是声明的核数不同。
    nodes = [
        n(1, 'MatMul_1core', 'Algorithm_MatMul', [], [], 1, {'matrix': 1000, 'iters': 12, 'cores': 1}),
        n(2, 'MatMul_16core', 'Algorithm_MatMul', [], [], 1, {'matrix': 1000, 'iters': 12, 'cores': 16}),
    ]
    return {'dagData': {'taskName': 'cpu', 'taskId': '1', 'taskDesc': '',
                        'taskCreateTime': time.time(), 'taskPolicy': 'p', 'nodes': nodes}}


def main():
    ts.kill_stale()
    t0 = time.time()
    while ts.port_in_use() and time.time() - t0 < 12:
        ts.kill_stale(); time.sleep(1)
    import redis
    r = redis.Redis(host='127.0.0.1', port=6379, db=0); r.flushall()
    procs = [ts.start('server.py', tag='server')]
    time.sleep(3)
    # 一台设备, 4 个并行 worker(能同时跑两个任务), 关探测。
    procs.append(ts.start('device_client.py',
                          {'OPENSAI_DATA_PORT': '9999', 'OPENSAI_PROBE_INTERVAL': '0',
                           'OPENSAI_TASK_WORKERS': '4'}, 'device1'))
    try:
        t0 = time.time()
        while int(r.scard('DEVICE_ONLINE_SET')) < 1 and time.time() - t0 < 40:
            time.sleep(2)
        time.sleep(3)
        if int(r.scard('DEVICE_ONLINE_SET')) < 1:
            print("FAIL: device not online"); return 1
        print("=== T3.2-B per-task CPU allocation (same matmul, different core budget) ===\n", flush=True)
        res = inject_collab.submit(dag(), timeout=120)
        if not res:
            print("(no result)"); return 1
        for nd in sorted(res.get('nodes', []), key=lambda x: x.get('id', 0)):
            print(f"  {nd.get('name'):<14} -> {nd.get('ResultInfo')}", flush=True)
    finally:
        for p in procs:
            ts.kill_tree(p)
        ts.kill_stale()
    return 0


if __name__ == '__main__':
    sys.exit(main())
