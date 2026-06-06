"""Demonstrate B4 memory tier = baseline + task budget.

Two identical ProduceBlob tasks (each allocates a 400MB blob) on the SAME device, both
declaring a per-task memory BUDGET (mem_mb), but different sizes:
  - budget 100MB  -> cap = worker_baseline + 100  -> the 400MB alloc exceeds it -> MemoryError (ERROR)
  - budget 800MB  -> cap = worker_baseline + 800  -> the 400MB alloc fits        -> OK

Shows the budget is the task's OWN marginal allowance on top of the runtime baseline
(so it never kills the baseline/torch itself), and that it is enforced end-to-end.
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


def dag():
    n = inject_collab._node
    nodes = [
        # 同样 400MB 的 ProduceBlob，都钉在设备1并行跑；只是声明的内存预算不同。
        n(1, 'Blob_budget100', 'Algorithm_ProduceBlob', [], [], 1, {'size_mb': 400, 'mem_mb': 100}),
        n(2, 'Blob_budget800', 'Algorithm_ProduceBlob', [], [], 1, {'size_mb': 400, 'mem_mb': 800}),
    ]
    return {'dagData': {'taskName': 'mem', 'taskId': '1', 'taskDesc': '',
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
        print("=== B4 memory tier = baseline + task budget (same 400MB task, different budget) ===\n", flush=True)
        res = inject_collab.submit(dag(), timeout=120)
        if not res:
            print("(no result)"); return 1
        for nd in sorted(res.get('nodes', []), key=lambda x: x.get('id', 0)):
            info = str(nd.get('ResultInfo'))
            verdict = 'KILLED (budget too small)' if 'ERROR' in info or 'MemoryError' in info else 'OK (fit budget)'
            print(f"  {nd.get('name'):<16} -> {verdict}\n      {info}", flush=True)
        print("\n  => 预算是【任务在基线之上自己要用的内存】：100MB 装不下 400MB 分配 -> 被杀；800MB 够 -> 通过。", flush=True)
    finally:
        for p in procs:
            ts.kill_tree(p)
        ts.kill_stale()
    return 0


if __name__ == '__main__':
    sys.exit(main())
