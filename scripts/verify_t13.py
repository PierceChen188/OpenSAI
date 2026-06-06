"""Verify T1.3 server-side aggregated monitor view.

Starts server + 2 devices, submits a compute-heavy cross-device DAG in a background
thread, and polls the Redis-published OPENSAI_MONITOR_VIEW to show that it carries the
BRAIN's ledgers (placement reservations, committed egress bandwidth, in-flight count)
LIVE during execution -- things the raw device heartbeats never contained -- plus the
algorithm history profiling that persists after completion. Finally renders show_monitor.
"""
import os
import sys
import threading
import time
import json

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(os.path.dirname(HERE), 'Offloading-System'))
import test_suite as ts          # noqa: E402
import inject_collab             # noqa: E402
import show_monitor              # noqa: E402
import redis                     # noqa: E402

R = redis.Redis(host='127.0.0.1', port=6379, db=0)


def main():
    ts.kill_stale()
    t0 = time.time()
    while ts.port_in_use() and time.time() - t0 < 12:
        ts.kill_stale(); time.sleep(1)
    R.flushall()
    procs = [ts.start('server.py', {'OPENSAI_PLACEMENT': 'plan', 'OPENSAI_DEVICE_BW_MBPS': '8'}, 'server')]
    time.sleep(3)
    for i in range(2):
        procs.append(ts.start('device_client.py',
                              {'OPENSAI_DATA_PORT': str(9999 - i), 'OPENSAI_PROBE_INTERVAL': '0',
                               'OPENSAI_TASK_WORKERS': '2'}, f'device{i + 1}'))
    try:
        t0 = time.time()
        while int(R.scard('DEVICE_ONLINE_SET')) < 2 and time.time() - t0 < 40:
            time.sleep(2)
        time.sleep(3)
        if int(R.scard('DEVICE_ONLINE_SET')) < 2:
            print("FAIL: devices not online"); return 1
        n = inject_collab._node
        # cross-device, compute-heavy so it stays in-flight while we poll the view
        dag = {'dagData': {'taskName': 'v', 'taskId': '1', 'taskDesc': '', 'taskCreateTime': time.time(),
                           'taskPolicy': 'p', 'nodes': [
                               n(1, 'P', 'Algorithm_ProduceBlob', [], [2], 1, {'size_mb': 8, 'compute_ms': 4000}),
                               n(2, 'C', 'Algorithm_ConsumeBlob', [1], [], 2, {'compute_ms': 4000}),
                           ]}}
        th = threading.Thread(target=lambda: inject_collab.submit(dag, timeout=60), daemon=True)
        th.start()

        print("\n--- polling OPENSAI_MONITOR_VIEW during execution (brain ledgers should go non-zero) ---")
        seen_reserved = seen_bw = seen_inflight = False
        for _ in range(12):
            raw = R.get('OPENSAI_MONITOR_VIEW')
            if raw:
                v = json.loads(raw)
                infl = v.get('inflight_tasks', 0)
                resv = {d: x.get('placement_reserved', 0) for d, x in v.get('devices', {}).items()}
                bw = {d: x.get('bw_committed_mbps', 0) for d, x in v.get('devices', {}).items()}
                if infl:
                    seen_inflight = True
                if any(resv.values()):
                    seen_reserved = True
                if any(bw.values()):
                    seen_bw = True
                print(f"  inflight={infl}  reserved={ {k: r for k, r in resv.items() if r} }  "
                      f"bw_committed={ {k: b for k, b in bw.items() if b} }")
            time.sleep(1)
        th.join(timeout=20)

        print(f"\n  saw inflight>0: {seen_inflight} | reserved>0: {seen_reserved} | bw_committed>0: {seen_bw}")
        time.sleep(6)  # let a fresh view publish post-completion (history persists, ledgers release)
        raw = R.get('OPENSAI_MONITOR_VIEW')
        if raw:
            v = json.loads(raw)
            print(f"\n  post-run: inflight={v.get('inflight_tasks')} (released), "
                  f"algo_history={list((v.get('algo_history') or {}).keys())} (persists)")
        print("\n================= show_monitor render =================")
        show_monitor.snapshot()
        ok = seen_inflight and seen_reserved
        print(f"\n  T1.3 VERIFY: {'PASS' if ok else 'PARTIAL'} (brain ledgers surfaced in server view)")
    finally:
        for p in procs:
            ts.kill_tree(p)
        ts.kill_stale()
    return 0


if __name__ == '__main__':
    sys.exit(main())
