"""Read-only monitoring viewer.

Prefers the SERVER-side aggregated view (Redis key OPENSAI_MONITOR_VIEW, published by the
server every ~5s) -- this merges device heartbeats with the brain's own in-memory ledgers
(placement reservations, committed egress bandwidth, in-flight tasks, algorithm history
profiling) that the raw heartbeats do NOT contain. If the server view is absent (e.g. old
server / not up yet), falls back to reading raw device heartbeats directly (no ledgers).

Run (with a stack up):
    .venv/Scripts/python.exe scripts/show_monitor.py        # one snapshot
    .venv/Scripts/python.exe scripts/show_monitor.py 10     # refresh 10 times, 2s apart
"""
import json
import os
import sys
import time

try:
    sys.stdout.reconfigure(encoding='utf-8')
except Exception:
    pass

import redis

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, 'Offloading-System'))
from config.common_redis_key_constant import ComRedisKeyConstant as K

R = redis.Redis(host='127.0.0.1', port=6379, db=0)
VIEW_KEY = 'OPENSAI_MONITOR_VIEW'
REFRESH = int(sys.argv[1]) if len(sys.argv) > 1 else 1


def _labels(devs):
    labels = {dno: f"dev{i + 1}" for i, dno in enumerate(sorted(devs))}
    ep2label = {}
    for dno, info in devs.items():
        ip, dp = info.get('ip_address'), info.get('data_port')
        if ip and dp is not None:
            ep2label[f"{ip}:{int(dp)}"] = labels[dno]
    return labels, ep2label


def _print_links(devs, labels, ep2label):
    print("\n=== observed inter-device links (active probe + passive transfers) ===", flush=True)
    any_link = False
    for dno in sorted(devs):
        for ep, st in (devs[dno].get('observed_links') or {}).items():
            any_link = True
            src = ep2label.get(ep, ep)
            print(f"  {src} -> {labels[dno]}:  {st.get('mbps', '?')} MB/s   "
                  f"{st.get('last_ms', '?')} ms   (n={st.get('samples', '?')})", flush=True)
    if not any_link:
        print("  (none yet — run a cross-device DAG, e.g. scripts/demo_collab.py, to populate)", flush=True)


def render_view(view):
    """T1.3 server 侧聚合视图：设备资源 + 大脑账本(预留/已承诺带宽/在飞/历史画像)。"""
    devs = view.get('devices') or {}
    labels, ep2label = _labels(devs)
    print(f"\n=== server 聚合监测视图 (设备心跳 + 大脑账本)  |  在飞任务={view.get('inflight_tasks', 0)} ===",
          flush=True)
    print(f"{'dev':<5}{'cpu%':>6}{'mem idle/tot(GB)':>18}{'run':>5}{'cores c/t/avail':>18}"
          f"{'mem_c(MB)':>10}{'resv':>6}{'bw_c(MB/s)':>11}{'rtt(ms)':>8}", flush=True)
    for dno in sorted(devs):
        d = devs[dno]
        mem = f"{d.get('idle_memory', '?')}/{d.get('total_memory', '?')}"
        cores = f"{d.get('cores_committed', 0)}/{d.get('cores_total', '?')}/{d.get('cores_available', '?')}"
        print(f"{labels[dno]:<5}{str(d.get('used_cpu', '?')):>6}{mem:>18}"
              f"{str(d.get('work_status', 0)):>5}{cores:>18}{str(d.get('mem_committed_mb', 0)):>10}"
              f"{str(d.get('placement_reserved', 0)):>6}{str(d.get('bw_committed_mbps', 0)):>11}"
              f"{str(d.get('delay', 0)):>8}", flush=True)
    ah = view.get('algo_history') or {}
    if ah:
        print("\n=== 算法历史画像 (B1: EWMA 实测，供自动定档/记账) ===", flush=True)
        print(f"  {'algo':<30}{'peak_mem(MB)':>13}{'task_mem(MB)':>13}{'proc(s)':>9}{'n':>5}", flush=True)
        for a, h in sorted(ah.items()):
            print(f"  {a:<30}{h.get('peak_mem_mb', 0):>13}{h.get('task_mem_mb', 0):>13}"
                  f"{h.get('proc_secs', 0):>9}{h.get('n', 0):>5}", flush=True)
    _print_links(devs, labels, ep2label)
    age = (time.time() - float(view.get('ts', 0))) if view.get('ts') else None
    if age is not None:
        print(f"\n  (view age: {age:.1f}s)", flush=True)


def load_devices():
    devices = {}
    for m in (R.smembers(K.DEVICE_ONLINE_SET) or set()):
        dno = m.decode() if isinstance(m, bytes) else str(m)
        raw = R.get(K.DEVICE_ONLINE_PREFIX + dno)
        if raw:
            try:
                devices[dno] = json.loads(raw)
            except Exception:
                pass
    return devices


def render_raw(devices):
    """回退：server 聚合视图不可用时，直接读设备心跳(没有大脑账本)。"""
    labels, ep2label = _labels(devices)
    print("\n=== devices (raw heartbeats; server 聚合视图未就绪 -> 无大脑账本) ===", flush=True)
    print(f"{'dev':<5} {'cpu':>7} {'mem idle/total(GB)':>20} {'running':>8} "
          f"{'cores c/t':>10} {'mem_comm(MB)':>13} {'rtt(ms)':>8}  data_port", flush=True)
    for dno in sorted(devices):
        d = devices[dno]
        mem = f"{d.get('idle_memory', '?')}/{d.get('total_memory', '?')}"
        cores = f"{d.get('cores_committed', 0)}/{d.get('cores_total', '?')}"
        print(f"{labels[dno]:<5} {str(d.get('used_cpu', '?')):>7} {mem:>20} "
              f"{str(d.get('work_status', 0)):>8} {cores:>10} {str(d.get('mem_committed_mb', 0)):>13} "
              f"{str(d.get('delay', 0)):>8}  {d.get('data_port', '?')}", flush=True)
    _print_links(devices, labels, ep2label)


def snapshot():
    raw = R.get(VIEW_KEY)
    view = None
    if raw:
        try:
            view = json.loads(raw)
        except Exception:
            view = None
    if view and view.get('devices'):
        render_view(view)
        return
    devices = load_devices()
    if not devices:
        print("(no online devices — is a stack running?)", flush=True)
        return
    render_raw(devices)


def main():
    for i in range(REFRESH):
        snapshot()
        if i < REFRESH - 1:
            time.sleep(2)
    return 0


if __name__ == '__main__':
    sys.exit(main())
