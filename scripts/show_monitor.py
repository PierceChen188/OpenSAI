"""Read-only monitoring viewer: prints the live device table + observed inter-device
link bandwidth straight from the Redis device registry. No effect on the running system.

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
REFRESH = int(sys.argv[1]) if len(sys.argv) > 1 else 1


def load_devices():
    devices = {}
    members = R.smembers(K.DEVICE_ONLINE_SET) or set()
    for m in members:
        dno = m.decode() if isinstance(m, bytes) else str(m)
        raw = R.get(K.DEVICE_ONLINE_PREFIX + dno)
        if raw:
            try:
                devices[dno] = json.loads(raw)
            except Exception:
                pass
    return devices


def snapshot():
    devices = load_devices()
    if not devices:
        print("(no online devices — is a stack running?)", flush=True)
        return
    # stable ASCII labels + endpoint->label map for resolving link telemetry
    labels = {dno: f"dev{i + 1}" for i, dno in enumerate(sorted(devices))}
    ep2label = {}
    for dno, info in devices.items():
        ip, dp = info.get('ip_address'), info.get('data_port')
        if ip and dp is not None:
            ep2label[f"{ip}:{int(dp)}"] = labels[dno]

    print("\n=== devices ===", flush=True)
    print(f"{'dev':<5} {'cpu':>7} {'mem idle/total(GB)':>20} {'running':>8} {'rtt(ms)':>8}  data_port", flush=True)
    for dno in sorted(devices):
        d = devices[dno]
        mem = f"{d.get('idle_memory', '?')}/{d.get('total_memory', '?')}"
        print(f"{labels[dno]:<5} {str(d.get('used_cpu', '?')):>7} {mem:>20} "
              f"{str(d.get('work_status', 0)):>8} {str(d.get('delay', 0)):>8}  {d.get('data_port', '?')}", flush=True)

    print("\n=== observed inter-device links (active probe + passive transfers) ===", flush=True)
    any_link = False
    for dno in sorted(devices):
        for ep, st in (devices[dno].get('observed_links') or {}).items():
            any_link = True
            src = ep2label.get(ep, ep)  # source endpoint -> label
            print(f"  {src} -> {labels[dno]}:  {st.get('mbps', '?')} MB/s   "
                  f"{st.get('last_ms', '?')} ms   (n={st.get('samples', '?')})", flush=True)
    if not any_link:
        print("  (none yet — run a cross-device DAG, e.g. scripts/demo_collab.py, to populate)", flush=True)


def main():
    for i in range(REFRESH):
        snapshot()
        if i < REFRESH - 1:
            time.sleep(2)
    return 0


if __name__ == '__main__':
    sys.exit(main())
