"""DEMO orchestrator: start a clean 2-device stack and run the collaborative
CIFAR-10 inference demo (inject_collab) against it, then tear the stack down.

Reuses the proven psutil-based startup/teardown from test_suite.py so the demo is
deterministic and self-cleaning. Assumes Redis is running on 127.0.0.1:6379.

Run with the venv python:
    .venv/Scripts/python.exe scripts/demo_collab.py [num_images]
"""
import os
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import test_suite as ts   # kill_stale / port_in_use / kill_tree / start
import inject_collab


def main():
    if len(sys.argv) > 1:
        inject_collab.N = int(sys.argv[1])

    ts.kill_stale()
    t0 = time.time()
    while ts.port_in_use() and time.time() - t0 < 12:
        ts.kill_stale()
        time.sleep(1)
    if ts.port_in_use():
        print('[demo] FAIL: port 8888 still in use after cleanup')
        return 1

    import redis
    r = redis.Redis(host='127.0.0.1', port=6379, db=0)
    try:
        r.ping()
    except Exception as e:
        print(f'[demo] FAIL: redis unavailable ({e}). Start redis first.')
        return 1
    r.flushall()

    procs = []
    try:
        procs.append(ts.start('server.py', tag='server'))
        time.sleep(3)
        procs.append(ts.start('device_client.py', {'OPENSAI_DATA_PORT': '9999'}, 'device1'))
        procs.append(ts.start('device_client.py', {'OPENSAI_DATA_PORT': '9998'}, 'device2'))

        t0 = time.time()
        while int(r.scard('DEVICE_ONLINE_SET')) < 2 and time.time() - t0 < 20:
            time.sleep(1)
        time.sleep(3)
        online = int(r.scard('DEVICE_ONLINE_SET'))
        print(f'[demo] devices online: {online}, server listening: {ts.port_in_use()}')
        if online < 2:
            print('[demo] FAIL: fewer than 2 devices registered')
            return 1

        return inject_collab.main()
    finally:
        for p in procs:
            ts.kill_tree(p)
        ts.kill_stale()


if __name__ == '__main__':
    sys.exit(main())
