"""OpenSAI fast regression suite — self-contained Python orchestrator.

Starts a clean stack (server + 2 devices) as subprocesses, runs the integration
scenarios from run_tests.py in-process, then tears the stack down. Uses psutil for
deterministic process cleanup (no flaky PowerShell orchestration). Assumes Redis is
already running on 127.0.0.1:6379 (scripts/test_suite.ps1 ensures that).

Run with the venv python:
    .venv/Scripts/python.exe scripts/test_suite.py
Exit 0 only if everything passes.  ~25-30s.
"""
import os
import socket
import subprocess
import sys
import time

import psutil

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
LOGS = os.path.join(ROOT, '.run')
VPY = sys.executable
os.makedirs(LOGS, exist_ok=True)
sys.path.insert(0, HERE)
import run_tests  # noqa: E402  -- the integration scenarios


def port_in_use(port=8888):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(0.5)
    try:
        s.connect(('127.0.0.1', port))
        return True
    except OSError:
        return False
    finally:
        s.close()


def kill_stale():
    me = os.getpid()
    # NB: iterate WITHOUT attrs and read name()/cmdline() inside the try. process_iter(attrs=...)
    # eagerly calls as_dict() during iteration, so a process exiting mid-scan raises OSError
    # (WinError 87 from ReadProcessMemory) before the loop body -> uncatchable there.
    for p in psutil.process_iter():
        try:
            if p.pid == me:
                continue
            if 'python' not in (p.name() or '').lower():
                continue
            cl = ' '.join(p.cmdline() or [])
            if 'dag_offload' in cl or 'run_tests.py' in cl:
                p.kill()
        except (psutil.NoSuchProcess, psutil.AccessDenied, OSError):
            pass


def kill_tree(proc):
    try:
        pp = psutil.Process(proc.pid)
        for c in pp.children(recursive=True):
            try:
                c.kill()
            except psutil.NoSuchProcess:
                pass
        pp.kill()
    except psutil.NoSuchProcess:
        pass


def start(script, env_extra=None, tag='proc'):
    env = dict(os.environ)
    env.update(env_extra or {})
    # 用轻量启动器拉起设备：让进程池的 worker 在 Windows spawn 时重新 import 的入口足够轻。
    if script == 'device_client.py':
        script = 'run_device.py'
    out = open(os.path.join(LOGS, f'{tag}.out.log'), 'w')
    err = open(os.path.join(LOGS, f'{tag}.err.log'), 'w')
    return subprocess.Popen(
        [VPY, os.path.join('Offloading-System', 'dag_offload', script)],
        cwd=ROOT, env=env, stdout=out, stderr=err,
    )


def main():
    # --- clean any stale stack + wait for port 8888 to free ---
    kill_stale()
    t0 = time.time()
    while port_in_use() and time.time() - t0 < 12:
        kill_stale()
        time.sleep(1)
    if port_in_use():
        print('[setup] FAIL: port 8888 still in use after cleanup')
        return 1

    # --- redis ---
    import redis
    r = redis.Redis(host='127.0.0.1', port=6379, db=0)
    try:
        r.ping()
    except Exception as e:
        print(f'[setup] FAIL: redis unavailable ({e}). Start redis first (scripts/test_suite.ps1 does).')
        return 1
    r.flushall()

    procs = []
    try:
        # --- start stack: server + 2 devices ---
        procs.append(start('server.py', tag='server'))
        time.sleep(3)
        procs.append(start('device_client.py', {'OPENSAI_DATA_PORT': '9999'}, 'device1'))
        procs.append(start('device_client.py', {'OPENSAI_DATA_PORT': '9998'}, 'device2'))

        # --- wait for both devices to register, then settle ---
        t0 = time.time()
        while int(r.scard('DEVICE_ONLINE_SET')) < 2 and time.time() - t0 < 20:
            time.sleep(1)
        time.sleep(3)
        online = int(r.scard('DEVICE_ONLINE_SET'))
        print(f'[setup] devices online: {online}, server listening: {port_in_use()}')
        if online < 2:
            print('[setup] FAIL: fewer than 2 devices registered')
            return 1

        # --- run integration scenarios (in-process) ---
        passed = 0
        for name, fn in run_tests.TESTS:
            try:
                ok, msg = fn()
            except Exception as ex:
                ok, msg = False, f'exception: {ex}'
            print(f"[{'PASS' if ok else 'FAIL'}] {name}: {msg}", flush=True)
            passed += 1 if ok else 0
        print(f'\n=== {passed}/{len(run_tests.TESTS)} scenarios passed ===')
        return 0 if passed == len(run_tests.TESTS) else 1
    finally:
        for p in procs:
            kill_tree(p)
        kill_stale()


if __name__ == '__main__':
    code = main()
    print('==== TEST SUITE: ' + ('ALL PASSED ====' if code == 0 else 'FAILURES ===='))
    sys.exit(code)
