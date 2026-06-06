"""Verify the new web bridge + console end-to-end.

Starts server + 2 devices + user_client (Flask :5000), then exercises the new JSON API:
GET / (console.html), GET /api/monitor, GET /api/config, POST /api/config, POST /api/submit
+ poll /api/result. Prints PASS/FAIL per check.
"""
import json
import os
import subprocess
import sys
import time
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, HERE)
import test_suite as ts          # noqa: E402

BASE = 'http://127.0.0.1:5000'


def get(path, timeout=10):
    with urllib.request.urlopen(BASE + path, timeout=timeout) as r:
        return json.loads(r.read().decode('utf-8'))


def post(path, body, timeout=15):
    req = urllib.request.Request(BASE + path, data=json.dumps(body).encode('utf-8'),
                                 headers={'Content-Type': 'application/json'}, method='POST')
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode('utf-8'))


def main():
    ts.kill_stale()
    t0 = time.time()
    while ts.port_in_use() and time.time() - t0 < 12:
        ts.kill_stale(); time.sleep(1)
    import redis
    r = redis.Redis(host='127.0.0.1', port=6379, db=0); r.flushall()
    procs = [ts.start('server.py', tag='server')]
    time.sleep(3)
    procs.append(ts.start('device_client.py', {'OPENSAI_DATA_PORT': '9999'}, 'device1'))
    procs.append(ts.start('device_client.py', {'OPENSAI_DATA_PORT': '9998'}, 'device2'))
    # start the web bridge
    env = dict(os.environ)
    web = subprocess.Popen([sys.executable, os.path.join('Offloading-System', 'dag_offload', 'user_client.py')],
                           cwd=ROOT, env=env,
                           stdout=open(os.path.join(ROOT, '.run', 'web.out.log'), 'w'),
                           stderr=open(os.path.join(ROOT, '.run', 'web.err.log'), 'w'))
    procs.append(web)
    npass = nfail = 0

    def check(name, cond, extra=''):
        nonlocal npass, nfail
        ok = bool(cond)
        npass += ok; nfail += (not ok)
        print(f"[{'PASS' if ok else 'FAIL'}] {name}{(' — ' + extra) if extra else ''}", flush=True)

    try:
        t0 = time.time()
        while int(r.scard('DEVICE_ONLINE_SET')) < 2 and time.time() - t0 < 40:
            time.sleep(1)
        time.sleep(3)
        # wait for Flask
        up = False
        for _ in range(30):
            try:
                urllib.request.urlopen(BASE + '/api/monitor', timeout=3); up = True; break
            except Exception:
                time.sleep(1)
        check('Flask :5000 up', up)
        if not up:
            return 1

        # console page
        try:
            with urllib.request.urlopen(BASE + '/', timeout=5) as resp:
                html = resp.read().decode('utf-8', 'replace')
            check('GET / console.html', 'OpenSAI Console' in html and 'id="nav"' in html, f'{len(html)} bytes')
        except Exception as e:
            check('GET / console.html', False, str(e))

        # monitor view (aggregated, with devices) — poll (view publishes on an interval)
        mon, ndev = {}, 0
        for _ in range(15):
            mon = get('/api/monitor'); ndev = len(mon.get('devices', {}))
            if ndev >= 2:
                break
            time.sleep(1)
        check('GET /api/monitor devices', ndev >= 2, f'{ndev} devices, runtime={mon.get("runtime",{}).get("placement")}')
        anydev = next(iter(mon.get('devices', {}).values()), {})
        check('  device has ledger fields', 'placement_reserved' in anydev and 'cores_available' in anydev)

        # config get/set
        cfg = get('/api/config')
        check('GET /api/config effective', 'effective' in cfg, f"placement={cfg.get('effective',{}).get('placement')}")
        ap = post('/api/config', {'placement': 'smart', 'w_load': 0.7})
        check('POST /api/config', ap.get('ok') and ap.get('applied', {}).get('placement') == 'smart')
        now = None
        for _ in range(10):  # server applies (~2s) then republishes (~2s)
            now = get('/api/monitor').get('runtime', {}).get('placement')
            if now == 'smart':
                break
            time.sleep(1)
        check('  runtime placement switched to smart', now == 'smart', f"now={now}")
        post('/api/config', {'placement': 'plan'})  # restore

        # submit a DAG + poll result
        dag = {'taskName': 'fe-verify', 'nodes': [
            {'id': 1, 'name': 'N1', 'form': {'beforeNodes': [], 'afterNodes': [2], 'taskAlgorithm': 'Algorithm_Pi_Calculation',
             'taskVersion': '1.0', 'taskInputInfo': {'inDataPath': './data/', 'outDataPath': './data/', 'count_num': 200000},
             'details': '', 'directAllocationId': -1, 'deviceParam': {}}, 'result': {'taskResultInfo': '', 'status': 1, 'delay': 0, 'deviceId': 0}},
            {'id': 2, 'name': 'N2', 'form': {'beforeNodes': [1], 'afterNodes': [], 'taskAlgorithm': 'Algorithm_Sleep_1',
             'taskVersion': '1.0', 'taskInputInfo': {'inDataPath': './data/', 'outDataPath': './data/', 'sleep_time': 1},
             'details': '', 'directAllocationId': -1, 'deviceParam': {}}, 'result': {'taskResultInfo': '', 'status': 1, 'delay': 0, 'deviceId': 0}}]}
        sub = post('/api/submit', {'dagData': dag})
        tid = sub.get('task_id')
        check('POST /api/submit', sub.get('ok') and tid, f'task_id={str(tid)[:28]}')
        res = None
        for _ in range(40):
            rr = get('/api/result?task_id=' + urllib.parse.quote(tid))
            if rr.get('ready'):
                res = rr['result']; break
            time.sleep(1.5)
        check('GET /api/result returns dagResult', res is not None,
              f"makespan={res.get('finishedTime'):.2f}s nodes={len(res.get('nodes',[]))}" if res else 'timeout')
        if res:
            pi = next((n for n in res['nodes'] if n['id'] == 1), {}).get('ResultInfo')
            check('  result has Pi value', isinstance(pi, (int, float)) and 2.5 < float(pi) < 3.6, f'pi={pi}')
    finally:
        for p in procs:
            ts.kill_tree(p)
        ts.kill_stale()
    print(f"\n=== frontend verify: {npass} passed, {nfail} failed ===")
    return 0 if nfail == 0 else 1


if __name__ == '__main__':
    import urllib.parse  # noqa
    sys.exit(main())
