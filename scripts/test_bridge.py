"""桥接(Web)层冒烟：起 server + 2 设备 + user_client 桥接(:5000)，校验
  F0-01 SPA 服务、A2-02 设备清单、A2-03 决策事件、A2-04 算法上传。
桥接各 /api/* 为一次性 SSE 流(text/event-stream)，读到首个 data 帧即可。
Run:  .venv/Scripts/python.exe scripts/test_bridge.py
"""
import json
import os
import socket
import sys
import time
import urllib.request

try:
    sys.stdout.reconfigure(encoding='utf-8')
except Exception:
    pass

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(ROOT, 'Offloading-System'))
import test_suite as ts        # noqa: E402
import run_tests as rt         # noqa: E402
import redis                   # noqa: E402

BRIDGE = 'http://127.0.0.1:5000'
RESULTS = []


def rec(cid, name, ok, detail=''):
    RESULTS.append({'case': cid, 'name': name, 'pass': bool(ok), 'detail': str(detail)})
    print(f"  [{'PASS' if ok else 'FAIL'}] {cid} {name}  ({detail})", flush=True)


def http_get(path, timeout=8):
    return urllib.request.urlopen(BRIDGE + path, timeout=timeout)


def sse_first(path, timeout=10):
    """读一次性 SSE 流的首个 data 事件，返回解析后的对象(或原始字符串)。"""
    req = urllib.request.urlopen(BRIDGE + path, timeout=timeout)
    t0 = time.time()
    for raw in req:
        if time.time() - t0 > timeout:
            break
        line = raw.decode('utf-8', 'replace').strip()
        if line.startswith('data:'):
            payload = line[5:].strip()
            try:
                return json.loads(payload)
            except Exception:
                return payload
    return None


def bridge_up(timeout=30):
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            urllib.request.urlopen(BRIDGE + '/', timeout=3)
            return True
        except Exception:
            time.sleep(1)
    return False


def main():
    try:
        r = redis.Redis(host='127.0.0.1', port=6379, db=0)
        r.ping()
    except Exception as e:
        print(f'[setup] FAIL redis: {e}'); return 1
    ts.kill_stale()
    t0 = time.time()
    while ts.port_in_use() and time.time() - t0 < 12:
        ts.kill_stale(); time.sleep(1)
    r.flushall()
    procs = [ts.start('server.py', {'OPENSAI_PLACEMENT': 'plan'}, 'bserver')]
    time.sleep(3)
    procs.append(ts.start('device_client.py', {'OPENSAI_DATA_PORT': '9990', 'OPENSAI_TASK_WORKERS': '2',
                                               'OPENSAI_POOL_WARMUP_S': '45'}, 'bdev0'))
    procs.append(ts.start('device_client.py', {'OPENSAI_DATA_PORT': '9991', 'OPENSAI_TASK_WORKERS': '2',
                                               'OPENSAI_POOL_WARMUP_S': '45'}, 'bdev1'))
    # 桥接(连 :8888，跑 Flask :5000)
    procs.append(ts.start('user_client.py', tag='bridge'))
    try:
        t0 = time.time()
        while int(r.scard('DEVICE_ONLINE_SET')) < 2 and time.time() - t0 < 90:
            time.sleep(2)
        if not bridge_up(40):
            rec('F0-01', '桥接服务 SPA/上线', False, '桥接 :5000 未就绪'); return _fin()
        time.sleep(3)

        # F0-01 SPA 服务
        try:
            body = http_get('/').read().decode('utf-8', 'replace')
            rec('F0-01', '桥接服务 SPA', 'id="root"' in body or 'OpenSAI' in body, 'index 已服务')
        except Exception as e:
            rec('F0-01', '桥接服务 SPA', False, f'异常: {e}')

        # A2-02 设备清单
        try:
            dv = sse_first('/api/devices', 12)
            n = len(dv) if isinstance(dv, (list, dict)) else 0
            rec('A2-02', '桥接/api/devices 设备清单', bool(dv) and n >= 1, f'设备项={n}')
        except Exception as e:
            rec('A2-02', '桥接/api/devices', False, f'异常: {e}')

        # A2-01 提交一个 DAG（经 TCP 直连，确保有结果可供事件流）
        rt.submit_dag(rt._dag('bridge_basic', [
            rt._node(1, 'P', 'Algorithm_ProduceBlob', [], [2], 1, {'size_mb': 1}),
            rt._node(2, 'C', 'Algorithm_ConsumeBlob', [1], [], 2, {})]), timeout=60)
        time.sleep(1)

        # A2-03 决策事件流
        try:
            ev = sse_first('/api/events', 12)
            # 桥接回传形如 {"eventsInfo": {"events": [...]}}
            if isinstance(ev, dict):
                evs = (ev.get('eventsInfo') or {}).get('events') or ev.get('events')
            else:
                evs = ev if isinstance(ev, list) else []
            kinds = set(e.get('kind') for e in (evs or []))
            rec('A2-03', '桥接/api/events 决策事件', bool(evs), f'事件数={len(evs or [])}, 种类={sorted(kinds)}')
        except Exception as e:
            rec('A2-03', '桥接/api/events', False, f'异常: {e}')

        # A2-04 算法上传
        try:
            import io
            algo_name = 'Algorithm_SmokeUploaded.py'
            content = (b"__all__=['Algorithm_SmokeUploaded']\n"
                       b"class Algorithm_SmokeUploaded:\n"
                       b"    def __init__(self,p,d):pass\n"
                       b"    def run(self):return 'ok'\n")
            boundary = '----tfboundary'
            body = (f'--{boundary}\r\nContent-Disposition: form-data; name="file"; '
                    f'filename="{algo_name}"\r\nContent-Type: text/x-python\r\n\r\n').encode() + \
                content + f'\r\n--{boundary}--\r\n'.encode()
            req = urllib.request.Request(BRIDGE + '/api/upload_py', data=body,
                                         headers={'Content-Type': f'multipart/form-data; boundary={boundary}'})
            resp = urllib.request.urlopen(req, timeout=10)
            code = resp.getcode()
            landed = os.path.join(ROOT, 'Offloading-System', 'device', 'implements', algo_name)
            ok = os.path.exists(landed)
            rec('A2-04', '桥接/api/upload_py 算法上传', code == 200 and ok, f'http={code}, 落地={ok}')
            try:
                os.remove(landed)
            except OSError:
                pass
        except Exception as e:
            rec('A2-04', '桥接/api/upload_py', False, f'异常: {e}')
        return _fin()
    finally:
        for p in procs:
            ts.kill_tree(p)
        ts.kill_stale()


def _fin():
    passed = sum(1 for x in RESULTS if x['pass'])
    out = os.path.join(ROOT, '.run', 'test_bridge_results.json')
    json.dump({'passed': passed, 'total': len(RESULTS), 'results': RESULTS},
              open(out, 'w', encoding='utf-8'), ensure_ascii=False, indent=2)
    print(f'\n=== 桥接冒烟：{passed}/{len(RESULTS)} 通过 ===', flush=True)
    return 0 if passed == len(RESULTS) else 1


if __name__ == '__main__':
    sys.exit(main())
