"""OpenSAI integration scenarios — run against an already-running stack
(scripts/test_suite.ps1 starts server + 2 devices, then invokes this).

Scenarios (lightweight, pure-Python algorithms, tiny data — fast):
  1. basic DAG (Pi -> Sleep)        : core schedule/dispatch/execute/collect
  2. cross-device data plane        : Produce@devA -> QUIC -> Consume@devB, sha match
  3. fan-in (2 producers -> fusion) : Consumer fetches from TWO devices

Exit 0 only if all scenarios pass.
"""
import json
import re
import socket
import sys
import threading
import time

HOST, PORT = '127.0.0.1', 8888


def submit_dag(dagdata, timeout=45):
    """Submit a DAG, return its dagResult dict (or None on timeout)."""
    holder = {}
    done = threading.Event()
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 128000000)
    s.connect((HOST, PORT))

    def reader():
        s.settimeout(timeout)
        try:
            while not done.is_set():
                data = s.recv(65535)
                if not data:
                    break
                try:
                    obj = json.loads(data.decode('utf-8', 'replace'))
                except Exception:
                    continue
                if 'dagResult' in obj:
                    holder['r'] = obj['dagResult']
                    done.set()
                    break
        except socket.timeout:
            pass
        finally:
            done.set()

    threading.Thread(target=reader, daemon=True).start()
    s.send(json.dumps({'user_beat': {'name': 'suite'}}).encode('utf-8'))
    time.sleep(0.6)  # avoid TCP coalescing of user_beat + dagData
    s.send(json.dumps(dagdata).encode('utf-8'))
    done.wait(timeout)
    time.sleep(0.2)
    try:
        s.close()
    except Exception:
        pass
    return holder.get('r')


def _node(nid, name, algo, before, after, direct, extra=None):
    info = {'inDataPath': './data/', 'outDataPath': './data/'}
    info.update(extra or {})
    return {'id': nid, 'name': name,
            'form': {'beforeNodes': before, 'afterNodes': after, 'taskAlgorithm': algo,
                     'taskVersion': '1.0', 'taskInputInfo': info, 'details': '',
                     'directAllocationId': direct, 'deviceParam': {}},
            'result': {'taskResultInfo': '', 'status': 1, 'delay': 0, 'deviceId': 0}}


def _dag(name, nodes):
    return {'dagData': {'taskName': name, 'taskId': '1', 'taskDesc': '',
                        'taskCreateTime': time.time(), 'taskPolicy': 'p', 'nodes': nodes}}


def _get(res, name):
    return next((n for n in res.get('nodes', []) if n.get('name') == name), {})


def _sha(s):
    m = re.search(r'sha=([0-9a-f]+)', str(s))
    return m.group(1) if m else None


def test_basic():
    res = submit_dag(_dag('t_basic', [
        _node(1, 'Pi', 'Algorithm_Pi_Calculation', [], [2], -1, {'count_num': 100000}),
        _node(2, 'Sleep', 'Algorithm_Sleep_1', [1], [], -1, {'sleep_time': 1}),
    ]))
    if not res:
        return False, 'no dagResult'
    pi = _get(res, 'Pi').get('ResultInfo')
    ft = res.get('finishedTime')
    if not (isinstance(pi, (int, float)) and 2.5 < float(pi) < 3.6):
        return False, f'pi not pi-like: {pi!r}'
    if not (isinstance(ft, (int, float)) and 0 <= float(ft) < 120):
        return False, f'finishedTime bad: {ft!r}'
    return True, f'pi={pi:.3f}, finishedTime={ft:.2f}s'


def test_crossdevice():
    res = submit_dag(_dag('t_cross', [
        _node(1, 'Producer', 'Algorithm_ProduceBlob', [], [2], 1, {'size_mb': 1}),
        _node(2, 'Consumer', 'Algorithm_ConsumeBlob', [1], [], 2),
    ]))
    if not res:
        return False, 'no dagResult'
    p, c = _get(res, 'Producer'), _get(res, 'Consumer')
    psha, cinfo = _sha(p.get('ResultInfo')), str(c.get('ResultInfo'))
    if not psha or psha not in cinfo:
        return False, f'sha mismatch: producer={psha} consumer={cinfo[:60]}'
    pdev, cdev = p.get('result', {}).get('deviceId'), c.get('result', {}).get('deviceId')
    if pdev == cdev:
        return False, f'both on same device ({pdev}) — placement did not split'
    return True, f'sha ok, {pdev} -> {cdev}'


def test_fanin():
    res = submit_dag(_dag('t_fanin', [
        _node(1, 'P1', 'Algorithm_ProduceBlob', [], [3], 1, {'size_mb': 1}),
        _node(2, 'P2', 'Algorithm_ProduceBlob', [], [3], 2, {'size_mb': 1}),
        _node(3, 'Fusion', 'Algorithm_ConsumeBlob', [1, 2], [], 1),
    ]))
    if not res:
        return False, 'no dagResult'
    s1, s2 = _sha(_get(res, 'P1').get('ResultInfo')), _sha(_get(res, 'P2').get('ResultInfo'))
    finfo = str(_get(res, 'Fusion').get('ResultInfo'))
    d1 = _get(res, 'P1').get('result', {}).get('deviceId')
    d2 = _get(res, 'P2').get('result', {}).get('deviceId')
    if not (s1 and s2 and s1 in finfo and s2 in finfo):
        return False, f'fusion missing a producer sha: s1={s1} s2={s2} fusion={finfo[:80]}'
    if 'consumed 2 inputs' not in finfo:
        return False, f'expected 2 fused inputs: {finfo[:80]}'
    if d1 == d2:
        return False, f'producers on same device ({d1}) — not a real fan-in across devices'
    return True, f'2 inputs fused from {d1} & {d2}'


TESTS = [
    ('basic DAG (Pi->Sleep)', test_basic),
    ('cross-device data plane', test_crossdevice),
    ('fan-in (2 producers -> fusion)', test_fanin),
]


def main():
    passed = 0
    for name, fn in TESTS:
        try:
            ok, msg = fn()
        except Exception as e:
            ok, msg = False, f'exception: {e}'
        print(f"[{'PASS' if ok else 'FAIL'}] {name}: {msg}", flush=True)
        passed += 1 if ok else 0
    print(f"\n=== {passed}/{len(TESTS)} integration scenarios passed ===", flush=True)
    return 0 if passed == len(TESTS) else 1


if __name__ == '__main__':
    sys.exit(main())
