"""OpenSAI integration scenarios — run against an already-running stack
(scripts/test_suite.ps1 starts server + 2 devices, then invokes this).

Scenarios (mostly lightweight, fast):
  1. basic DAG (Pi -> Sleep)        : core schedule/dispatch/execute/collect
  2. cross-device data plane        : Produce@devA -> QUIC -> Consume@devB, sha match
  3. fan-in (2 producers -> fusion) : Consumer fetches from TWO devices
  4. locality placement             : unpinned dependent chain co-locates (avoids transfer)
  5. collab perception (T6.3)       : CIFAR image split L/R -> FeatureExtract on 2 devices
                                      -> features over QUIC -> Fusion classifies (real model)

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
        buf = bytearray()
        try:
            while not done.is_set():
                chunk = s.recv(65536)
                if not chunk:
                    break
                buf.extend(chunk)
                while True:
                    nl = buf.find(b'\n')
                    if nl < 0:
                        break
                    line = bytes(buf[:nl]); del buf[:nl + 1]
                    if not line.strip():
                        continue
                    try:
                        obj = json.loads(line.decode('utf-8', 'replace'))
                    except Exception:
                        continue
                    if 'dagResult' in obj:
                        holder['r'] = obj['dagResult']
                        done.set()
                        return
        except socket.timeout:
            pass
        finally:
            done.set()

    threading.Thread(target=reader, daemon=True).start()
    # NDJSON 分帧：每条消息一行 + '\n'，无需 sleep 防粘包
    s.sendall((json.dumps({'user_beat': {'name': 'suite'}}) + '\n').encode('utf-8'))
    s.sendall((json.dumps(dagdata) + '\n').encode('utf-8'))
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


def test_locality():
    # Unpinned (directAllocationId=-1) chain. T3.1 locality-aware placement should
    # co-locate the consumer with the producer's data (avoid a cross-device transfer).
    res = submit_dag(_dag('t_locality', [
        _node(1, 'Producer', 'Algorithm_ProduceBlob', [], [2], -1, {'size_mb': 1}),
        _node(2, 'Consumer', 'Algorithm_ConsumeBlob', [1], [], -1),
    ]))
    if not res:
        return False, 'no dagResult'
    p, c = _get(res, 'Producer'), _get(res, 'Consumer')
    psha, cinfo = _sha(p.get('ResultInfo')), str(c.get('ResultInfo'))
    pdev = p.get('result', {}).get('deviceId')
    cdev = c.get('result', {}).get('deviceId')
    if not psha or psha not in cinfo:
        return False, f'consumer missing producer sha: p={psha} c={cinfo[:60]}'
    if pdev != cdev:
        return False, f'not co-located: producer@{pdev} consumer@{cdev} (locality should co-locate)'
    return True, f'co-located on {pdev} (locality avoided a cross-device transfer)'


def test_collab():
    # T6.3 协同感知冒烟：一张 CIFAR 图按左右两半在两台设备上分别提特征(真实 torch 模型)，
    # 两份特征经 QUIC 数据面传到 Fusion 节点拼接分类。守住"协同感知 DAG 形状"端到端不被改坏。
    # (用真实 Algorithm_FeatureExtract/Fusion + 训练好的权重；不断言准确率，只验证管线跑通+跨设备。)
    res = submit_dag(_dag('t_collab', [
        _node(1, 'FE_left', 'Algorithm_FeatureExtract', [], [3], 1, {'image_index': 0, 'half': 'left'}),
        _node(2, 'FE_right', 'Algorithm_FeatureExtract', [], [3], 2, {'image_index': 0, 'half': 'right'}),
        _node(3, 'Fusion', 'Algorithm_Fusion', [1, 2], [], 1, {}),
    ]), timeout=90)
    if not res:
        return False, 'no dagResult'
    fe_l, fe_r, fu = _get(res, 'FE_left'), _get(res, 'FE_right'), _get(res, 'Fusion')
    finfo = str(fu.get('ResultInfo'))
    m = re.search(r'class=(\d+)', finfo)
    if not m:
        return False, f'fusion produced no class (collab pipeline broke): {finfo[:90]}'
    cls = int(m.group(1))
    if not (0 <= cls <= 9):
        return False, f'class out of range: {cls}'
    ldev = fe_l.get('result', {}).get('deviceId')
    rdev = fe_r.get('result', {}).get('deviceId')
    if ldev is None or rdev is None or ldev == rdev:
        return False, f'features not split across devices: left@{ldev} right@{rdev}'
    return True, f'collab inference ok: class={cls} (left@{ldev} + right@{rdev} -> fused via data plane)'


TESTS = [
    ('basic DAG (Pi->Sleep)', test_basic),
    ('cross-device data plane', test_crossdevice),
    ('fan-in (2 producers -> fusion)', test_fanin),
    ('locality placement (co-locate dependent)', test_locality),
    ('collab perception (CIFAR split->fuse across devices)', test_collab),
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
