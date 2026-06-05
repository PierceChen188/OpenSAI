"""单设备数据面隔离测试：Producer -> Consumer（都落在同一台 device），
验证数据面接线：server 是否把 input_artifacts 发给 Consumer、device 是否拉到产物。
即使同机，也通过 QUIC(loopback 到自身的 ArtifactServer) 走完整传输路径。
PASS -> exit 0, FAIL -> exit 1。
"""
import json
import re
import socket
import sys
import threading
import time

HOST, PORT = '127.0.0.1', 8888

dag = {'dagData': {
    'taskName': 'blob1', 'taskId': '1', 'taskDesc': 'single-device dataplane test',
    'taskCreateTime': time.time(), 'taskPolicy': 'p1',
    'nodes': [
        {'id': 1, 'name': 'Producer',
         'form': {'beforeNodes': [], 'afterNodes': [2], 'taskAlgorithm': 'Algorithm_ProduceBlob',
                  'taskVersion': '1.0',
                  'taskInputInfo': {'inDataPath': './data/', 'outDataPath': './data/', 'size_mb': 2},
                  'details': 'produce', 'directAllocationId': -1, 'deviceParam': {}},
         'result': {'taskResultInfo': '', 'status': 1, 'delay': 0, 'deviceId': 1001}},
        {'id': 2, 'name': 'Consumer',
         'form': {'beforeNodes': [1], 'afterNodes': [], 'taskAlgorithm': 'Algorithm_ConsumeBlob',
                  'taskVersion': '1.0',
                  'taskInputInfo': {'inDataPath': './data/', 'outDataPath': './data/'},
                  'details': 'consume', 'directAllocationId': -1, 'deviceParam': {}},
         'result': {'taskResultInfo': '', 'status': 1, 'delay': 0, 'deviceId': 1001}},
    ]}}

holder = {}
done = threading.Event()
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 128000000)
sock.connect((HOST, PORT))


def reader():
    sock.settimeout(45)
    try:
        while not done.is_set():
            data = sock.recv(65535)
            if not data:
                break
            try:
                obj = json.loads(data.decode('utf-8', 'replace'))
            except Exception:
                continue
            if 'dagRspStatus' in obj:
                print('[reader] ACK', obj['dagRspStatus'], flush=True)
            elif 'dagResult' in obj:
                holder['r'] = obj['dagResult']
                done.set(); break
    except socket.timeout:
        print('[reader] TIMEOUT', flush=True)
    finally:
        done.set()


threading.Thread(target=reader, daemon=True).start()
sock.send(json.dumps({'user_beat': {'name': 'blob1tester'}}).encode('utf-8'))
time.sleep(0.6)
sock.send(json.dumps(dag).encode('utf-8'))
print('[inject] submitted, waiting...', flush=True)
done.wait(timeout=45)
time.sleep(0.3)
sock.close()

res = holder.get('r')
if not res:
    print('[FAIL] no dagResult', flush=True)
    sys.exit(1)
print(json.dumps(res, ensure_ascii=False), flush=True)
nodes = {n.get('name'): n for n in res.get('nodes', [])}
prod = str(nodes.get('Producer', {}).get('ResultInfo', ''))
cons = str(nodes.get('Consumer', {}).get('ResultInfo', ''))
m = re.search(r'sha=([0-9a-f]+)', prod)
ok = bool(m) and (m.group(1) in cons) and ('ERROR' not in cons)
print(f"\nproducer={prod}\nconsumer={cons}", flush=True)
print('[PASS] single-device dataplane OK' if ok else '[FAIL] sha mismatch / no artifact', flush=True)
sys.exit(0 if ok else 1)
