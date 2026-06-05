"""跨设备数据面端到端测试：提交 ProduceBlob -> ConsumeBlob 的 DAG，
强制两个节点分到不同 device（directAllocationId 1/2），验证：
  - 整图完成、拿到 dagResult；
  - 两节点确实在不同 device 上执行；
  - Consumer 读到的 sha == Producer 写的 sha（=> 数据经 QUIC 正确跨设备传输）。
PASS -> exit 0, FAIL -> exit 1。
"""
import json
import re
import socket
import sys
import threading
import time

HOST, PORT = '127.0.0.1', 8888
SIZE_MB = 2

dag = {'dagData': {
    'taskName': 'blobx', 'taskId': '1', 'taskDesc': 'cross-device dataplane test',
    'taskCreateTime': time.time(), 'taskPolicy': 'p1',
    'nodes': [
        {'id': 1, 'name': 'Producer',
         'form': {'beforeNodes': [], 'afterNodes': [2],
                  'taskAlgorithm': 'Algorithm_ProduceBlob', 'taskVersion': '1.0',
                  'taskInputInfo': {'inDataPath': './data/', 'outDataPath': './data/', 'size_mb': SIZE_MB},
                  'details': 'produce', 'directAllocationId': 1, 'deviceParam': {}},
         'result': {'taskResultInfo': '', 'status': 1, 'delay': 0, 'deviceId': 1001}},
        {'id': 2, 'name': 'Consumer',
         'form': {'beforeNodes': [1], 'afterNodes': [],
                  'taskAlgorithm': 'Algorithm_ConsumeBlob', 'taskVersion': '1.0',
                  'taskInputInfo': {'inDataPath': './data/', 'outDataPath': './data/'},
                  'details': 'consume', 'directAllocationId': 2, 'deviceParam': {}},
         'result': {'taskResultInfo': '', 'status': 1, 'delay': 0, 'deviceId': 1001}},
    ]}}

holder = {}
done = threading.Event()

try:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 128000000)
    sock.connect((HOST, PORT))
except Exception as e:
    print(f"[FAIL] 连不上 server {HOST}:{PORT} ({e})")
    sys.exit(1)


def reader():
    sock.settimeout(60)
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
                print('[reader] server ACK dagRspStatus =', obj['dagRspStatus'])
            elif 'dagResult' in obj:
                holder['r'] = obj['dagResult']
                done.set()
                break
    except socket.timeout:
        print('[reader] TIMEOUT 等待 dagResult')
    finally:
        done.set()


threading.Thread(target=reader, daemon=True).start()
sock.send(json.dumps({'user_beat': {'name': 'blobtester'}}).encode('utf-8'))
time.sleep(0.6)
sock.send(json.dumps(dag).encode('utf-8'))
print(f"[inject] 提交 Producer->Consumer DAG (size={SIZE_MB}MB), 等待结果...")
done.wait(timeout=60)
time.sleep(0.3)
try:
    sock.close()
except Exception:
    pass

res = holder.get('r')
if not res:
    print('[FAIL] 未收到 dagResult')
    sys.exit(1)
print(json.dumps(res, ensure_ascii=False, indent=2))

nodes = {n.get('name'): n for n in res.get('nodes', [])}
prod = nodes.get('Producer', {})
cons = nodes.get('Consumer', {})
prod_info = str(prod.get('ResultInfo', ''))
cons_info = str(cons.get('ResultInfo', ''))
prod_dev = prod.get('result', {}).get('deviceId')
cons_dev = cons.get('result', {}).get('deviceId')

problems = []
m = re.search(r'sha=([0-9a-f]+)', prod_info)
if not m:
    problems.append(f"Producer 未产出 sha: {prod_info!r}")
else:
    sha = m.group(1)
    if sha not in cons_info:
        problems.append(f"Consumer 的 sha 与 Producer 不一致 (sha={sha}); consumer={cons_info!r}")
if prod_dev == cons_dev:
    problems.append(f"两节点跑在同一设备({prod_dev})，未发生跨设备传输；本测试需要不同设备")
if 'ERROR' in prod_info or 'ERROR' in cons_info or '失败' in cons_info:
    problems.append(f"算法返回错误: producer={prod_info!r} consumer={cons_info!r}")

if problems:
    print('\n[FAIL] ' + '\n[FAIL] '.join(problems))
    sys.exit(1)

print(f"\n[PASS] 跨设备数据面 OK | Producer@{prod_dev} -> Consumer@{cons_dev} | sha 一致")
sys.exit(0)
