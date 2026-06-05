"""OpenSAI end-to-end smoke test.

Submits a pure-Python 2-node DAG (Pi -> Sleep) to a running server.py and
asserts the pipeline works:
  * the server ACKs the submission,
  * a final dagResult comes back,
  * Node1 (Algorithm_Pi_Calculation) returns a number near pi,
  * the reported finishedTime is a small elapsed value (regression guard for
    the server.py `float(task_id[12:])` bug).

Exit code 0 = PASS, 1 = FAIL. Run via scripts/smoke_test.ps1 (uses the venv).
"""
import socket, json, time, threading, sys

HOST, PORT = '127.0.0.1', 8888
TASK_NAME = 'pitest'          # 6 chars on purpose: trips the old [12:] slice bug

dag_data = {'dagData': {
    'taskName': TASK_NAME, 'taskId': '1', 'taskDesc': 'e2e smoke test',
    'taskCreateTime': time.time(), 'taskPolicy': 'p1',
    'nodes': [
        {'id': 1, 'name': 'Node1',
         'form': {'beforeNodes': [], 'afterNodes': [2],
                  'taskAlgorithm': 'Algorithm_Pi_Calculation', 'taskVersion': '1.0',
                  'taskInputInfo': {'inDataPath': './data/', 'outDataPath': './data/', 'count_num': 500000},
                  'details': 'pi', 'directAllocationId': -1, 'deviceParam': {}},
         'result': {'taskResultInfo': '', 'status': 1, 'delay': 0, 'deviceId': 1001}},
        {'id': 2, 'name': 'Node2',
         'form': {'beforeNodes': [1], 'afterNodes': [],
                  'taskAlgorithm': 'Algorithm_Sleep_1', 'taskVersion': '1.0',
                  'taskInputInfo': {'inDataPath': './data/', 'outDataPath': './data/', 'sleep_time': 1},
                  'details': 'sleep', 'directAllocationId': -1, 'deviceParam': {}},
         'result': {'taskResultInfo': '', 'status': 1, 'delay': 0, 'deviceId': 1001}},
    ]}}

result_holder = {}
done = threading.Event()

try:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 128000000)
    sock.connect((HOST, PORT))
except Exception as e:
    print(f"[FAIL] cannot connect to server at {HOST}:{PORT} ({e}). Is it running?")
    sys.exit(1)

def reader():
    sock.settimeout(45)
    try:
        while not done.is_set():
            data = sock.recv(65535)
            if not data:
                print('[reader] server closed connection'); break
            try:
                obj = json.loads(data.decode('utf-8', 'replace'))
            except Exception:
                continue
            if 'dagRspStatus' in obj:
                print('[reader] server ACK: dagRspStatus =', obj['dagRspStatus'])
            elif 'dagResult' in obj:
                result_holder['dagResult'] = obj['dagResult']
                done.set(); break
    except socket.timeout:
        print('[reader] TIMEOUT (45s) waiting for dagResult')
    finally:
        done.set()

threading.Thread(target=reader, daemon=True).start()

# register as a user first, then (spaced, to avoid TCP coalescing) submit the DAG
sock.send(json.dumps({'user_beat': {'name': 'smoketester'}}).encode('utf-8'))
time.sleep(0.6)
sock.send(json.dumps(dag_data).encode('utf-8'))
print(f"[smoke] submitted DAG '{TASK_NAME}' (Pi -> Sleep), waiting for result...")

done.wait(timeout=45)
time.sleep(0.2)
try:
    sock.close()
except Exception:
    pass

# ---- evaluate ----
res = result_holder.get('dagResult')
if not res:
    print('[FAIL] no dagResult received within timeout')
    sys.exit(1)

print(json.dumps(res, ensure_ascii=False, indent=2))

problems = []
nodes = {n.get('name'): n for n in res.get('nodes', [])}

pi = nodes.get('Node1', {}).get('ResultInfo')
if not (isinstance(pi, (int, float)) and 2.5 < float(pi) < 3.6):
    problems.append(f"Node1 result is not a pi-like number: {pi!r}")

ft = res.get('finishedTime')
if not (isinstance(ft, (int, float)) and 0 <= float(ft) < 120):
    problems.append(f"finishedTime should be a small elapsed time (<120s), got {ft!r} "
                    f"-- the float(task_id[12:]) bug?")

if problems:
    print('\n[FAIL] ' + '\n[FAIL] '.join(problems))
    sys.exit(1)

print(f"\n[PASS] DAG completed end-to-end | Node1 pi={pi} | finishedTime={ft:.3f}s")
sys.exit(0)
