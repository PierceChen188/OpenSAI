"""DEMO: collaborative CIFAR-10 inference across 2 devices.

For each test image, builds a DAG:
    FeatureExtract(left half)  @ device A  --feature--+
                                                       +--> Fusion @ device C --> class
    FeatureExtract(right half) @ device B  --feature--+
The two feature tensors travel device->device over the QUIC data plane; the fusion
node concatenates them and classifies. Prints predicted vs true label + accuracy.

Run against a running 2-device stack:
    .venv/Scripts/python.exe scripts/inject_collab.py [num_images]
"""
import json
import os
import re
import socket
import sys
import threading
import time

# Make our own output encoding-safe: emit UTF-8 regardless of the console code
# page (PowerShell 5.1 defaults to a non-UTF-8 OEM page and would garble non-ASCII).
try:
    sys.stdout.reconfigure(encoding='utf-8')
except Exception:
    pass

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(_ROOT, 'Offloading-System'))

import torchvision
import torchvision.transforms as T
from device.implements.collab_model import CLASSES, data_dir

HOST, PORT = '127.0.0.1', 8888
N = int(sys.argv[1]) if len(sys.argv) > 1 else 10


def submit(dag, timeout=90):
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
                    holder['r'] = obj['dagResult']; done.set(); break
        except socket.timeout:
            pass
        finally:
            done.set()

    threading.Thread(target=reader, daemon=True).start()
    s.send(json.dumps({'user_beat': {'name': 'collab'}}).encode('utf-8'))
    time.sleep(0.5)
    s.send(json.dumps(dag).encode('utf-8'))
    done.wait(timeout)
    time.sleep(0.2)
    try:
        s.close()
    except Exception:
        pass
    return holder.get('r')


def _node(nid, name, algo, before, after, direct, extra):
    info = {'inDataPath': './data/', 'outDataPath': './data/'}
    info.update(extra)
    return {'id': nid, 'name': name,
            'form': {'beforeNodes': before, 'afterNodes': after, 'taskAlgorithm': algo,
                     'taskVersion': '1.0', 'taskInputInfo': info, 'details': '',
                     'directAllocationId': direct, 'deviceParam': {}},
            'result': {'taskResultInfo': '', 'status': 1, 'delay': 0, 'deviceId': 0}}


def collab_dag(idx):
    return {'dagData': {'taskName': f'collab{idx}', 'taskId': '1', 'taskDesc': '',
                        'taskCreateTime': time.time(), 'taskPolicy': 'p', 'nodes': [
        _node(1, 'FE_left', 'Algorithm_FeatureExtract', [], [3], 1, {'image_index': idx, 'half': 'left'}),
        _node(2, 'FE_right', 'Algorithm_FeatureExtract', [], [3], 2, {'image_index': idx, 'half': 'right'}),
        _node(3, 'Fusion', 'Algorithm_Fusion', [1, 2], [], 1, {}),
    ]}}


def main():
    testset = torchvision.datasets.CIFAR10(root=data_dir(), train=False, download=False, transform=T.ToTensor())
    print(f"\n=== Collaborative CIFAR-10 inference across 2 devices ({N} images) ===\n", flush=True)
    correct = 0
    done = 0
    cross = 0

    # Device IDs may be non-ASCII (e.g. the Chinese display name). Map each to a
    # stable ASCII tag (dev1/dev2/...) so the demo output never depends on the console encoding.
    dev_label = {}

    def label(dev_id):
        if dev_id not in dev_label:
            dev_label[dev_id] = f"dev{len(dev_label) + 1}"
        return dev_label[dev_id]

    def dev_of(nodes, node_name):
        n = next((x for x in nodes if x.get('name') == node_name), {})
        return n.get('result', {}).get('deviceId')

    for idx in range(N):
        true = int(testset[idx][1])
        res = submit(collab_dag(idx))
        if not res:
            print(f"image#{idx:<4} -> (no result / timeout)", flush=True)
            continue
        nodes = res.get('nodes', [])
        info = str(next((n for n in nodes if n.get('name') == 'Fusion'), {}).get('ResultInfo', ''))
        ldev, rdev = dev_of(nodes, 'FE_left'), dev_of(nodes, 'FE_right')
        m = re.search(r'class=(\d+)', info)
        pred = int(m.group(1)) if m else -1
        ok = (pred == true)
        correct += 1 if ok else 0
        cross += 1 if (ldev is not None and ldev != rdev) else 0
        done += 1
        flag = 'OK' if ok else ' x'
        print(f"image#{idx:<4} true={CLASSES[true]:<11} pred={CLASSES[pred] if pred >= 0 else '?':<11} "
              f"[{flag}]  left@{label(ldev)} right@{label(rdev)}", flush=True)
    if done:
        print(f"\n=== accuracy: {correct}/{done} = {correct / done:.0%}  (random baseline = 10%) ===", flush=True)
        print(f"=== cross-device: {cross}/{done} images had their two halves on different devices ===", flush=True)
    return 0 if done == N else 1


if __name__ == '__main__':
    sys.exit(main())
