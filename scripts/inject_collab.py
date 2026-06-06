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
    for idx in range(N):
        true = int(testset[idx][1])
        res = submit(collab_dag(idx))
        if not res:
            print(f"image#{idx:<4} -> (no result / timeout)", flush=True)
            continue
        fusion = next((n for n in res.get('nodes', []) if n.get('name') == 'Fusion'), {})
        info = str(fusion.get('ResultInfo', ''))
        fe = [n for n in res.get('nodes', []) if n.get('name', '').startswith('FE')]
        devs = sorted({n.get('result', {}).get('deviceId') for n in fe})
        m = re.search(r'class=(\d+)', info)
        pred = int(m.group(1)) if m else -1
        ok = (pred == true)
        correct += 1 if ok else 0
        done += 1
        flag = 'OK ' if ok else ' x '
        print(f"image#{idx:<4} true={CLASSES[true]:<11} pred={CLASSES[pred] if pred>=0 else '?':<11} "
              f"[{flag}]  (extractors on {devs})", flush=True)
    if done:
        print(f"\n=== accuracy: {correct}/{done} = {correct / done:.0%}  (random baseline = 10%) ===", flush=True)
    return 0 if done == N else 1


if __name__ == '__main__':
    sys.exit(main())
