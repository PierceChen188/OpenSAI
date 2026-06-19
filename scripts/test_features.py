"""断言式回归：覆盖本版本新增能力（数据面多链路 + 存储档位），与 makespan 实验不同——这里只判 PASS/FAIL。

单栈(2 设备)跑一个小 DAG：ProduceBlob(size_mb=4, storage_mb=200, 双网卡, pin=1) -> ConsumeBlob(pin=2)。
一次提交同时考察三件事，全部断言：
  1) 传输正确：errs==0（含 ConsumeBlob 的 sha 校验）。
  2) 多链路生效：开 OPENSAI_MULTINIC 后大脑下发 >=2 个源（server 日志 [multipath] 多链路源=N>=2）。
  3) 存储档位生效：声明 storage_mb 后大脑记账（server 日志 [alloc] 存储）。

退出码非 0 表示有断言失败，可直接接 CI。
Run:  .venv/Scripts/python.exe scripts/test_features.py
"""
import os
import re
import socket
import sys
import time

try:
    sys.stdout.reconfigure(encoding='utf-8')
except Exception:
    pass

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(ROOT, 'Offloading-System'))
import test_suite as ts          # noqa: E402
import inject_collab             # noqa: E402

SERVER_LOG = os.path.join(ROOT, '.run', 'server.err.log')


def lan_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(('8.8.8.8', 80))
        ip = s.getsockname()[0]
        return ip if ip and not ip.startswith('127.') else None
    except Exception:
        return None
    finally:
        s.close()


def build_dag():
    n = inject_collab._node
    nodes = [
        n(1, 'Prod', 'Algorithm_ProduceBlob', [], [2], 1, {'size_mb': 4, 'storage_mb': 200}),
        n(2, 'Cons', 'Algorithm_ConsumeBlob', [1], [], 2, {}),
    ]
    return {'dagData': {'taskName': 'feat1', 'taskId': '1', 'taskDesc': '',
                        'taskCreateTime': time.time(), 'taskPolicy': 'p', 'nodes': nodes}}


def read_server_log():
    for enc in ('gbk', 'utf-8'):
        try:
            return open(SERVER_LOG, encoding=enc, errors='replace').read()
        except OSError:
            return ''
    return ''


def main():
    ip2 = lan_ip()
    if not ip2:
        print("SKIP: 无法获取本机 LAN IP（单机模拟双网卡需要），跳过多链路断言。", flush=True)
        return 0
    nic = f"127.0.0.1,{ip2}"
    ts.kill_stale()
    t0 = time.time()
    while ts.port_in_use() and time.time() - t0 < 12:
        ts.kill_stale(); time.sleep(1)
    import redis
    redis.Redis(host='127.0.0.1', port=6379, db=0).flushall()
    # OPENSAI_MULTINIC* 由 server(_multinic_sources) 读取；OPENSAI_NIC_IPS 由 device 上报。
    procs = [ts.start('server.py', {'OPENSAI_PLACEMENT': 'plan', 'OPENSAI_MULTINIC': '1',
                                    'OPENSAI_MULTINIC_MIN_MB': '1'}, 'server')]
    time.sleep(3)
    for i in range(2):
        procs.append(ts.start('device_client.py',
                              {'OPENSAI_DATA_PORT': str(9980 + i), 'OPENSAI_PROBE_INTERVAL': '600',
                               'OPENSAI_TASK_WORKERS': '2', 'OPENSAI_POOL_WARMUP_S': '45',
                               'OPENSAI_NIC_IPS': nic}, f'dev{9980 + i}'))
    checks = []
    try:
        r = redis.Redis(host='127.0.0.1', port=6379, db=0)
        t0 = time.time()
        while int(r.scard('DEVICE_ONLINE_SET')) < 2 and time.time() - t0 < 90:
            time.sleep(2)
        if int(r.scard('DEVICE_ONLINE_SET')) < 2:
            print("FAIL: devices did not come online"); return 1
        time.sleep(5)
        res = inject_collab.submit(build_dag(), timeout=120)
        if not res:
            print("FAIL: no DAG result (timeout)"); return 1
        nodes = res.get('nodes', [])
        got = sum(1 for nd in nodes if str(nd.get('ResultInfo', '')))
        errs = sum(1 for nd in nodes if 'ERROR' in str(nd.get('ResultInfo', ''))
                   or 'MISSING' in str(nd.get('ResultInfo', ''))) + (2 - got)
        time.sleep(1)
        log = read_server_log()
        srcs = [int(m) for m in re.findall(r'多链路源=(\d+)', log)]
        max_src = max(srcs) if srcs else 0
        storage_alloc = ('[alloc]' in log and '存储' in log)

        checks.append(('transfer correct (errs==0)', errs == 0, f"errs={errs}"))
        checks.append(('multipath >=2 sources', max_src >= 2, f"max 多链路源={max_src}"))
        checks.append(('storage alloc accounted', storage_alloc, f"[alloc]存储={storage_alloc}"))
    finally:
        for p in procs:
            ts.kill_tree(p)
        ts.kill_stale()

    print("\n=== feature assertions ===", flush=True)
    ok = True
    for name, passed, detail in checks:
        print(f"  [{'PASS' if passed else 'FAIL'}] {name}  ({detail})", flush=True)
        ok = ok and passed
    print(f"\n{'==== FEATURES: ALL PASSED ====' if ok else '==== FEATURES: FAILED ===='}", flush=True)
    return 0 if ok else 1


if __name__ == '__main__':
    sys.exit(main())
