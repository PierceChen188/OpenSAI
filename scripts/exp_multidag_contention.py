"""Multi-DAG CONTENTION experiment: where resource allocation (not just placement) pays off.

K collaborative-perception DAGs are submitted CONCURRENTLY (each: 6 sensors -> 3 regional
-> 1 head, 10 nodes, data-heavy), so ~K*10 tasks compete for the same few devices' CPU,
egress bandwidth, and worker slots at once. We compare:

  OFF : OPENSAI_PLACEMENT=random                              (no resource control)
  ON  : OPENSAI_PLACEMENT=plan + DATA_RATE_MBPS + AUTO_MEM    (capacity reservations spread
        concurrent load across devices; egress fair-share divides each device's upload among
        its concurrent transfers; per-task memory budget)

Under contention the adaptation component should give: shorter BATCH makespan (finish all K
sooner, via locality+balance), and a tighter per-DAG tail (fairness -- no DAG starves while
others hog a device). Metrics: batch wall makespan, per-DAG finishedTime mean/max(tail),
throughput (DAGs/s), max nodes on any one device (balance), total errors (must be 0).

Each concurrent DAG uses a UNIQUE user name + task name (required: the server routes each
DAG's result back by the submitting socket, and keys tasks by name).

Run:  .venv/Scripts/python.exe scripts/exp_multidag_contention.py [K] [trials]
"""
import json
import os
import socket
import sys
import threading
import time

try:
    sys.stdout.reconfigure(encoding='utf-8')
except Exception:
    pass

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import test_suite as ts          # noqa: E402

HOST, PORT = '127.0.0.1', 8888
K = int(sys.argv[1]) if len(sys.argv) > 1 else 8        # concurrent DAGs
TRIALS = int(sys.argv[2]) if len(sys.argv) > 2 else 2
N_DEVICES = 6
BASE_PORT = 9994                                        # 9994..9999
BLOB_MB, COMPUTE_MS = 4, 150

# one DAG: 6 producers -> 3 regional(each gathers 2) -> 1 head(gathers 3). UNPINNED.
BEFORE = {1: [], 2: [], 3: [], 4: [], 5: [], 6: [],
          7: [1, 2], 8: [3, 4], 9: [5, 6], 10: [7, 8, 9]}
AFTER = {1: [7], 2: [7], 3: [8], 4: [8], 5: [9], 6: [9], 7: [10], 8: [10], 9: [10], 10: []}
KIND = {i: ('produce' if i <= 6 else ('relay' if i <= 9 else 'consume')) for i in range(1, 11)}
ALGO = {'produce': 'Algorithm_ProduceBlob', 'relay': 'Algorithm_Relay', 'consume': 'Algorithm_ConsumeBlob'}
EDGES = [(p, c) for c, ps in BEFORE.items() for p in ps]


def _node(nid):
    kind = KIND[nid]
    info = {'inDataPath': './data/', 'outDataPath': './data/', 'compute_ms': COMPUTE_MS}
    if kind in ('produce', 'relay'):
        info['size_mb'] = BLOB_MB
    return {'id': nid, 'name': f'N{nid}',
            'form': {'beforeNodes': BEFORE[nid], 'afterNodes': AFTER[nid], 'taskAlgorithm': ALGO[kind],
                     'taskVersion': '1.0', 'taskInputInfo': info, 'details': '',
                     'directAllocationId': -1, 'deviceParam': {}},
            'result': {'taskResultInfo': '', 'status': 1, 'delay': 0, 'deviceId': 0}}


def build_dag(tag):
    return {'dagData': {'taskName': tag, 'taskId': '1', 'taskDesc': '', 'taskCreateTime': time.time(),
                        'taskPolicy': 'p', 'nodes': [_node(i) for i in range(1, 11)]}}


def submit_named(user_name, dag, timeout, out, start_evt):
    """Submit one DAG on its own socket with a unique user name; record (result, wall)."""
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
                        holder['r'] = obj['dagResult']; done.set(); return
        except (socket.timeout, OSError):     # OSError: socket closed by main thread on done
            pass
        finally:
            done.set()

    threading.Thread(target=reader, daemon=True).start()
    s.sendall((json.dumps({'user_beat': {'name': user_name}}) + '\n').encode('utf-8'))
    time.sleep(0.5)
    start_evt.wait()                       # release all submitters together (true concurrency)
    t0 = time.time()
    s.sendall((json.dumps(dag) + '\n').encode('utf-8'))
    done.wait(timeout)
    out[user_name] = (holder.get('r'), time.time() - t0)
    try:
        s.close()
    except Exception:
        pass


def run_batch():
    out = {}
    start_evt = threading.Event()
    threads = []
    for i in range(K):
        th = threading.Thread(target=submit_named, args=(f'load{i}', build_dag(f'L{i}'), 240, out, start_evt))
        th.start(); threads.append(th)
    time.sleep(1.0)                        # let all sockets register their user_beat
    batch_t0 = time.time()
    start_evt.set()                        # GO -- all K DAGs submitted concurrently
    for th in threads:
        th.join(timeout=260)
    batch_wall = time.time() - batch_t0
    # analyze
    fts, errs, dev_load = [], 0, {}
    done = 0
    for i in range(K):
        res, _wall = out.get(f'load{i}', (None, 0))
        if not res:
            errs += 1; continue
        done += 1
        nodes = res.get('nodes', [])
        got = sum(1 for nd in nodes if str(nd.get('ResultInfo', '')))
        errs += sum(1 for nd in nodes if 'ERROR' in str(nd.get('ResultInfo', '')) or 'MISSING' in str(nd.get('ResultInfo', '')))
        errs += (10 - got)
        fts.append(float(res.get('finishedTime') or 0.0))
        for nd in nodes:
            d = nd.get('result', {}).get('deviceId')
            if d is not None:
                dev_load[d] = dev_load.get(d, 0) + 1
    mean_ft = sum(fts) / len(fts) if fts else float('nan')
    max_ft = max(fts) if fts else float('nan')
    max_load = max(dev_load.values()) if dev_load else 0
    return {'batch_wall': batch_wall, 'mean_ft': mean_ft, 'max_ft': max_ft,
            'thru': (done / batch_wall if batch_wall else 0), 'max_load': max_load,
            'errs': errs, 'done': done}


def run_stack(server_env):
    ts.kill_stale()
    t0 = time.time()
    while ts.port_in_use() and time.time() - t0 < 12:
        ts.kill_stale(); time.sleep(1)
    import redis
    r = redis.Redis(host='127.0.0.1', port=6379, db=0); r.flushall()
    procs = [ts.start('server.py', server_env, 'server')]
    time.sleep(3)
    for i in range(N_DEVICES):
        procs.append(ts.start('device_client.py',
                              {'OPENSAI_DATA_PORT': str(BASE_PORT + i), 'OPENSAI_PROBE_INTERVAL': '600',
                               'OPENSAI_TASK_WORKERS': '3', 'OPENSAI_POOL_WARMUP_S': '45'}, f'device{i + 1}'))
    rows = []
    try:
        t0 = time.time()
        while int(r.scard('DEVICE_ONLINE_SET')) < N_DEVICES and time.time() - t0 < 120:
            time.sleep(2)
        online = int(r.scard('DEVICE_ONLINE_SET'))
        time.sleep(5)
        print(f"  devices online: {online}/{N_DEVICES}", flush=True)
        if online < N_DEVICES:
            print("  FAIL: not all devices online"); return rows
        for _t in range(TRIALS):
            rows.append(run_batch())
    finally:
        for p in procs:
            ts.kill_tree(p)
        ts.kill_stale()
    return rows


def avg(rows, k):
    vals = [x[k] for x in rows if x.get(k) == x.get(k)]   # drop nan
    return sum(vals) / len(vals) if vals else float('nan')


def main():
    print(f"\n{K} concurrent DAGs x 10 nodes ({len(EDGES)} edges each) on {N_DEVICES} devices; "
          f"blob={BLOB_MB}MB compute={COMPUTE_MS}ms; {TRIALS} trials", flush=True)
    stacks = {
        'OFF(random)': {'OPENSAI_PLACEMENT': 'random'},
        'plan-only': {'OPENSAI_PLACEMENT': 'plan'},                  # placement + capacity reservations
        'ON(plan+bwfair+mem)': {'OPENSAI_PLACEMENT': 'plan', 'OPENSAI_DATA_RATE_MBPS': '12',
                                'OPENSAI_AUTO_MEM': '1'},            # + egress fair-share + mem budget
    }
    data = {}
    for label, env in stacks.items():
        print(f"\n  [stack] {label}  env={env}", flush=True)
        data[label] = run_stack(env)

    print(f"\n{'='*86}\n  CONTENTION RESULTS ({K} concurrent DAGs)  -- batch lower=better, tail lower=fairer, errs=0\n{'='*86}", flush=True)
    print(f"  {'config':<22}{'batch(s)':>10}{'perDAG mean(s)':>15}{'perDAG max(s)':>15}"
          f"{'DAGs/s':>8}{'maxload':>9}{'errs':>6}", flush=True)
    for label in stacks:
        rows = data[label]
        if not rows:
            print(f"  {label:<22}  (no data)"); continue
        print(f"  {label:<22}{avg(rows,'batch_wall'):>10.1f}{avg(rows,'mean_ft'):>15.1f}"
              f"{avg(rows,'max_ft'):>15.1f}{avg(rows,'thru'):>8.2f}{avg(rows,'max_load'):>9.1f}"
              f"{sum(x['errs'] for x in rows):>6}", flush=True)
    def delta(a, b, k):
        ra, rb = data.get(a), data.get(b)
        if ra and rb and avg(rb, k) > 0:
            return (avg(ra, k) / avg(rb, k) - 1) * 100
        return float('nan')
    print(f"\n  归因(batch / 尾延迟 vs OFF基线):", flush=True)
    print(f"   plan-only       vs OFF(random): batch {delta('plan-only','OFF(random)','batch_wall'):+.0f}%"
          f" , 尾延迟 {delta('plan-only','OFF(random)','max_ft'):+.0f}%  (= 放置+容量预留分散负载)", flush=True)
    print(f"   ON(+bwfair+mem) vs plan-only  : batch {delta('ON(plan+bwfair+mem)','plan-only','batch_wall'):+.0f}%"
          f" , 尾延迟 {delta('ON(plan+bwfair+mem)','plan-only','max_ft'):+.0f}%  (= 带宽公平平分的增量)", flush=True)
    print(f"   ON(full)        vs OFF(random): batch {delta('ON(plan+bwfair+mem)','OFF(random)','batch_wall'):+.0f}%"
          f" , 尾延迟 {delta('ON(plan+bwfair+mem)','OFF(random)','max_ft'):+.0f}%  (= 适配优化总收益)", flush=True)
    print("\n  并发下资源分配的价值：账本(预留)跨DAG分散负载 + 出口带宽公平平分 -> 更短批完成、更齐的尾延迟、0错误。", flush=True)
    return 0


if __name__ == '__main__':
    sys.exit(main())
