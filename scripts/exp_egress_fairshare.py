"""A1 demo: a device's egress bandwidth is FAIRLY SHARED across concurrent streams.

One ArtifactServer with a device egress budget (OPENSAI_DATA_RATE_MBPS = 4 MB/s):
  1) SOLO     : one consumer fetches 6MB        -> ~full budget (4 MB/s)
  2) SHARED   : two consumers fetch 6MB each    -> ~2 MB/s each; AGGREGATE ~4
                (old behavior: each grabbed the full 4 -> aggregate 8 = 2x over the cap)
  3) REBALANCE: 6MB + 2MB concurrently          -> small finishes first, big speeds 2->4

Standalone (no server/redis): drives ArtifactServer + fetch_blob directly so the
numbers reflect the sender-side fair-share alone, not full-stack scheduling jitter.
"""
import os
import sys
import tempfile
import threading
import time

os.environ['OPENSAI_DATA_RATE_MBPS'] = '4'   # 设备出口预算 4 MB/s（须在导入 quic_server 前设）

try:
    sys.stdout.reconfigure(encoding='utf-8')
except Exception:
    pass

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(ROOT, 'Offloading-System'))

from network.dataplane.quic_client import fetch_blob          # noqa: E402
from network.dataplane.quic_server import ArtifactServer      # noqa: E402

HOST, PORT = '127.0.0.1', 9555


def make_blob(d, name, mb):
    p = os.path.join(d, name)
    with open(p, 'wb') as f:
        f.write(os.urandom(mb * 1024 * 1024))
    return p


def fetch(aid, out, key):
    t0 = time.time()
    r = fetch_blob(HOST, PORT, aid, timeout=120)
    dt = max(1e-6, time.time() - t0)
    out[key] = (r['bytes'], dt, r['bytes'] / 1024 / 1024 / dt)


def both(a_id, b_id):
    out = {}
    ths = [threading.Thread(target=fetch, args=(a_id, out, 'a')),
           threading.Thread(target=fetch, args=(b_id, out, 'b'))]
    w0 = time.time()
    for t in ths:
        t.start()
    for t in ths:
        t.join()
    return out, time.time() - w0


def main():
    d = tempfile.mkdtemp(prefix='a1_')
    srv = ArtifactServer(host=HOST, port=PORT)
    srv.register('big', make_blob(d, 'big', 6))
    srv.register('big2', make_blob(d, 'big2', 6))
    srv.register('small', make_blob(d, 'small', 2))
    srv.start(wait=True, timeout=10)
    time.sleep(0.4)
    print(f"设备出口预算 OPENSAI_DATA_RATE_MBPS = {os.environ['OPENSAI_DATA_RATE_MBPS']} MB/s\n")

    # 1) SOLO --------------------------------------------------------------
    solo = {}
    fetch('big', solo, 'x')
    _, dt, rate = solo['x']
    print(f"[1 单流]     拉 6MB: {dt:.2f}s, 速率 {rate:.2f} MB/s   (期望≈4：独占整个出口预算)")

    # 2) SHARED: 2x 6MB concurrently --------------------------------------
    out, wall = both('big', 'big2')
    (ba, da, ra), (bb, db, rb) = out['a'], out['b']
    agg = (ba + bb) / 1024 / 1024 / wall
    print(f"\n[2 双流并发] 各拉 6MB:")
    print(f"    流A: {da:.2f}s, {ra:.2f} MB/s    流B: {db:.2f}s, {rb:.2f} MB/s")
    print(f"    => 并发期间各≈2 MB/s（4 平分给 2 条）；总吞吐 {agg:.2f} MB/s")
    print(f"    => 守住 4 MB/s 出口（旧法两条各占满=8，超额 2 倍）")

    # 3) REBALANCE: 6MB + 2MB concurrently --------------------------------
    out, _ = both('big', 'small')
    _, dbig, rbig = out['a']
    _, dsm, rsm = out['b']
    print(f"\n[3 动态再平分] 6MB 与 2MB 并发:")
    print(f"    2MB(小): {dsm:.2f}s, {rsm:.2f} MB/s   -> 先结束")
    print(f"    6MB(大): {dbig:.2f}s, {rbig:.2f} MB/s  -> 小流结束后自动提速(2→4)，故平均 >2")


if __name__ == '__main__':
    main()
