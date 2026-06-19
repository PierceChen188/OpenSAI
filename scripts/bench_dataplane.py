"""数据面客户端微基准：量化单产物吞吐、顺序 vs 并行多取、同源重复取的握手开销。
独立运行：直接驱动 ArtifactServer + fetch_blob（不起完整 server/device 栈）。

用法:  .venv/Scripts/python.exe scripts/bench_dataplane.py
改造前后各跑一次对比（连接复用看 [D]，并发看 [C]）。
"""
import hashlib
import os
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor

try:
    sys.stdout.reconfigure(encoding='utf-8')
except Exception:
    pass

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(ROOT, 'Offloading-System'))

from network.dataplane.quic_client import fetch_blob, fetch_striped   # noqa: E402
from network.dataplane.quic_server import ArtifactServer      # noqa: E402

HOST, PORT = '127.0.0.1', 9557
N = 8           # 并发/多取的产物个数
MED_MB = 4      # 每个中等产物大小


def make_blob(d, name, mb):
    p = os.path.join(d, name)
    with open(p, 'wb') as f:
        f.write(os.urandom(mb * 1024 * 1024))
    return p


def sha16(path):
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for b in iter(lambda: f.read(1 << 20), b''):
            h.update(b)
    return h.hexdigest()[:16]


def main():
    d = tempfile.mkdtemp(prefix='bench_dp_')
    srv = ArtifactServer(host=HOST, port=PORT)
    srv.register('big', make_blob(d, 'big', 32))
    for i in range(N):
        srv.register(f'm{i}', make_blob(d, f'm{i}', MED_MB))
    srv.register('small', make_blob(d, 'small', 1))
    srv.start(wait=True, timeout=10)
    time.sleep(0.4)
    print(f"数据面微基准 (localhost; N={N}, 中等产物={MED_MB}MB)\n", flush=True)

    # [A] 单个 32MB 产物吞吐
    t0 = time.time(); fetch_blob(HOST, PORT, 'big', timeout=120); dtA = time.time() - t0
    print(f"[A] 单个 32MB:        {dtA:.2f}s, {32 / dtA:.1f} MB/s", flush=True)

    # [B] N x 中等产物 顺序拉取（模拟当前“多前驱顺序拉”）
    t0 = time.time()
    for i in range(N):
        fetch_blob(HOST, PORT, f'm{i}', timeout=120)
    dtB = time.time() - t0
    print(f"[B] {N}x{MED_MB}MB 顺序:    {dtB:.2f}s, 合计 {N * MED_MB / dtB:.1f} MB/s", flush=True)

    # [C] N x 中等产物 并行拉取
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=N) as ex:
        list(ex.map(lambda i: fetch_blob(HOST, PORT, f'm{i}', timeout=120), range(N)))
    dtC = time.time() - t0
    print(f"[C] {N}x{MED_MB}MB 并行:    {dtC:.2f}s, 合计 {N * MED_MB / dtC:.1f} MB/s   (并行/顺序 加速 {dtB / max(dtC,1e-6):.1f}x)", flush=True)

    # [D] 1MB x20 同源重复拉取（暴露每次握手开销 -> 连接复用收益看这里）
    t0 = time.time()
    for _ in range(20):
        fetch_blob(HOST, PORT, 'small', timeout=120)
    dtD = time.time() - t0
    print(f"[D] 1MB x20 同源重复:  {dtD:.2f}s, 平均 {dtD / 20 * 1000:.0f} ms/次", flush=True)

    # ===== [E]/[F] 中继多链路聚合：每路限速 RATE MB/s，直连 vs 直连+中继(回源 A) =====
    # 用每路限速模拟“单链路带宽受限”：两条受限路径(直连 + 经中继)应聚合到 ~2x 单路。
    RATE = 8.0
    mp_mb = 40
    mp_path = make_blob(d, 'mp', mp_mb)
    srv.register('mp', mp_path)
    relay = ArtifactServer(host=HOST, port=PORT + 1)     # 中继：不注册 'mp'，请求时回源到 A
    relay.start(wait=True, timeout=10)
    time.sleep(0.4)
    n_mp = os.path.getsize(mp_path)
    orig = sha16(mp_path)

    e_out = os.path.join(d, 'e_out')
    t0 = time.time()
    fetch_striped([{'host': HOST, 'port': PORT}], 'mp', e_out, n_mp, rate_mbps=RATE)
    dtE = time.time() - t0
    print(f"[E] 仅直连 @{RATE:.0f}MB/s:       {dtE:.2f}s, {mp_mb / dtE:.1f} MB/s   "
          f"sha {'OK' if sha16(e_out) == orig else 'MISMATCH'}", flush=True)

    f_out = os.path.join(d, 'f_out')
    sources = [{'host': HOST, 'port': PORT},
               {'host': HOST, 'port': PORT + 1, 'origin_ip': HOST, 'origin_port': PORT}]
    t0 = time.time()
    fetch_striped(sources, 'mp', f_out, n_mp, rate_mbps=RATE)
    dtF = time.time() - t0
    print(f"[F] 直连+中继 @{RATE:.0f}MB/s/路:  {dtF:.2f}s, {mp_mb / dtF:.1f} MB/s   "
          f"(聚合 {dtE / max(dtF, 1e-6):.1f}x)   sha {'OK' if sha16(f_out) == orig else 'MISMATCH'}", flush=True)


if __name__ == '__main__':
    main()
