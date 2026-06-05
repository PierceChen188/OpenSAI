"""数据面单元测试：起一个 ArtifactServer 服务一个临时文件，用 fetch_blob 经 QUIC
拉回来，断言字节一致。在单机 loopback 上即可完整验证传输逻辑。

PASS -> exit 0, FAIL -> exit 1。
"""
import hashlib
import logging
import os
import sys
import tempfile

# 让脚本能 import Offloading-System 下的包
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(_ROOT, 'Offloading-System'))

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

from network.dataplane.quic_server import ArtifactServer  # noqa: E402
from network.dataplane.quic_client import fetch_blob       # noqa: E402

PORT = 19999
SIZE = 3 * 1024 * 1024  # 3 MB


def main():
    data = os.urandom(SIZE)
    src = tempfile.NamedTemporaryFile(delete=False)
    src.write(data)
    src.close()
    dest = src.name + '.fetched'

    server = ArtifactServer(host='127.0.0.1', port=PORT)
    server.register('test:1', src.name)
    server.start()
    print(f"[test] server up on 127.0.0.1:{PORT}, serving {SIZE} bytes")

    # 正常拉取
    res = fetch_blob('127.0.0.1', PORT, 'test:1', dest)
    with open(dest, 'rb') as f:
        got = f.read()
    match = got == data
    thr = SIZE / res['elapsed'] / 1024 / 1024 if res['elapsed'] > 0 else 0
    print(f"[test] fetched {res['bytes']} bytes in {res['elapsed']:.3f}s "
          f"(~{thr:.1f} MB/s), sha-match={match}")

    # 不存在的 artifact 应抛 FileNotFoundError
    notfound_ok = False
    try:
        fetch_blob('127.0.0.1', PORT, 'no_such', dest + '.x')
    except FileNotFoundError:
        notfound_ok = True
    print(f"[test] not-found handling correct = {notfound_ok}")

    # 清理
    for p in (src.name, dest):
        try:
            os.remove(p)
        except OSError:
            pass

    ok = match and notfound_ok
    print('\n[PASS] data-plane QUIC round-trip OK' if ok else '\n[FAIL] data-plane test failed')
    return 0 if ok else 1


if __name__ == '__main__':
    sys.exit(main())
