"""数据面 QUIC 客户端：从某台 device 拉取一个 artifact。

fetch_blob 是同步封装（内部跑一个 asyncio 事件循环），可直接在 device_client 的
工作线程里调用。返回 {'bytes', 'elapsed', 'dest'} 供监测/打点。
"""
import asyncio
import os
import ssl
import time

from aioquic.asyncio import connect
from aioquic.asyncio.protocol import QuicConnectionProtocol
from aioquic.quic.configuration import QuicConfiguration
from aioquic.quic.events import StreamDataReceived

from config.dataplane_config import ALPN

_NOT_FOUND = (1 << 64) - 1


class _BlobClientProtocol(QuicConnectionProtocol):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._recv = {}      # stream_id -> 收到的字节
        self._waiters = {}   # stream_id -> Future

    async def fetch(self, artifact_id: str) -> bytes:
        sid = self._quic.get_next_available_stream_id()
        fut = asyncio.get_event_loop().create_future()
        self._waiters[sid] = fut
        self._recv[sid] = b''
        self._quic.send_stream_data(sid, artifact_id.encode('utf-8'), end_stream=True)
        self.transmit()
        data = await fut

        if len(data) < 8:
            raise IOError(f"响应过短: {len(data)} bytes")
        length = int.from_bytes(data[:8], 'big')
        if length == _NOT_FOUND:
            raise FileNotFoundError(f"对端无此 artifact: {artifact_id}")
        body = data[8:]
        if len(body) != length:
            raise IOError(f"传输不完整: 期望 {length}, 实得 {len(body)}")
        return body

    def quic_event_received(self, event):
        if isinstance(event, StreamDataReceived):
            sid = event.stream_id
            self._recv[sid] = self._recv.get(sid, b'') + event.data
            if event.end_stream:
                w = self._waiters.pop(sid, None)
                if w is not None and not w.done():
                    w.set_result(self._recv.pop(sid, b''))


async def _fetch_async(host: str, port: int, artifact_id: str, timeout: float) -> bytes:
    config = QuicConfiguration(is_client=True, alpn_protocols=[ALPN])
    config.verify_mode = ssl.CERT_NONE  # 自签证书，原型阶段不校验
    async with connect(host, port, configuration=config,
                       create_protocol=_BlobClientProtocol) as protocol:
        return await asyncio.wait_for(protocol.fetch(artifact_id), timeout)


def fetch_blob(host: str, port: int, artifact_id: str,
               dest_path: str = None, timeout: float = 120) -> dict:
    """从 host:port 拉取 artifact_id；若给了 dest_path 则写盘。返回传输统计。"""
    t0 = time.time()
    data = asyncio.run(_fetch_async(host, port, artifact_id, timeout))
    elapsed = time.time() - t0
    if dest_path:
        d = os.path.dirname(os.path.abspath(dest_path))
        if d:
            os.makedirs(d, exist_ok=True)
        with open(dest_path, 'wb') as f:
            f.write(data)
    return {'bytes': len(data), 'elapsed': elapsed, 'dest': dest_path}
