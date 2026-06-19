"""数据面 QUIC 客户端：从某台 device 拉取一个 artifact。

fetch_blob 是同步封装，可直接在 device_client 的工作线程里调用，返回 {'bytes','elapsed','dest'}。

P0-1a 吞吐地基：内部维护**一个常驻 asyncio 事件循环线程** + **按源设备 (host,port) 复用的 QUIC
长连接池**——避免每次拉取都新建事件循环并重新握手；并发调用(多前驱/多产物)在该循环上以多 stream
并行。对外接口(fetch_blob 同步签名)不变，调用方无需改动。
"""
import asyncio
import json
import logging
import os
import ssl
import threading
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

    async def fetch(self, artifact_id: str, rate_mbps=None, offset=None, length=None) -> bytes:
        sid = self._quic.get_next_available_stream_id()
        fut = asyncio.get_running_loop().create_future()
        self._waiters[sid] = fut
        self._recv[sid] = bytearray()
        # 带限速/范围时发结构化请求；否则发裸 artifact_id（向后兼容）。
        if (rate_mbps and rate_mbps > 0) or offset or (length is not None):
            req_obj = {'id': artifact_id}
            if rate_mbps and rate_mbps > 0:
                req_obj['rate_mbps'] = rate_mbps
            if offset:
                req_obj['offset'] = int(offset)
            if length is not None:
                req_obj['length'] = int(length)
            req = json.dumps(req_obj).encode('utf-8')
        else:
            req = artifact_id.encode('utf-8')
        self._quic.send_stream_data(sid, req, end_stream=True)
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
            # 用 bytearray 累加（摊还 O(1)）。早先用 bytes 拼接是 O(n^2)。
            buf = self._recv.get(sid)
            if buf is None:
                buf = bytearray()
                self._recv[sid] = buf
            buf.extend(event.data)
            if event.end_stream:
                w = self._waiters.pop(sid, None)
                if w is not None and not w.done():
                    w.set_result(bytes(self._recv.pop(sid, bytearray())))


def _client_config() -> QuicConfiguration:
    config = QuicConfiguration(is_client=True, alpn_protocols=[ALPN])
    config.verify_mode = ssl.CERT_NONE  # 自签证书，原型阶段不校验
    # 放大流量控制窗口（默认仅 ~1MB，大块产物会频繁等窗口更新）。
    config.max_data = 256 * 1024 * 1024
    config.max_stream_data = 64 * 1024 * 1024
    config.idle_timeout = 300.0           # 复用长连接：拉长空闲超时，减少被动断连
    return config


# ===== 常驻事件循环（后台守护线程；所有数据面拉取协程在此运行）=====
_loop = None
_loop_lock = threading.Lock()


def _get_loop():
    global _loop
    with _loop_lock:
        if _loop is None or not _loop.is_running():
            _loop = asyncio.new_event_loop()
            threading.Thread(target=_loop.run_forever, daemon=True,
                             name='dataplane-client-loop').start()
        return _loop


# ===== 按 (host,port) 复用的 QUIC 长连接池（仅在常驻循环线程内访问，无需额外锁保护字典）=====
class _Conn:
    __slots__ = ('cm', 'protocol')

    def __init__(self, cm, protocol):
        self.cm = cm
        self.protocol = protocol

    async def close(self):
        try:
            await self.cm.__aexit__(None, None, None)
        except Exception:
            pass


_pool = {}            # (host,port) -> _Conn
_create_lock = None   # asyncio.Lock，懒创建于常驻循环


async def _get_conn(host: str, port: int) -> _Conn:
    global _create_lock
    key = (host, int(port))
    conn = _pool.get(key)
    if conn is not None:
        return conn
    if _create_lock is None:
        _create_lock = asyncio.Lock()
    async with _create_lock:                 # 串行化建连，避免并发对同一源重复建连
        conn = _pool.get(key)                # 双检
        if conn is not None:
            return conn
        cm = connect(host, int(port), configuration=_client_config(),
                     create_protocol=_BlobClientProtocol)
        protocol = await cm.__aenter__()
        conn = _Conn(cm, protocol)
        _pool[key] = conn
        return conn


async def _fetch_pooled(host: str, port: int, artifact_id: str, rate_mbps,
                        offset=None, length=None) -> bytes:
    key = (host, int(port))
    last_exc = None
    for _attempt in range(2):                # 连接失效 -> 剔除、关闭、重连重试一次
        conn = await _get_conn(host, port)
        try:
            return await conn.protocol.fetch(artifact_id, rate_mbps, offset, length)
        except FileNotFoundError:            # 应用级：对端无此产物，不重试
            raise
        except Exception as e:               # 可能是连接断开 -> 剔除后重连重试
            last_exc = e
            _pool.pop(key, None)
            await conn.close()
    raise last_exc


def fetch_blob(host: str, port: int, artifact_id: str,
               dest_path: str = None, timeout: float = 120, rate_mbps=None) -> dict:
    """从 host:port 拉取 artifact_id；若给了 dest_path 则写盘。返回传输统计。
    复用按源设备的 QUIC 长连接（免重复握手）；并发调用在常驻循环上以多 stream 并行。
    rate_mbps>0 时请求发送端按该速率限速（T5.3）。"""
    t0 = time.time()
    loop = _get_loop()
    fut = asyncio.run_coroutine_threadsafe(
        _fetch_pooled(host, int(port), artifact_id, rate_mbps), loop)
    data = fut.result(timeout)
    elapsed = time.time() - t0
    if dest_path:
        d = os.path.dirname(os.path.abspath(dest_path))
        if d:
            os.makedirs(d, exist_ok=True)
        with open(dest_path, 'wb') as f:
            f.write(data)
    return {'bytes': len(data), 'elapsed': elapsed, 'dest': dest_path}


def fetch_range(host: str, port: int, artifact_id: str, offset: int, length: int,
                timeout: float = 120, rate_mbps=None) -> bytes:
    """拉取 artifact 的 [offset, offset+length) 字节段并返回原始字节（多链路分条带用）。
    复用按源设备的长连接；并发调用在常驻循环上以多 stream 并行。"""
    loop = _get_loop()
    fut = asyncio.run_coroutine_threadsafe(
        _fetch_pooled(host, int(port), artifact_id, rate_mbps, int(offset), int(length)), loop)
    return fut.result(timeout)


def close_all() -> None:
    """关闭全部复用连接（设备关停时可选调用）。"""
    loop = _loop
    if loop is None or not loop.is_running():
        return

    async def _close():
        for k in list(_pool.keys()):
            c = _pool.pop(k, None)
            if c:
                await c.close()
    try:
        asyncio.run_coroutine_threadsafe(_close(), loop).result(timeout=5)
    except Exception as e:
        logging.debug(f"[dataplane] close_all 忽略: {e}")
