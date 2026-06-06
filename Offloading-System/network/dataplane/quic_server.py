"""ArtifactServer：数据面 QUIC 服务端。

每台 device 跑一个，监听一个数据端口。其它 device 通过 QUIC 打开一条流、发来一个
artifact_id，本端把对应的本地文件流式回传。

线协议（一条 QUIC 双向流上）：
  请求：client 发 artifact_id 的 utf-8 字节，end_stream；
        或发 JSON {"id": "...", "rate_mbps": R}（R>0 时请求按 R MB/s 限速，T5.3）。
  响应：server 发 [8字节大端长度][文件字节...]，end_stream
        找不到时长度字段为 0xFFFFFFFFFFFFFFFF（哨兵）
QUIC 流自带边界与可靠有序传输，无需 sleep 防粘包。

T5.3 网络限速：发送端按令牌桶分块限速。生效速率 = min(本次请求速率, 设备出口上限
OPENSAI_DATA_RATE_MBPS)，0 表示不限（走原整块快速路径）。适配优化(T3.2)只需在派发的
input_artifacts 里给出 rate_mbps，消费者带进请求即可，无需改这里。
"""
import asyncio
import functools
import json
import logging
import os
import threading
import time

from aioquic.asyncio import serve
from aioquic.asyncio.protocol import QuicConnectionProtocol
from aioquic.quic.configuration import QuicConfiguration
from aioquic.quic.events import StreamDataReceived

from config.dataplane_config import ALPN, CERT_FILE, DATA_HOST, DATA_PORT, KEY_FILE
from network.dataplane.certs import ensure_cert

_NOT_FOUND = (1 << 64) - 1
_CHUNK = 65536                       # 限速时的分块大小
# 设备出口带宽上限（MB/s，0=不限）。每台 device 的全局上限；可被更小的"每条传输"速率进一步收紧。
_DEVICE_RATE_BPS = float(os.environ.get('OPENSAI_DATA_RATE_MBPS', '0') or 0) * 1024 * 1024


class RateLimiter:
    """异步令牌桶：限制把字节交给 QUIC 的速率（从而限制端到端吞吐）。rate<=0 表示不限。"""

    def __init__(self, rate_bps, burst_bytes=None):
        self.rate = float(rate_bps)
        self.capacity = float(burst_bytes) if burst_bytes else max(rate_bps * 0.1, _CHUNK)
        self.tokens = self.capacity
        self.last = None

    async def acquire(self, n):
        if self.rate <= 0:
            return
        while True:
            now = time.monotonic()
            if self.last is None:
                self.last = now
            self.tokens = min(self.capacity, self.tokens + (now - self.last) * self.rate)
            self.last = now
            if self.tokens >= n:
                self.tokens -= n
                return
            await asyncio.sleep((n - self.tokens) / self.rate)


def _effective_rate_bps(req_rate_bps):
    """请求速率与设备出口上限取交集（都为正取较小者；只有一个为正取它；都不限则 0）。"""
    rates = [r for r in (req_rate_bps, _DEVICE_RATE_BPS) if r and r > 0]
    return min(rates) if rates else 0.0


class ArtifactServerProtocol(QuicConnectionProtocol):
    def __init__(self, *args, registry=None, **kwargs):
        super().__init__(*args, **kwargs)
        self._registry = registry if registry is not None else {}
        self._reqbuf = {}  # stream_id -> 收到的请求字节

    def quic_event_received(self, event):
        if isinstance(event, StreamDataReceived):
            sid = event.stream_id
            buf = self._reqbuf.get(sid)
            if buf is None:
                buf = bytearray()
                self._reqbuf[sid] = buf
            buf.extend(event.data)
            if event.end_stream:
                raw = bytes(self._reqbuf.pop(sid, bytearray())).decode('utf-8', 'replace').strip()
                artifact_id, req_rate_bps = raw, 0.0
                if raw[:1] == '{':                       # 结构化请求（带限速）
                    try:
                        obj = json.loads(raw)
                        if isinstance(obj, dict) and 'id' in obj:
                            artifact_id = str(obj['id'])
                            req_rate_bps = float(obj.get('rate_mbps') or 0) * 1024 * 1024
                    except (ValueError, TypeError):
                        pass
                self._serve(sid, artifact_id, req_rate_bps)

    def _serve(self, stream_id, artifact_id, req_rate_bps=0.0):
        # 在事件循环里起一个任务做（可能限速的）流式回传。
        asyncio.ensure_future(self._serve_async(stream_id, artifact_id, req_rate_bps))

    async def _serve_async(self, stream_id, artifact_id, req_rate_bps):
        path = self._registry.get(artifact_id)
        if not (path and os.path.exists(path)):
            self._quic.send_stream_data(stream_id, _NOT_FOUND.to_bytes(8, 'big'), end_stream=True)
            self.transmit()
            logging.warning(f"[dataplane] 未找到 artifact '{artifact_id}'")
            return
        with open(path, 'rb') as f:
            data = f.read()
        rate = _effective_rate_bps(req_rate_bps)
        header = len(data).to_bytes(8, 'big')
        if rate <= 0:
            # 不限速：原整块快速路径，零额外开销。
            self._quic.send_stream_data(stream_id, header + data, end_stream=True)
            self.transmit()
            logging.info(f"[dataplane] 提供 artifact '{artifact_id}' ({len(data)} bytes, 不限速)")
            return
        # 限速：先发长度头，再按令牌桶分块发送。
        self._quic.send_stream_data(stream_id, header, end_stream=(len(data) == 0))
        self.transmit()
        limiter = RateLimiter(rate)
        for i in range(0, len(data), _CHUNK):
            chunk = data[i:i + _CHUNK]
            await limiter.acquire(len(chunk))
            self._quic.send_stream_data(stream_id, chunk, end_stream=(i + _CHUNK >= len(data)))
            self.transmit()
        logging.info(f"[dataplane] 提供 artifact '{artifact_id}' ({len(data)} bytes, 限速 {rate/1024/1024:.1f} MB/s)")


class ArtifactServer:
    """在独立线程里跑一个 QUIC 事件循环，对外提供 artifact 拉取。"""

    def __init__(self, host=None, port=None, cert_file=None, key_file=None):
        self.host = host if host is not None else DATA_HOST
        self.port = port if port is not None else DATA_PORT
        self.cert_file = cert_file or CERT_FILE
        self.key_file = key_file or KEY_FILE
        self._registry = {}            # artifact_id -> 本地文件绝对路径
        self._thread = None
        self._started = threading.Event()

    def register(self, artifact_id: str, file_path: str) -> None:
        """登记一个可被其它 device 拉取的本机产出。"""
        self._registry[artifact_id] = os.path.abspath(file_path)
        logging.info(f"[dataplane] 注册 artifact '{artifact_id}' -> {file_path}")

    def local_path(self, artifact_id: str):
        """若该 artifact 由本机产出（已注册且文件存在），返回其本地路径，否则 None。
        供同机前驱「跳过 QUIC、直接本地读」的 locality 优化使用。"""
        p = self._registry.get(artifact_id)
        return p if (p and os.path.exists(p)) else None

    def start(self, wait=True, timeout=10) -> None:
        ensure_cert(self.cert_file, self.key_file)
        self._thread = threading.Thread(target=self._run_thread, daemon=True)
        self._thread.start()
        if wait:
            if not self._started.wait(timeout=timeout):
                raise RuntimeError('ArtifactServer 启动超时')

    def _run_thread(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self._serve_forever())
        except Exception as e:  # pragma: no cover
            logging.error(f"[dataplane] 服务线程异常: {e}")

    async def _serve_forever(self):
        config = QuicConfiguration(is_client=False, alpn_protocols=[ALPN])
        config.load_cert_chain(self.cert_file, self.key_file)
        # 放大流量控制窗口（默认 ~1MB），与客户端一致，支撑大块产物传输。
        config.max_data = 256 * 1024 * 1024
        config.max_stream_data = 64 * 1024 * 1024
        create = functools.partial(ArtifactServerProtocol, registry=self._registry)
        await serve(self.host, self.port, configuration=config, create_protocol=create)
        logging.info(f"[dataplane] ArtifactServer 监听 {self.host}:{self.port}")
        self._started.set()
        await asyncio.Future()  # 永久运行
