"""ArtifactServer：数据面 QUIC 服务端。

每台 device 跑一个，监听一个数据端口。其它 device 通过 QUIC 打开一条流、发来一个
artifact_id，本端把对应的本地文件流式回传。

线协议（一条 QUIC 双向流上）：
  请求：client 发 artifact_id 的 utf-8 字节，end_stream
  响应：server 发 [8字节大端长度][文件字节...]，end_stream
        找不到时长度字段为 0xFFFFFFFFFFFFFFFF（哨兵）
QUIC 流自带边界与可靠有序传输，无需 sleep 防粘包。
"""
import asyncio
import functools
import logging
import os
import threading

from aioquic.asyncio import serve
from aioquic.asyncio.protocol import QuicConnectionProtocol
from aioquic.quic.configuration import QuicConfiguration
from aioquic.quic.events import StreamDataReceived

from config.dataplane_config import ALPN, CERT_FILE, DATA_HOST, DATA_PORT, KEY_FILE
from network.dataplane.certs import ensure_cert

_NOT_FOUND = (1 << 64) - 1


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
                artifact_id = bytes(self._reqbuf.pop(sid, bytearray())).decode('utf-8', 'replace').strip()
                self._serve(sid, artifact_id)

    def _serve(self, stream_id, artifact_id):
        path = self._registry.get(artifact_id)
        if path and os.path.exists(path):
            with open(path, 'rb') as f:
                data = f.read()
            payload = len(data).to_bytes(8, 'big') + data
            logging.info(f"[dataplane] 提供 artifact '{artifact_id}' ({len(data)} bytes)")
        else:
            payload = _NOT_FOUND.to_bytes(8, 'big')
            logging.warning(f"[dataplane] 未找到 artifact '{artifact_id}'")
        self._quic.send_stream_data(stream_id, payload, end_stream=True)
        self.transmit()


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
