"""ArtifactServer：数据面 QUIC 服务端。

每台 device 跑一个，监听一个数据端口。其它 device 通过 QUIC 打开一条流、发来一个
artifact_id，本端把对应的本地文件流式回传。

线协议（一条 QUIC 双向流上）：
  请求：client 发 artifact_id 的 utf-8 字节，end_stream；
        或发 JSON {"id": "...", "rate_mbps": R}（R>0 时请求按 R MB/s 限速，T5.3）。
  响应：server 发 [8字节大端长度][文件字节...]，end_stream
        找不到时长度字段为 0xFFFFFFFFFFFFFFFF（哨兵）
QUIC 流自带边界与可靠有序传输，无需 sleep 防粘包。

T5.3 网络限速 + A1 出口公平分配：发送端按令牌桶分块限速。每条流的生效速率 =
min(本次请求速率 rate_mbps, 设备出口公平份额)；其中【设备出口公平份额 = 设备出口预算
OPENSAI_DATA_RATE_MBPS / 当前活跃受控流数】，随流的加入/结束实时再平分（A1，替代"先到先得、
每条流各自占满出口"的旧行为——旧法 N 条并发会各占满出口 = N 倍超额）。两者都不限时走原整块
快速路径。适配优化(T3.2)只需在派发的 input_artifacts 里给出 rate_mbps，消费者带进请求即可。
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

from config.dataplane_config import ALPN, ARTIFACTS_DIR, CERT_FILE, DATA_HOST, DATA_PORT, KEY_FILE
from network.dataplane.certs import ensure_cert

_NOT_FOUND = (1 << 64) - 1
_CHUNK = 65536                       # 限速时的分块大小


class _EgressShare:
    """设备出口带宽公平分配(A1)：所有【活跃的受控流】动态平分设备出口预算。
    budget<=0 表示设备出口不限。所有 serve 协程跑在同一事件循环线程内，故计数无需加锁。
    每条流应得 = budget / 活跃流数（随流的加入/结束实时变化 → 一条结束，其余自动均分更多）。"""

    def __init__(self, budget_bps=None):
        if budget_bps is None:
            budget_bps = float(os.environ.get('OPENSAI_DATA_RATE_MBPS', '0') or 0) * 1024 * 1024
        self.budget = float(budget_bps)
        self.active = 0

    def join(self):
        self.active += 1

    def leave(self):
        self.active = max(0, self.active - 1)

    def fair_share(self):
        """当前每条活跃流应得出口速率(bps)；budget<=0 时返回 inf(本端不限，仅受每流上限约束)。"""
        if self.budget <= 0:
            return float('inf')
        return self.budget / max(1, self.active)


class RateLimiter:
    """异步令牌桶：限制把字节交给 QUIC 的速率。rate_fn() 返回【当前】允许速率(bps)，
    可随设备公平份额动态变化；返回 <=0 或 inf 表示此刻不限。"""

    def __init__(self, rate_fn):
        self.rate_fn = rate_fn if callable(rate_fn) else (lambda r=float(rate_fn): r)
        self.tokens = 0.0
        self.last = None

    async def acquire(self, n):
        while True:
            rate = self.rate_fn()
            if not rate or rate <= 0 or rate == float('inf'):
                return                                   # 此刻不限
            now = time.monotonic()
            if self.last is None:
                self.last = now
            capacity = max(rate * 0.1, _CHUNK)           # 突发桶随当前速率
            self.tokens = min(capacity, self.tokens + (now - self.last) * rate)
            self.last = now
            if self.tokens >= n:
                self.tokens -= n
                return
            await asyncio.sleep((n - self.tokens) / rate)


class ArtifactServerProtocol(QuicConnectionProtocol):
    def __init__(self, *args, registry=None, share=None, **kwargs):
        super().__init__(*args, **kwargs)
        self._registry = registry if registry is not None else {}
        self._share = share if share is not None else _EgressShare()  # 设备级出口公平分配(所有连接共用)
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
                artifact_id, req_rate_bps, offset, length = raw, 0.0, 0, None
                origin_ip, origin_port = None, None
                if raw[:1] == '{':                       # 结构化请求（带限速/范围/中继回源）
                    try:
                        obj = json.loads(raw)
                        if isinstance(obj, dict) and 'id' in obj:
                            artifact_id = str(obj['id'])
                            req_rate_bps = float(obj.get('rate_mbps') or 0) * 1024 * 1024
                            offset = int(obj.get('offset') or 0)
                            _ln = obj.get('length')
                            length = int(_ln) if _ln is not None else None
                            origin_ip = obj.get('origin_ip')
                            origin_port = obj.get('origin_port')
                    except (ValueError, TypeError):
                        pass
                self._serve(sid, artifact_id, req_rate_bps, offset, length, origin_ip, origin_port)

    def _serve(self, stream_id, artifact_id, req_rate_bps=0.0, offset=0, length=None,
               origin_ip=None, origin_port=None):
        # 在事件循环里起一个任务做（可能限速的）流式回传。
        asyncio.ensure_future(self._serve_async(stream_id, artifact_id, req_rate_bps, offset, length,
                                                origin_ip, origin_port))

    async def _serve_async(self, stream_id, artifact_id, req_rate_bps, offset=0, length=None,
                           origin_ip=None, origin_port=None):
        path = self._registry.get(artifact_id)
        data = None
        if path and os.path.exists(path):
            # 本机有：范围读（offset/length 缺省即整文件；length<=0 视为到文件尾）。
            try:
                fsize = os.path.getsize(path)
                off = max(0, int(offset or 0))
                ln = (max(0, fsize - off) if (length is None or int(length) <= 0)
                      else min(int(length), max(0, fsize - off)))
                with open(path, 'rb') as f:
                    f.seek(off)
                    data = f.read(ln)
            except OSError as e:
                self._quic.send_stream_data(stream_id, _NOT_FOUND.to_bytes(8, 'big'), end_stream=True)
                self.transmit()
                logging.warning(f"[dataplane] 读取 artifact '{artifact_id}' 范围[{offset}:{length}]失败: {e}")
                return
        elif origin_ip and origin_port:
            # 中继代理：本机没有 -> 回源 origin 拉该范围（放线程池，避免阻塞本服务事件循环）。
            try:
                loop = asyncio.get_running_loop()
                from network.dataplane.quic_client import fetch_range as _fr
                _off = max(0, int(offset or 0))
                _len = int(length) if (length is not None and int(length) > 0) else 0
                data = await loop.run_in_executor(
                    None, lambda: _fr(origin_ip, int(origin_port), artifact_id, _off, _len))
                logging.info(f"[dataplane] 中继回源 '{artifact_id}' 范围[{_off}:{_len}] 自 "
                             f"{origin_ip}:{origin_port} ({len(data)} bytes)")
            except Exception as e:
                self._quic.send_stream_data(stream_id, _NOT_FOUND.to_bytes(8, 'big'), end_stream=True)
                self.transmit()
                logging.warning(f"[dataplane] 中继回源失败 '{artifact_id}' 自 {origin_ip}:{origin_port}: {e}")
                return
        else:
            self._quic.send_stream_data(stream_id, _NOT_FOUND.to_bytes(8, 'big'), end_stream=True)
            self.transmit()
            logging.warning(f"[dataplane] 未找到 artifact '{artifact_id}'（且无回源地址）")
            return
        header = len(data).to_bytes(8, 'big')
        flow_cap = req_rate_bps if (req_rate_bps and req_rate_bps > 0) else None   # 每流上限(大脑下发)
        device_limited = self._share.budget > 0                                   # 设备出口预算(A1平分)
        if flow_cap is None and not device_limited:
            # 完全不限：原整块快速路径，零额外开销。
            self._quic.send_stream_data(stream_id, header + data, end_stream=True)
            self.transmit()
            logging.info(f"[dataplane] 提供 artifact '{artifact_id}' ({len(data)} bytes, 不限速)")
            return
        # 受控：登记为活跃流；速率 = min(每流上限, 设备出口公平份额)，随活跃流数实时变化。
        self._share.join()
        try:
            def rate_fn():
                fs = self._share.fair_share()
                return fs if flow_cap is None else min(flow_cap, fs)
            self._quic.send_stream_data(stream_id, header, end_stream=(len(data) == 0))
            self.transmit()
            limiter = RateLimiter(rate_fn)
            for i in range(0, len(data), _CHUNK):
                chunk = data[i:i + _CHUNK]
                await limiter.acquire(len(chunk))
                self._quic.send_stream_data(stream_id, chunk, end_stream=(i + _CHUNK >= len(data)))
                self.transmit()
            eff = rate_fn()
            note = '不限' if eff == float('inf') else f"{eff / 1024 / 1024:.2f} MB/s"
            logging.info(f"[dataplane] 提供 artifact '{artifact_id}' ({len(data)} bytes, "
                         f"出口活跃流={self._share.active}, 速率≈{note})")
        finally:
            self._share.leave()


class ArtifactServer:
    """在独立线程里跑一个 QUIC 事件循环，对外提供 artifact 拉取。"""

    def __init__(self, host=None, port=None, cert_file=None, key_file=None):
        self.host = host if host is not None else DATA_HOST
        self.port = port if port is not None else DATA_PORT
        self.cert_file = cert_file or CERT_FILE
        self.key_file = key_file or KEY_FILE
        self._registry = {}            # artifact_id -> 本地文件绝对路径
        self._share = None             # 设备出口公平分配(A1)，在服务线程内创建后供所有连接共用
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

    def cleanup_task(self, task_id: str) -> int:
        """7a：删除某 DAG 任务在本机的全部落盘产物（输出 {task_id}_{node}.blob +
        拉取的输入 {task_id}_{pred}.in.blob）并清掉 registry 登记。整图完成后由 server
        通知调用——此时产物必然无下游再需要。返回删除的文件数。
        注：文件名以 `{task_id}_` 为前缀，下划线分隔避免 task_id 前缀串扰。"""
        import glob as _glob
        removed = 0
        for f in _glob.glob(os.path.join(ARTIFACTS_DIR, _glob.escape(task_id) + "_*")):
            try:
                os.remove(f)
                removed += 1
            except OSError as e:
                logging.debug(f"[dataplane] 清理删除 {f} 忽略: {e}")
        prefix = f"{task_id}:"                       # registry key 形如 'task_id:node_id'
        for aid in [k for k in list(self._registry) if k.startswith(prefix)]:
            self._registry.pop(aid, None)
        if removed:
            logging.info(f"[dataplane] 清理任务 {task_id} 产物: 删除 {removed} 个文件")
        return removed

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
        self._share = _EgressShare()   # 读 OPENSAI_DATA_RATE_MBPS 作为设备出口预算
        create = functools.partial(ArtifactServerProtocol, registry=self._registry, share=self._share)
        await serve(self.host, self.port, configuration=config, create_protocol=create)
        logging.info(f"[dataplane] ArtifactServer 监听 {self.host}:{self.port}")
        self._started.set()
        await asyncio.Future()  # 永久运行
