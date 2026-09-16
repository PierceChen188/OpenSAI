"""
OpenSAI SDK control-plane transport (TCP + NDJSON, request_id dispatch).

Implements the §2.7.2 contract:

* UTF-8 NDJSON framing (one JSON object per ``\\n``-terminated line).
* Serialized writes (single send lock) — concurrent senders are safe.
* A background receive thread that routes each frame to a pending
  ``send_command`` (matched by ``request_id``, with a legacy type-signal
  fallback for the pre-``request_id`` server) or to async event handlers.
* Idempotent ``close()`` that wakes every pending call with
  ``TransportClosedError``.

Constructing a :class:`Transport` never touches the network; the TCP socket and
receive thread are created by :meth:`connect`.
"""

from __future__ import annotations

import json
import logging
import socket
import threading
import time
import uuid
from collections import deque
from typing import Any, Callable

from opensai_contracts.errors import ErrorCode, ErrorDetail

from .errors import (
    ProtocolFrameTooLarge,
    RemoteError,
    TransportClosedError,
    TransportError,
    TransportTimeoutError,
)

__all__ = [
    "Transport",
    "OpenSAITransport",  # backward-compatible alias
    "TransportError",
    "TimeoutError",
    "TransportClosedError",
    "ProtocolFrameTooLarge",
]

TimeoutError = TransportTimeoutError  # backward-compatible alias

log = logging.getLogger(__name__)

# Legacy response-type mapping for servers that do not echo ``request_id``.
_EXPECTED_RESPONSE: dict[str, str] = {
    "dagData": "dagRspStatus",
    "jobQuery": "job",
    "algorithmsInfo": "algorithmsInfo",
    "devicesInfo": "devicesInfo",
    "policiesInfo": "policiesInfo",
    "eventsInfo": "eventsInfo",
}


class _Pending:
    """A single in-flight ``send_command`` awaiting its response."""

    __slots__ = ("request_id", "expected_type", "event", "result", "error")

    def __init__(self, request_id: str, expected_type: str | None):
        self.request_id = request_id
        self.expected_type = expected_type
        self.event = threading.Event()
        self.result: dict | None = None
        self.error: Exception | None = None


def _build_remote_error(message: dict) -> RemoteError:
    raw = message.get("error")
    if isinstance(raw, dict):
        detail = ErrorDetail.from_dict(raw)
    elif "code" in message:
        detail = ErrorDetail.from_dict(message)
    else:
        detail = ErrorDetail(
            code=ErrorCode.INTERNAL_ERROR,
            message=str(raw if raw is not None else "remote error"),
            retryable=False,
        )
    return RemoteError(detail.message, code=detail.code, retryable=detail.retryable, details=detail.details)


class Transport:
    """Thread-safe NDJSON TCP transport for the OpenSAI control plane."""

    RECV_CHUNK = 65536

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 8888,
        timeout: float = 30.0,
        max_frame_bytes: int = 8388608,
    ) -> None:
        self._host = host
        self._port = port
        self._timeout = timeout
        self._max_frame_bytes = max_frame_bytes
        self._sock: socket.socket | None = None
        self._send_lock = threading.Lock()
        self._buf = bytearray()  # residual bytes across recv chunks
        self._recv_thread: threading.Thread | None = None
        self._running = False
        self._closed = False

        self._pending: dict[str, _Pending] = {}
        self._pending_lock = threading.Lock()

        self._event_handlers: dict[str, tuple[str | None, Callable]] = {}
        self._event_lock = threading.Lock()

        self._legacy_frames: deque = deque()
        self._legacy_cond = threading.Condition()

    # ── lifecycle ────────────────────────────────────────────────────────

    def connect(self, client_id: str = "opensai_sdk", capabilities: list | None = None) -> None:
        """Open TCP, start the receive thread, and register as a user client."""
        if self._sock is not None:
            return
        if self._closed:
            raise TransportClosedError("transport is closed")
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.connect((self._host, self._port))
        except OSError as exc:
            raise TransportError(
                f"failed to connect to {self._host}:{self._port}: {exc}",
                details={"host": self._host, "port": self._port},
            ) from exc
        self._sock = sock
        self._running = True
        self._recv_thread = threading.Thread(target=self._receive_loop, name="opensai-transport-rx", daemon=True)
        self._recv_thread.start()
        # Register as a user client (legacy server ignores the extra fields).
        self._send_frame({"user_beat": {"name": client_id}})

    def close(self) -> None:
        """Idempotently stop the receive thread, close the socket, wake pending."""
        if self._closed:
            return
        self._closed = True
        self._running = False
        sock = self._sock
        self._sock = None
        if sock is not None:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                sock.close()
            except OSError:
                pass
        thread = self._recv_thread
        self._recv_thread = None
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=1.0)
        self._wake_all(TransportClosedError("transport closed"))

    def __enter__(self) -> "Transport":
        self.connect()
        return self

    def __exit__(self, *args) -> None:
        self.close()

    @property
    def is_connected(self) -> bool:
        return self._sock is not None and self._running

    # ── send ─────────────────────────────────────────────────────────────

    def _send_frame(self, message: dict) -> None:
        """Serialize ``message`` to one NDJSON frame and send it atomically."""
        line = (json.dumps(message, ensure_ascii=False) + "\n").encode("utf-8")
        if len(line) > self._max_frame_bytes:
            raise ProtocolFrameTooLarge(
                f"frame of {len(line)} bytes exceeds max_frame_bytes={self._max_frame_bytes}",
                details={"bytes": len(line), "limit": self._max_frame_bytes},
            )
        if self._sock is None:
            raise TransportError("not connected")
        with self._send_lock:
            try:
                self._sock.sendall(line)
            except OSError as exc:
                raise TransportError(f"send failed: {exc}") from exc

    def send_message(self, obj: dict) -> None:
        """Backward-compatible alias for :meth:`_send_frame`."""
        self._send_frame(obj)

    def send_command(
        self,
        cmd: str,
        payload: dict | None = None,
        request_id: str | None = None,
        timeout: float | None = None,
        idempotency_key: str | None = None,
    ) -> dict:
        """Send a command and block for its correlated response.

        Raises ``TimeoutError`` on timeout and ``RemoteError`` when the server
        returns a structured error. A non-idempotent command that was already
        written to the socket is never transparently re-sent.
        """
        if self._sock is None or not self._running:
            raise TransportError("not connected")
        rid = request_id or uuid.uuid4().hex
        entry = _Pending(rid, _EXPECTED_RESPONSE.get(cmd))
        with self._pending_lock:
            self._pending[rid] = entry

        message: dict = {cmd: payload or {}}
        message["request_id"] = rid
        if idempotency_key is not None:
            message["idempotency_key"] = idempotency_key

        try:
            self._send_frame(message)
        except Exception:
            with self._pending_lock:
                self._pending.pop(rid, None)
            raise

        t = timeout if timeout is not None else self._timeout
        if not entry.event.wait(timeout=t):
            with self._pending_lock:
                self._pending.pop(rid, None)
            raise TransportTimeoutError(
                f"timed out waiting for response to '{cmd}'",
                details={"command": cmd, "request_id": rid},
            )
        if entry.error is not None:
            raise entry.error
        return entry.result if entry.result is not None else {}

    # ── receive (background thread) ──────────────────────────────────────

    def _receive_loop(self) -> None:
        while self._running:
            sock = self._sock
            if sock is None:
                break
            try:
                chunk = sock.recv(self.RECV_CHUNK)
            except (socket.timeout, OSError) as exc:
                if self._running:
                    log.debug("receive error: %s", exc)
                break
            if not chunk:
                break  # EOF — server closed
            self._buf.extend(chunk)
            while True:
                nl = self._buf.find(b"\n")
                if nl < 0:
                    break
                line = bytes(self._buf[:nl])
                del self._buf[: nl + 1]
                if not line.strip():
                    continue
                try:
                    message = json.loads(line.decode("utf-8"))
                except json.JSONDecodeError:
                    log.warning("skipping malformed NDJSON frame")
                    continue
                self._dispatch_message(message)
        self._running = False
        self._wake_all(TransportClosedError("connection closed"))

    def _dispatch_message(self, message: Any) -> None:
        if not isinstance(message, dict):
            return
        rid = message.get("request_id")
        # 1. request_id correlation
        if rid is not None:
            with self._pending_lock:
                entry = self._pending.pop(rid, None)
            if entry is not None:
                self._settle(entry, message)
                return
        # 2. legacy type-signal correlation
        entry = self._match_legacy(message)
        if entry is not None:
            self._settle(entry, message)
            return
        # 3. async event or legacy queue
        if self._is_async_event(message):
            self._dispatch_event(message)
        else:
            with self._legacy_cond:
                self._legacy_frames.append(message)
                self._legacy_cond.notify_all()

    def _match_legacy(self, message: dict) -> _Pending | None:
        with self._pending_lock:
            for rid, entry in list(self._pending.items()):
                if entry.expected_type is not None and entry.expected_type in message:
                    del self._pending[rid]
                    return entry
        return None

    @staticmethod
    def _settle(entry: _Pending, message: dict) -> None:
        if message.get("ok") is False or message.get("error") is not None or message.get("dagRspStatus") is False:
            entry.error = _build_remote_error(message)
        else:
            entry.result = message
        entry.event.set()

    def _wake_all(self, error: Exception) -> None:
        with self._pending_lock:
            pending = list(self._pending.values())
            self._pending.clear()
        for entry in pending:
            entry.error = error
            entry.event.set()

    # ── events ───────────────────────────────────────────────────────────

    @staticmethod
    def _is_async_event(message: dict) -> bool:
        if "event_type" in message:
            return True
        return any(k in message for k in ("dagResult", "eventsInfo"))

    def _dispatch_event(self, message: dict) -> None:
        event_type = message.get("event_type") or next(
            (k for k in ("dagResult", "eventsInfo") if k in message), "unknown"
        )
        with self._event_lock:
            handlers = [
                (hid, fn) for hid, (et, fn) in self._event_handlers.items()
                if et is None or et == event_type
            ]
        for hid, fn in handlers:
            try:
                fn(message)
            except Exception:  # pragma: no cover - isolation
                log.exception("event handler %s raised", hid)

    def add_event_handler(self, event_type: str | None, handler: Callable[[dict], None]) -> str:
        """Register an async event handler; returns a stable handler id."""
        handler_id = uuid.uuid4().hex
        with self._event_lock:
            self._event_handlers[handler_id] = (event_type, handler)
        return handler_id

    def remove_event_handler(self, handler_id: str) -> bool:
        with self._event_lock:
            return self._event_handlers.pop(handler_id, None) is not None

    # ── legacy blocking reads (backward compatibility) ───────────────────

    def recv_message(self, timeout: float | None = None) -> dict | None:
        """Block until the next frame arrives (legacy synchronous style)."""
        if self._sock is None:
            raise TransportError("not connected")
        deadline = (time.monotonic() + timeout) if timeout is not None else None
        with self._legacy_cond:
            while not self._legacy_frames:
                if self._closed:
                    raise TransportClosedError("transport closed")
                remaining = (deadline - time.monotonic()) if deadline is not None else None
                if remaining is not None and remaining <= 0:
                    raise TransportTimeoutError("receive timed out")
                self._legacy_cond.wait(timeout=remaining)
            return self._legacy_frames.popleft()

    def recv_typed(self, message_type: str, timeout: float | None = None) -> dict:
        """Receive until a message carrying ``message_type`` arrives (legacy)."""
        deadline = (time.monotonic() + timeout) if timeout is not None else None
        while True:
            remaining = (deadline - time.monotonic()) if deadline is not None else None
            if remaining is not None and remaining <= 0:
                raise TransportTimeoutError(f"receive timed out waiting for {message_type}")
            msg = self.recv_message(timeout=remaining)
            if msg is None:
                raise TransportClosedError("connection closed")
            if isinstance(msg, dict) and message_type in msg:
                return msg

    def fetch_result(self, job_id: str, timeout: float = 30.0) -> dict:
        """Legacy helper: wait for the next ``dagResult`` frame."""
        return self.recv_typed("dagResult", timeout=timeout)


# Backward-compatible alias used by earlier SDK code.
OpenSAITransport = Transport
