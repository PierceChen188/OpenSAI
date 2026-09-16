"""
OpenSAI SDK Redis bridge (read-only device / cluster / monitor data).

Reads the monitor snapshot from Redis without ever mutating server state.
Redis is imported lazily and connections are created on first use, so merely
constructing a :class:`RedisBridge` never touches the network.

Credentials never appear in ``repr`` or logs.
"""

from __future__ import annotations

import logging
from typing import Any

from .errors import MonitorUnavailableError

__all__ = ["RedisBridge"]

log = logging.getLogger(__name__)

# Redis key carrying the authoritative monitor view (see §2.4.1).
MONITOR_VIEW_KEY = "OPENSAI_MONITOR_VIEW"


class RedisBridge:
    """Read-only bridge to the OpenSAI Redis monitor namespace.

    Parameters
    ----------
    host:
        Redis host.
    port:
        Redis port.
    db:
        Redis database index.
    username / password:
        Optional credentials (never logged / repr'd).
    ssl:
        Use TLS to Redis.
    socket_timeout:
        Per-command socket timeout in seconds.
    """

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 6379,
        db: int = 0,
        username: str | None = None,
        password: str | None = None,
        ssl: bool = False,
        socket_timeout: float = 5.0,
    ) -> None:
        self._host = host
        self._port = port
        self._db = db
        self._username = username
        self._password = password
        self._ssl = ssl
        self._socket_timeout = socket_timeout
        self._client = None
        self._closed = False

    def __repr__(self) -> str:
        return (
            f"RedisBridge(host={self._host}, port={self._port}, db={self._db}, "
            f"ssl={self._ssl})"
        )

    # ── connection (lazy) ────────────────────────────────────────────────

    def _ensure_client(self):
        if self._closed:
            raise MonitorUnavailableError(
                "RedisBridge is closed",
                details={"host": self._host},
            )
        if self._client is None:
            try:
                import redis
            except ImportError as exc:  # pragma: no cover - redis optional
                raise MonitorUnavailableError(
                    "redis package is not installed",
                    details={"error": str(exc)},
                ) from exc
            try:
                self._client = redis.Redis(
                    host=self._host,
                    port=self._port,
                    db=self._db,
                    username=self._username,
                    password=self._password,
                    ssl=self._ssl,
                    socket_timeout=self._socket_timeout,
                    decode_responses=True,
                )
                # Trigger a connection attempt so failures surface early.
                self._client.ping()
            except Exception as exc:
                self._client = None
                raise MonitorUnavailableError(
                    f"failed to connect to Redis {self._host}:{self._port}: {exc}",
                    details={"host": self._host, "port": self._port},
                ) from exc
        return self._client

    # ── reads ────────────────────────────────────────────────────────────

    def get_monitor_view(self, max_age_s: float = 30.0) -> dict:
        """Read the monitor view snapshot; return a normalized dict.

        Uses the snapshot ``ts`` to judge overall age; it never fabricates
        per-device ``last_seen``.
        """
        client = self._ensure_client()
        try:
            raw = client.get(MONITOR_VIEW_KEY)
        except Exception as exc:
            raise MonitorUnavailableError(
                f"failed to read {MONITOR_VIEW_KEY}: {exc}",
                details={"key": MONITOR_VIEW_KEY},
            ) from exc
        if raw is None:
            return {}
        if isinstance(raw, str):
            import json

            try:
                raw = json.loads(raw)
            except json.JSONDecodeError:
                return {}
        if not isinstance(raw, dict):
            return {}
        return dict(raw)

    def list_devices(self, include_stale: bool = False, max_age_s: float = 30.0) -> list:
        """Return normalized device snapshots from the monitor view.

        Each device is normalized to the §2.4.1 field convention
        (device_id, allocation_index, ip, status, cpu_percent, slots, memory,
        storage, snapshot_ts, view_stale).
        """
        view = self.get_monitor_view(max_age_s=max_age_s)
        devices = view.get("devices", {})
        if not isinstance(devices, dict):
            return []
        out = []
        # allocation_index is the 1-based index by ascending device_id.
        for idx, device_id in enumerate(sorted(devices.keys()), start=1):
            d = devices[device_id]
            if not isinstance(d, dict):
                d = {}
            out.append(self._normalize_device(device_id, idx, d, include_stale, view))
        return out

    def get_cluster_status(self, include_history: bool = False, max_age_s: float = 30.0) -> dict:
        """Aggregate devices, ledger and links into a cluster status dict."""
        view = self.get_monitor_view(max_age_s=max_age_s)
        devices = self.list_devices(include_stale=include_history, max_age_s=max_age_s)
        online = [d for d in devices if d.get("status") not in ("offline", "unknown")]
        total_slots = sum(int(d.get("slots_total", 0)) for d in devices)
        used_slots = sum(int(d.get("slots_used", 0)) for d in devices)
        alerts = self._alerts(view, devices)
        return {
            "online_count": len(online),
            "total_slots": total_slots,
            "used_slots": used_slots,
            "health": {
                "status": self._health_status(view, devices, alerts),
                "inflight_tasks": len(view.get("inflight", {}) or {}),
            },
            "devices": devices,
            "links": view.get("links", []),
            "alerts": alerts,
            "stale": self._is_stale(view, max_age_s),
        }

    # ── close ────────────────────────────────────────────────────────────

    def close(self) -> None:
        """Idempotently release the Redis connection."""
        if self._closed:
            return
        if self._client is not None:
            try:
                self._client.close()
            except Exception:  # pragma: no cover - best-effort
                pass
            self._client = None
        self._closed = True

    def __enter__(self) -> "RedisBridge":
        return self

    def __exit__(self, *args) -> None:
        self.close()

    # ── normalization helpers ────────────────────────────────────────────

    @staticmethod
    def _normalize_device(device_id, allocation_index, d, include_stale, view) -> dict:
        return {
            "device_id": device_id,
            "allocation_index": allocation_index,
            "ip": d.get("ip_address", d.get("ip", "")),
            "status": d.get("status", "unknown"),
            "cpu_percent": d.get("used_cpu", d.get("cpu_percent", 0)),
            "slots_used": int(d.get("work_status", 0) or 0),
            "slots_total": int(d.get("worker_slots", 0) or 0),
            "memory_mib": d.get("used_memory", d.get("memory_mib", 0)),
            "storage_mib": d.get("storage_committed_mb", d.get("storage_mib", 0)),
            "snapshot_ts": view.get("ts"),
            "view_stale": False,
        }

    @staticmethod
    def _is_stale(view: dict, max_age_s: float) -> bool:
        import time

        ts = view.get("ts")
        if ts is None:
            return True
        try:
            return (time.time() - float(ts)) > max_age_s
        except (TypeError, ValueError):
            return True

    @staticmethod
    def _alerts(view: dict, devices: list) -> list:
        alerts = []
        for d in devices:
            cpu = d.get("cpu_percent", 0)
            try:
                cpu_pct = float(str(cpu).rstrip("%"))
            except (TypeError, ValueError):
                cpu_pct = 0.0
            if cpu_pct >= 90:
                alerts.append({"device": d.get("device_id"), "type": "high_cpu", "value": cpu})
            delay = d.get("delay", 0)
            if isinstance(delay, (int, float)) and delay > 500:
                alerts.append({"device": d.get("device_id"), "type": "high_delay", "value_ms": delay})
        return alerts

    @staticmethod
    def _health_status(view: dict, devices: list, alerts: list) -> str:
        if not devices:
            return "empty"
        if alerts:
            return "critical"
        if any(d.get("status") == "offline" for d in devices):
            return "degraded"
        return "healthy"
