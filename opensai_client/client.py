"""
OpenSAI SDK client (framework-agnostic control-plane client).

Implements the §2.2.2 lifecycle and the §2.3.2 / §2.4.2 discovery, DAG and job
methods. Construction is lazy: ``__init__`` never opens a socket — the TCP
connection and Redis bridge are created on the first business call (or inside
``with OpenSAIClient(...) as c``).

The SDK reuses the neutral ``opensai_contracts`` package for all structures,
wire conversion and the shared ``DAGValidator`` — it never re-implements them.
"""

from __future__ import annotations

import dataclasses
import time
from typing import Any, Callable

from opensai_contracts import (
    AlgorithmInfo,
    DAGSpec,
    DAGValidator,
    JobHandle,
    JobResult,
    JobStatus,
    OpenSAIConfig,
    dag_spec_to_dag_data,
    dag_result_to_job_result,
    is_terminal,
    job_ack_from_wire,
    parse_dag,
    single_task_dag,
)
from opensai_contracts.errors import ErrorCode

from .config import Config
from .errors import ClientClosedError, OpenSAIClientError, TimeoutError
from .redis_bridge import RedisBridge
from .transport import Transport
from .validator import ClientValidator, ValidationError

__all__ = ["OpenSAIClient"]


class OpenSAIClient:
    """Lazy, framework-agnostic SDK client for the OpenSAI control plane.

    Parameters
    ----------
    config:
        Optional immutable :class:`OpenSAIConfig`. When omitted, defaults are
        used. For backward compatibility a plain string first argument is
        treated as ``server_host``.
    server_host / server_port / timeout:
        Optional overrides applied on top of ``config``.
    transport / redis_bridge:
        Optional injected dependencies (for unit testing).
    """

    def __init__(
        self,
        config: OpenSAIConfig | None = None,
        server_host: str | None = None,
        server_port: int | None = None,
        timeout: float | None = None,
        transport: Transport | None = None,
        redis_bridge: RedisBridge | None = None,
    ) -> None:
        # Backward-compatible positional form: OpenSAIClient('host', port).
        if isinstance(config, str) and not isinstance(config, OpenSAIConfig):
            if server_host is not None and server_port is None:
                server_port = server_host  # 2nd positional was actually the port
            server_host = config
            config = None

        self._config: OpenSAIConfig = config if isinstance(config, OpenSAIConfig) else OpenSAIConfig()
        if server_host is not None:
            self._config = dataclasses.replace(self._config, server_host=server_host)
        if server_port is not None:
            self._config = dataclasses.replace(self._config, server_port=server_port)
        if timeout is not None:
            self._config = dataclasses.replace(self._config, timeout=timeout)

        self._transport = transport
        self._redis_bridge = redis_bridge
        self._connected = False
        self._closed = False
        self._algorithm_cache: list[AlgorithmInfo] | None = None
        self._validator = ClientValidator(client=self, limits=None)

    # ── lifecycle ────────────────────────────────────────────────────────

    def _ensure_transport(self) -> Transport:
        if self._transport is None:
            self._transport = Transport(
                host=self._config.server_host,
                port=self._config.server_port,
                timeout=self._config.timeout,
                max_frame_bytes=self._config.max_frame_bytes,
            )
        return self._transport

    def _ensure_connected(self) -> None:
        if self._closed:
            raise ClientClosedError("client is closed")
        if not self._connected:
            transport = self._ensure_transport()
            transport.connect(client_id="opensai_sdk")
            self._connected = True

    def connect(self) -> None:
        """Establish the control-plane connection (lazy-compat convenience)."""
        self._ensure_connected()

    def close(self) -> None:
        """Idempotently close the control-plane socket and Redis connection."""
        if self._closed:
            return
        self._closed = True
        if self._transport is not None:
            self._transport.close()
        if self._redis_bridge is not None:
            self._redis_bridge.close()
        self._connected = False

    def __enter__(self) -> "OpenSAIClient":
        self._ensure_connected()
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> bool:
        self.close()
        return False  # never suppress the caller's exception

    @property
    def is_connected(self) -> bool:
        return self._connected

    @property
    def config(self) -> OpenSAIConfig:
        return self._config

    # ── discovery ────────────────────────────────────────────────────────

    def list_algorithms(self, refresh: bool = False) -> list[AlgorithmInfo]:
        """Return the server's algorithm list merged with manifest metadata."""
        self._ensure_connected()
        if not refresh and self._algorithm_cache is not None:
            return self._algorithm_cache
        response = self._transport.send_command("algorithmsInfo", {})
        algorithms = _parse_algorithms_info(response)
        self._algorithm_cache = algorithms
        return algorithms

    def list_devices(self, include_stale: bool = False, max_age_s: float = 30.0) -> list:
        """Return normalized device snapshots (via RedisBridge)."""
        bridge = self._ensure_redis_bridge()
        return bridge.list_devices(include_stale=include_stale, max_age_s=max_age_s)

    def cluster_status(self, include_history: bool = False, max_age_s: float = 30.0) -> dict:
        """Return the aggregated cluster status (via RedisBridge)."""
        bridge = self._ensure_redis_bridge()
        return bridge.get_cluster_status(include_history=include_history, max_age_s=max_age_s)

    def get_events(self) -> list:
        """Return recent decision events (legacy compatibility helper)."""
        self._ensure_connected()
        response = self._transport.send_command("eventsInfo", {})
        events = response.get("eventsInfo", {})
        if isinstance(events, dict):
            return events.get("events", [])
        return events if isinstance(events, list) else []

    def _ensure_redis_bridge(self) -> RedisBridge:
        if self._redis_bridge is None:
            self._redis_bridge = RedisBridge(
                host=self._config.redis_host,
                port=self._config.redis_port,
                db=self._config.redis_db,
                username=self._config.redis_username,
                password=self._config.redis_password,
                ssl=self._config.redis_ssl,
            )
        return self._redis_bridge

    # ── validation ───────────────────────────────────────────────────────

    def validate_dag(self, dag: DAGSpec | dict, strict: bool = True, fetch_registry: bool = True) -> Any:
        """Validate a DAG via the shared validator; no submission side-effects."""
        if fetch_registry:
            self._validator._ensure_registry()
        return self._validator.validate(dag)

    # ── submission ───────────────────────────────────────────────────────

    def submit_dag(
        self,
        dag: DAGSpec | dict,
        validate: bool = True,
        idempotency_key: str | None = None,
        metadata: dict | None = None,
    ) -> JobHandle:
        """Validate, encode and submit a DAG; returns a typed ``JobHandle``."""
        self._ensure_connected()
        spec = _as_dag_spec(dag)
        if metadata:
            spec = dataclasses.replace(spec, metadata={**spec.metadata, **metadata})
        if validate:
            spec = self._validator.validate_or_raise(spec)

        envelope = dag_spec_to_dag_data(spec)
        inner = envelope["dagData"]
        response = self._transport.send_command("dagData", inner, idempotency_key=idempotency_key)
        return job_ack_from_wire(response)

    def get_result(self, job_id: str, include_nodes: bool = True) -> JobResult:
        """Non-blocking job query (``jobQuery``); returns a snapshot."""
        self._ensure_connected()
        response = self._transport.send_command(
            "jobQuery", {"job_id": job_id, "include_nodes": include_nodes}
        )
        return _parse_job_query(response, job_id)

    def wait_result(
        self,
        job_id: str,
        timeout: float | None = 300.0,
        poll_interval: float = 1.0,
        on_update: Callable[[JobResult], None] | None = None,
    ) -> JobResult:
        """Poll for a terminal state using a monotonic deadline.

        A timeout does NOT cancel the server-side job; the caller may keep
        polling later.
        """
        self._ensure_connected()
        deadline = time.monotonic() + (timeout if timeout is not None else 300.0)
        while True:
            result = self.get_result(job_id)
            if is_terminal(result.status):
                return result
            if on_update is not None:
                on_update(result)
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(
                    f"wait_result timed out for job {job_id}; job continues on server",
                    code=ErrorCode.WAIT_TIMEOUT,
                    details={"job_id": job_id},
                )
            time.sleep(min(poll_interval, remaining))

    def submit_task(
        self,
        algorithm: str,
        params: dict | None = None,
        resources: Any = None,
        direct_allocation_id: int = -1,
        task_name: str | None = None,
        wait: bool = False,
    ) -> JobHandle | JobResult:
        """Construct a single-node DAG and submit it (optionally wait)."""
        dag = single_task_dag(
            algorithm=algorithm,
            params=params,
            resources=resources,
            direct_allocation_id=direct_allocation_id,
            name=task_name,
        )
        job = self.submit_dag(dag)
        if wait:
            return self.wait_result(job.job_id)
        return job


# ── internal helpers ────────────────────────────────────────────────────────

def _as_dag_spec(dag: DAGSpec | dict) -> DAGSpec:
    if isinstance(dag, DAGSpec):
        return dag
    return parse_dag(dag, allow_wire_format=True)


def _parse_algorithms_info(response: dict) -> list[AlgorithmInfo]:
    raw = response.get("algorithmsInfo", [])
    if isinstance(raw, list):
        entries = raw
    elif isinstance(raw, dict):
        entries = raw.get("algorithms", list(raw.keys()))
    else:
        entries = []
    out: list[AlgorithmInfo] = []
    for item in entries:
        if isinstance(item, AlgorithmInfo):
            out.append(item)
        elif isinstance(item, str):
            out.append(AlgorithmInfo(name=item, schema_available=False, submittable=False))
        elif isinstance(item, dict):
            try:
                out.append(AlgorithmInfo.from_dict(item))
            except Exception:
                out.append(AlgorithmInfo(name=str(item.get("name", "")), schema_available=False, submittable=False))
    return out


def _parse_job_query(response: dict, job_id: str) -> JobResult:
    job = response.get("job")
    if isinstance(job, JobResult):
        return job
    if isinstance(job, dict):
        # normalize into a JobResult
        status = job.get("status", JobStatus.QUEUED)
        return JobResult(
            job_id=job.get("job_id", job_id),
            status=status,
            submitted_at=job.get("submitted_at"),
            started_at=job.get("started_at"),
            completed_at=job.get("completed_at"),
            duration_s=job.get("duration_s"),
            nodes=job.get("nodes", []),
            error=job.get("error"),
        )
    # Legacy dagResult fallback
    return dag_result_to_job_result(response)
