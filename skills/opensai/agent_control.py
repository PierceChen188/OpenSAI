"""
OpenSAI Agent Control — policy / rate-limit / audit runtime (§2.8.3).

The primary class is :class:`OpenSAIAgentControl`. It enforces a tool-access
policy (allow/deny, node count, wait duration, direct-allocation and resource
caps, roles), applies sliding-window + concurrency rate limits, and emits
redacted audit events. ``AgentControl`` is kept as a compatibility alias.

All business tools MUST go through :meth:`invoke` (the single entry point);
:meth:`bind_handlers` is internal — there is no second public registration path.
"""

from __future__ import annotations

import hashlib
import json
import logging
import threading
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable

log = logging.getLogger(__name__)

__all__ = [
    "OpenSAIAgentControl",
    "AgentControl",  # compatibility alias
    "Policy",
    "AgentControlPolicy",
    "PolicyDecision",
    "RateLimitLease",
    "AuditEvent",
    "AgentContext",
    "ToolResponse",
    "RateLimiter",
    "CallRecord",
    "RateLimitExceeded",
    "PolicyVersionConflict",
]


class Policy(str, Enum):
    """Legacy coarse policy (kept for compatibility)."""

    ALLOW = "allow"
    BLOCK = "block"
    AUDIT = "audit"


# ── §2.8.3 data structures ─────────────────────────────────────────────────

@dataclass(frozen=True)
class AgentControlPolicy:
    """Immutable agent-access policy; does not replace device resource control."""

    version: str = "1.0"
    allowed_tools: tuple = ()          # empty => allow all except denied
    denied_tools: tuple = ()
    rate_limits: dict = field(default_factory=dict)   # tool -> {max_per_second, max_concurrent, max_total}
    max_dag_nodes: int | None = None   # None => unlimited
    max_wait_s: float | None = None
    allow_direct_allocation: bool = True
    resource_limits: dict = field(default_factory=dict)  # e.g. {"cores_max": 8, "mem_mb_max": 4096}
    audit_redact_paths: tuple = ()


@dataclass(frozen=True)
class PolicyDecision:
    """Result of a single permission check."""

    allowed: bool
    code: str = "OK"
    message: str = ""
    limits: dict = field(default_factory=dict)
    policy_version: str = ""


@dataclass(frozen=True)
class RateLimitLease:
    """A rate-limit/concurrency credit; ``retry_after_s`` set when denied."""

    subject: str
    tool_name: str
    lease_id: str
    acquired_at: float
    retry_after_s: float | None = None


@dataclass(frozen=True)
class AgentContext:
    """Tool-invocation context (caller identity, roles, metadata)."""

    actor: str
    session_id: str | None = None
    request_id: str | None = None
    roles: tuple = ()
    metadata: dict = field(default_factory=dict)


@dataclass(frozen=True)
class AuditEvent:
    """Redacted audit record."""

    actor: str
    tool: str
    request_id: str
    job_id: str | None
    duration_ms: float
    result_code: str
    redacted_digest: str
    timestamp: float


@dataclass
class ToolResponse:
    """Unified tool result envelope."""

    ok: bool
    result: Any = None
    error: dict | None = None

    def to_dict(self) -> dict:
        return {"ok": self.ok, "result": self.result, "error": self.error}


class RateLimitExceeded(Exception):
    def __init__(self, message: str, retry_after_s: float | None = None):
        super().__init__(message)
        self.retry_after_s = retry_after_s


class PolicyVersionConflict(Exception):
    def __init__(self, expected: str, actual: str):
        super().__init__(f"policy version conflict: expected {expected!r}, got {actual!r}")
        self.expected = expected
        self.actual = actual


# ── legacy RateLimiter (kept for compatibility) ─────────────────────────────

class RateLimiter:
    """Token-bucket style rate limiter (legacy helper)."""

    def __init__(self, max_calls_per_second: float = 10.0, max_total_calls: int | None = None):
        self._max_rate = max_calls_per_second
        self._max_total = max_total_calls
        self._lock = threading.Lock()
        self._call_times: dict[str, list[float]] = defaultdict(list)
        self._total_calls: dict[str, int] = defaultdict(int)

    def check(self, tool_name: str) -> tuple[bool, str]:
        now = time.time()
        with self._lock:
            times = self._call_times[tool_name]
            while times and times[0] < now - 1.0:
                times.pop(0)
            if len(times) >= self._max_rate:
                return False, f"Rate limit exceeded for '{tool_name}': {self._max_rate} calls/second"
            if self._max_total is not None and self._total_calls[tool_name] >= self._max_total:
                return False, f"Total call limit exceeded for '{tool_name}': {self._max_total}"
            times.append(now)
            self._total_calls[tool_name] += 1
            return True, ""

    def get_stats(self, tool_name: str) -> dict:
        with self._lock:
            return {
                "recent_calls": len(self._call_times[tool_name]),
                "total_calls": self._total_calls[tool_name],
                "max_rate": self._max_rate,
                "max_total": self._max_total,
            }


@dataclass
class CallRecord:
    """Legacy call record (kept for compatibility)."""

    tool_name: str
    timestamp: float = field(default_factory=time.time)
    parameters: dict = field(default_factory=dict)
    result_summary: str = ""
    blocked: bool = False
    block_reason: str = ""


# ── OpenSAIAgentControl ────────────────────────────────────────────────────

class OpenSAIAgentControl:
    """Policy / rate-limit / audit runtime for OpenSAI Agent tools."""

    def __init__(
        self,
        policy: AgentControlPolicy | None = None,
        audit_sink: Callable[[AuditEvent], None] | None = None,
        clock: Callable[[], float] | None = None,
    ) -> None:
        self._policy = policy or AgentControlPolicy()
        self._audit_sink = audit_sink
        self._clock = clock or time.time
        self._handlers: dict[str, Callable] = {}
        self._lock = threading.Lock()
        self._call_times: dict[tuple[str, str], list[float]] = defaultdict(list)
        self._concurrent: dict[tuple[str, str], int] = defaultdict(int)
        self._total: dict[tuple[str, str], int] = defaultdict(int)
        self._leases: dict[str, tuple[str, str]] = {}

    # ── handler binding ──────────────────────────────────────────────────

    def bind_handlers(self, handlers: dict[str, Callable]) -> None:
        """Bind raw tool handlers (internal; not a public registration path)."""
        self._handlers.update(handlers)

    # ── invoke ───────────────────────────────────────────────────────────

    def invoke(self, tool_name: str, arguments: dict | None, context: AgentContext) -> ToolResponse:
        """Run one tool call through param-check → policy → rate-limit → call → audit."""
        arguments = arguments or {}
        if not isinstance(arguments, dict):
            return self._reject(tool_name, "INVALID_ARGUMENTS", "arguments must be an object")
        if tool_name not in self._handlers:
            return self._reject(tool_name, "TOOL_NOT_REGISTERED", f"unknown tool {tool_name!r}")

        decision = self.check_policy(tool_name, arguments, context)
        if not decision.allowed:
            self.record_audit(self._event(context, tool_name, 0.0, decision.code, arguments))
            return self._reject(tool_name, decision.code, decision.message,
                                extra={"policy_version": decision.policy_version})

        lease = self.acquire_rate_limit(context.actor, tool_name)
        if not lease.lease_id:  # denied
            self.record_audit(self._event(context, tool_name, 0.0, "RATE_LIMITED", arguments))
            return self._reject(tool_name, "RATE_LIMITED", "rate limit exceeded",
                                retryable=True, extra={"retry_after_s": lease.retry_after_s})

        start = self._clock()
        try:
            result = self._handlers[tool_name](**arguments)
            self.record_audit(self._event(context, tool_name, (self._clock() - start) * 1000, "OK", arguments))
            return ToolResponse(ok=True, result=result)
        except Exception as exc:
            self.record_audit(self._event(context, tool_name, (self._clock() - start) * 1000, "ERROR", arguments))
            return self._reject(tool_name, "TOOL_ERROR", str(exc))
        finally:
            self.release_rate_limit(lease)

    def _reject(self, tool_name: str, code: str, message: str,
                retryable: bool = False, extra: dict | None = None) -> ToolResponse:
        return ToolResponse(ok=False, error={
            "code": code, "message": message, "retryable": retryable, "details": extra or {},
        })

    # ── policy ───────────────────────────────────────────────────────────

    def check_policy(self, tool_name: str, arguments: dict, context: AgentContext) -> PolicyDecision:
        p = self._policy
        # allow/deny tools
        if tool_name in p.denied_tools:
            return self._deny(tool_name, "TOOL_DENIED", f"tool {tool_name!r} is denied by policy")
        if p.allowed_tools and tool_name not in p.allowed_tools:
            return self._deny(tool_name, "TOOL_NOT_ALLOWED", f"tool {tool_name!r} is not allowed by policy")

        # DAG-node cap + arbitrary-code guard (submit tools)
        dag = _extract_dag(arguments)
        if dag is not None:
            nodes = dag.get("nodes") if isinstance(dag, dict) else None
            if isinstance(nodes, list):
                if p.max_dag_nodes is not None and len(nodes) > p.max_dag_nodes:
                    return self._deny(tool_name, "MAX_DAG_NODES",
                                      f"DAG has {len(nodes)} nodes; max {p.max_dag_nodes}")
                for node in nodes:
                    if isinstance(node, dict) and any(k in node for k in ("code", "command", "script", "exec")):
                        return self._deny(tool_name, "ARBITRARY_CODE",
                                          "DAG node contains arbitrary code fields (code/command/script/exec)")
                    if isinstance(node, dict) and node.get("direct_allocation_id", -1) not in (None, -1) \
                            and not p.allow_direct_allocation:
                        return self._deny(tool_name, "DIRECT_ALLOCATION_FORBIDDEN",
                                          "direct device allocation is not allowed by policy")
            # resource caps
            for node in nodes if isinstance(nodes, list) else []:
                if isinstance(node, dict) and isinstance(node.get("resources"), dict):
                    over = _resource_overflow(node["resources"], p.resource_limits)
                    if over:
                        return self._deny(tool_name, "RESOURCE_LIMIT", over)

        # wait-duration cap
        if tool_name in ("opensai_wait_result", "wait_result") and p.max_wait_s is not None:
            wait = arguments.get("timeout")
            if isinstance(wait, (int, float)) and wait > p.max_wait_s:
                return self._deny(tool_name, "MAX_WAIT_EXCEEDED",
                                  f"wait timeout {wait}s exceeds policy max {p.max_wait_s}s")

        return PolicyDecision(allowed=True, code="OK", policy_version=p.version)

    def _deny(self, tool_name: str, code: str, message: str) -> PolicyDecision:
        return PolicyDecision(allowed=False, code=code, message=message, policy_version=self._policy.version)

    # ── rate limit ───────────────────────────────────────────────────────

    def acquire_rate_limit(self, subject: str, tool_name: str, now: float | None = None) -> RateLimitLease:
        now = self._clock() if now is None else now
        key = (subject, tool_name)
        limit = self._policy.rate_limits.get(tool_name, {})
        max_per_second = float(limit.get("max_per_second", float("inf")))
        max_concurrent = int(limit.get("max_concurrent", 0))  # 0 => unlimited
        max_total = int(limit.get("max_total", 0))            # 0 => unlimited

        with self._lock:
            times = self._call_times[key]
            while times and times[0] <= now - 1.0:
                times.pop(0)
            if len(times) >= max_per_second:
                retry = max(0.0, 1.0 - (now - times[0]))
                return self._denied_lease(subject, tool_name, retry)
            if max_total and self._total[key] >= max_total:
                return self._denied_lease(subject, tool_name, None)
            if max_concurrent and self._concurrent[key] >= max_concurrent:
                return self._denied_lease(subject, tool_name, None)
            times.append(now)
            self._total[key] += 1
            self._concurrent[key] += 1
            lease_id = uuid.uuid4().hex
            self._leases[lease_id] = key
            return RateLimitLease(subject, tool_name, lease_id, now)

    def _denied_lease(self, subject: str, tool_name: str, retry: float | None) -> RateLimitLease:
        return RateLimitLease(subject, tool_name, "", self._clock(), retry_after_s=retry)

    def release_rate_limit(self, lease: RateLimitLease | None) -> None:
        if lease is None or not lease.lease_id:
            return
        with self._lock:
            key = self._leases.pop(lease.lease_id, None)
            if key is not None:
                self._concurrent[key] = max(0, self._concurrent[key] - 1)

    # ── audit ────────────────────────────────────────────────────────────

    def record_audit(self, event: AuditEvent) -> None:
        if self._audit_sink is not None:
            try:
                self._audit_sink(event)
            except Exception:  # pragma: no cover - audit must not break the call
                log.exception("audit sink raised")

    def _event(self, context: AgentContext, tool: str, duration_ms: float,
               result_code: str, arguments: dict) -> AuditEvent:
        digest = self._redact(arguments)
        job_id = None
        if isinstance(arguments, dict):
            job_id = arguments.get("job_id")
        return AuditEvent(
            actor=context.actor,
            tool=tool,
            request_id=context.request_id or "",
            job_id=job_id,
            duration_ms=duration_ms,
            result_code=result_code,
            redacted_digest=digest,
            timestamp=self._clock(),
        )

    def _redact(self, arguments: dict) -> str:
        redacted = dict(arguments)
        for path in self._policy.audit_redact_paths:
            _redact_path(redacted, path)
        blob = json.dumps(redacted, sort_keys=True, default=str)
        return hashlib.sha256(blob.encode("utf-8")).hexdigest()[:16]

    # ── policy reload (P2) ───────────────────────────────────────────────

    def reload_policy(self, new_policy: AgentControlPolicy, expected_version: str) -> str:
        with self._lock:
            if self._policy.version != expected_version:
                raise PolicyVersionConflict(expected_version, self._policy.version)
            self._policy = new_policy
            return new_policy.version

    @property
    def policy(self) -> AgentControlPolicy:
        return self._policy


# Compatibility alias (design doc §2.8.3: "AgentControl 仅作为兼容别名").
AgentControl = OpenSAIAgentControl


# ── internal helpers ────────────────────────────────────────────────────────

def _extract_dag(arguments: dict) -> dict | None:
    for key in ("dag_spec", "dag", "spec"):
        value = arguments.get(key)
        if isinstance(value, dict):
            return value
    return None


def _resource_overflow(resources: dict, limits: dict) -> str | None:
    for field, cap in limits.items():
        value = resources.get(field, resources.get(field.replace("_max", "")))
        if isinstance(value, (int, float)) and isinstance(cap, (int, float)) and value > cap:
            return f"resource {field}={value} exceeds policy cap {cap}"
    return None


def _redact_path(obj: dict, path: str) -> None:
    parts = path.split(".")
    cur = obj
    for part in parts[:-1]:
        if isinstance(cur, dict) and part in cur:
            cur = cur[part]
        else:
            return
    last = parts[-1]
    if isinstance(cur, dict) and last in cur:
        cur[last] = "***"
