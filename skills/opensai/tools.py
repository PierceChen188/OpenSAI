"""
OpenSAI AgentSkill runtime — tool registry, manifest and invocation (§2.8.2).

Defines the 8 ``opensai_*`` Agent tools, each mapping to one ``OpenSAIClient``
method. :func:`register_tools` is the single public registration entry point:
it registers the JSON Schemas into a :class:`ToolRegistry`, binds controlled
handlers onto an :class:`~skills.opensai.agent_control.OpenSAIAgentControl`,
and returns the tool names. :func:`shutdown` stops new calls and closes the
shared ``OpenSAIClient``.
"""

from __future__ import annotations

import threading
from typing import Any, Callable

__all__ = [
    "TOOL_DEFINITIONS",
    "TOOL_REGISTRY",
    "ToolRegistry",
    "make_handler",
    "register_tools",
    "shutdown",
]


class ToolRegistry:
    """Collects tool JSON Schemas by name."""

    def __init__(self) -> None:
        self._tools: dict[str, dict] = {}

    def register(self, tool_def: dict) -> None:
        self._tools[tool_def["name"]] = tool_def

    def names(self) -> list[str]:
        return list(self._tools.keys())

    def tools(self) -> list[dict]:
        return list(self._tools.values())

    def get(self, name: str) -> dict | None:
        return self._tools.get(name)


# ── tool descriptors (JSON Schema) ──────────────────────────────────────────

def _obj(properties: dict, required: list[str]) -> dict:
    return {"type": "object", "properties": properties, "required": required}


TOOL_DEFINITIONS: list[dict[str, Any]] = [
    {
        "name": "opensai_list_algorithms",
        "description": "List algorithms registered on the OpenSAI cluster.",
        "parameters": _obj({"refresh": {"type": "boolean", "default": False}}, []),
        "returns": {"type": "array", "items": {"type": "object"}},
    },
    {
        "name": "opensai_list_devices",
        "description": "List online computation devices in the cluster.",
        "parameters": _obj(
            {"include_stale": {"type": "boolean", "default": False},
             "max_age_s": {"type": "number", "default": 30}}, []),
        "returns": {"type": "array", "items": {"type": "object"}},
    },
    {
        "name": "opensai_cluster_status",
        "description": "Query cluster health summary.",
        "parameters": _obj(
            {"include_history": {"type": "boolean", "default": False},
             "max_age_s": {"type": "number", "default": 30}}, []),
        "returns": {"type": "object"},
    },
    {
        "name": "opensai_validate_dag",
        "description": "Validate a DAG spec without submitting it.",
        "parameters": _obj({"dag_spec": {"type": "object"}}, ["dag_spec"]),
        "returns": {"type": "object"},
    },
    {
        "name": "opensai_submit_dag",
        "description": "Submit a DAG for distributed execution and return a job handle.",
        "parameters": _obj(
            {"dag_spec": {"type": "object"},
             "validate": {"type": "boolean", "default": True},
             "idempotency_key": {"type": "string"}}, ["dag_spec"]),
        "returns": {"type": "object"},
    },
    {
        "name": "opensai_submit_task",
        "description": "Submit a single algorithm task as a one-node DAG.",
        "parameters": _obj(
            {"algorithm": {"type": "string"},
             "params": {"type": "object", "default": {}},
             "resources": {"type": "object"},
             "direct_allocation_id": {"type": "integer", "default": -1},
             "task_name": {"type": "string"},
             "wait": {"type": "boolean", "default": False}}, ["algorithm"]),
        "returns": {"type": "object"},
    },
    {
        "name": "opensai_get_result",
        "description": "Non-blocking job result query.",
        "parameters": _obj(
            {"job_id": {"type": "string"},
             "include_nodes": {"type": "boolean", "default": True}}, ["job_id"]),
        "returns": {"type": "object"},
    },
    {
        "name": "opensai_wait_result",
        "description": "Block until a job reaches a terminal state.",
        "parameters": _obj(
            {"job_id": {"type": "string"},
             "timeout": {"type": "number", "default": 300},
             "poll_interval": {"type": "number", "default": 1}}, ["job_id"]),
        "returns": {"type": "object"},
    },
]

TOOL_REGISTRY: dict[str, dict] = {t["name"]: t for t in TOOL_DEFINITIONS}


# ── handler factory ─────────────────────────────────────────────────────────

def _as_dict(value: Any) -> Any:
    if hasattr(value, "to_dict"):
        return value.to_dict()
    return value


def _handler(client_getter: Callable[[], Any], tool_name: str) -> Callable:
    def _h(**kwargs):
        client = client_getter()
        if tool_name == "opensai_list_algorithms":
            return _as_dict(client.list_algorithms(refresh=kwargs.get("refresh", False)))
        if tool_name == "opensai_list_devices":
            return client.list_devices(include_stale=kwargs.get("include_stale", False),
                                       max_age_s=kwargs.get("max_age_s", 30))
        if tool_name == "opensai_cluster_status":
            return client.cluster_status(include_history=kwargs.get("include_history", False),
                                         max_age_s=kwargs.get("max_age_s", 30))
        if tool_name == "opensai_validate_dag":
            report = client.validate_dag(kwargs["dag_spec"])
            return _as_dict(report)
        if tool_name == "opensai_submit_dag":
            return _as_dict(client.submit_dag(
                kwargs["dag_spec"],
                validate=kwargs.get("validate", True),
                idempotency_key=kwargs.get("idempotency_key"),
            ))
        if tool_name == "opensai_submit_task":
            return _as_dict(client.submit_task(
                kwargs["algorithm"],
                params=kwargs.get("params", {}),
                resources=kwargs.get("resources"),
                direct_allocation_id=kwargs.get("direct_allocation_id", -1),
                task_name=kwargs.get("task_name"),
                wait=kwargs.get("wait", False),
            ))
        if tool_name == "opensai_get_result":
            return _as_dict(client.get_result(kwargs["job_id"],
                                              include_nodes=kwargs.get("include_nodes", True)))
        if tool_name == "opensai_wait_result":
            return _as_dict(client.wait_result(
                kwargs["job_id"],
                timeout=kwargs.get("timeout", 300),
                poll_interval=kwargs.get("poll_interval", 1),
            ))
        raise ValueError(f"No handler for tool {tool_name!r}")

    _h.__name__ = f"handle_{tool_name}"
    _h.__doc__ = f"Handler for {tool_name}"
    return _h


def make_handler(tool_name: str, client_getter: Callable[[], Any] | None = None) -> Callable:
    """Return a handler for a registered tool (backward-compatible entry)."""
    if tool_name not in TOOL_REGISTRY:
        raise KeyError(f"Unknown tool: '{tool_name}'. Registered: {list(TOOL_REGISTRY)}")
    if client_getter is None:
        from opensai_client import OpenSAIClient

        def client_getter():
            return OpenSAIClient()
    return _handler(client_getter, tool_name)


# ── shared client + registration ────────────────────────────────────────────

_shared_client: Any = None
_shared_lock = threading.Lock()


def _default_client_factory() -> Any:
    from opensai_client import OpenSAIClient
    return OpenSAIClient()


def register_tools(
    registry: ToolRegistry | dict | list | None = None,
    client_factory: Callable[[], Any] | None = None,
    control: Any = None,
) -> list[str]:
    """Register the tool schemas and bind controlled handlers to ``control``.

    Returns the ordered list of tool names. The shared client is created lazily
    via ``client_factory`` and released by :func:`shutdown`.
    """
    global _shared_client
    if client_factory is None:
        client_factory = _default_client_factory

    def get_client() -> Any:
        global _shared_client
        with _shared_lock:
            if _shared_client is None:
                _shared_client = client_factory()
            return _shared_client

    handlers = {t["name"]: _handler(get_client, t["name"]) for t in TOOL_DEFINITIONS}

    if control is not None:
        control.bind_handlers(handlers)

    if registry is not None:
        for t in TOOL_DEFINITIONS:
            _register_schema(registry, t)

    return [t["name"] for t in TOOL_DEFINITIONS]


def shutdown() -> None:
    """Close the shared client (stop new calls, release resources)."""
    global _shared_client
    with _shared_lock:
        if _shared_client is not None:
            try:
                close = getattr(_shared_client, "close", None)
                if close is not None:
                    close()
            finally:
                _shared_client = None


def _register_schema(registry: Any, tool_def: dict) -> None:
    if isinstance(registry, ToolRegistry):
        registry.register(tool_def)
    elif isinstance(registry, dict):
        registry[tool_def["name"]] = tool_def
    elif isinstance(registry, list):
        registry.append(tool_def)
