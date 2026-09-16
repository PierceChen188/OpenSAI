"""
OpenSAI AgentSkill — OpenClaw-compatible plugin package (APP-1.7).

Exposes OpenSAI capabilities as 8 ``opensai_*`` Agent tools. The single public
registration entry point is :func:`register_tools`; the controlled invocation
runtime is :class:`skills.opensai.agent_control.OpenSAIAgentControl`.
"""

from __future__ import annotations

import logging
import os
from typing import Any

__all__ = [
    "TOOL_DEFINITIONS",
    "TOOL_REGISTRY",
    "ToolRegistry",
    "make_handler",
    "register_tools",
    "shutdown",
    "create_client",
    "get_config",
    "MANIFEST",
]


# ── manifest / metadata (OpenClaw auto-discovery) ──────────────────────────

MANIFEST: dict[str, Any] = {
    "name": "opensai",
    "version": "0.1.0",
    "display_name": "OpenSAI Offloading",
    "description": (
        "Distributed computation offloading for AI agents. "
        "Orchestrate DAG-based workloads across a cluster of edge devices."
    ),
    "author": "OpenSAI Lab",
    "license": "MIT",
    "tool_count": 8,
    "requires_connection": True,
    "protocol": "NDJSON over TCP",
    "config": {
        "OPENSAI_HOST": {"type": "string", "default": "127.0.0.1", "description": "Server host"},
        "OPENSAI_PORT": {"type": "integer", "default": 8888, "description": "Server port"},
        "OPENSAI_TIMEOUT": {"type": "integer", "default": 30, "description": "Default timeout (seconds)"},
        "OPENSAI_LOG_LEVEL": {"type": "string", "default": "INFO", "description": "Log level"},
        "OPENSAI_REDIS_HOST": {"type": "string", "default": "127.0.0.1", "description": "Redis host"},
        "OPENSAI_REDIS_PORT": {"type": "integer", "default": 6379, "description": "Redis port"},
    },
}


def get_config() -> dict:
    """Return the current effective configuration from environment."""
    return {
        "host": os.environ.get("OPENSAI_HOST", MANIFEST["config"]["OPENSAI_HOST"]["default"]),
        "port": int(os.environ.get("OPENSAI_PORT", MANIFEST["config"]["OPENSAI_PORT"]["default"])),
        "timeout": int(os.environ.get("OPENSAI_TIMEOUT", MANIFEST["config"]["OPENSAI_TIMEOUT"]["default"])),
        "log_level": os.environ.get("OPENSAI_LOG_LEVEL", MANIFEST["config"]["OPENSAI_LOG_LEVEL"]["default"]),
        "redis_host": os.environ.get("OPENSAI_REDIS_HOST", MANIFEST["config"]["OPENSAI_REDIS_HOST"]["default"]),
        "redis_port": int(os.environ.get("OPENSAI_REDIS_PORT", MANIFEST["config"]["OPENSAI_REDIS_PORT"]["default"])),
    }


def create_client(**overrides):
    """Create a lazy ``OpenSAIClient`` from config (does not connect yet)."""
    from opensai_client import Config, OpenSAIClient

    config = get_config()
    config.update(overrides)
    logging.basicConfig(
        level=getattr(logging, config["log_level"].upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    resolved = Config.load(overrides={
        "server_host": config["host"],
        "server_port": int(config["port"]),
        "timeout": float(config["timeout"]),
        "redis_host": config["redis_host"],
        "redis_port": int(config["redis_port"]),
    })
    return OpenSAIClient(config=resolved)


# ── lazy imports (circular-import safe) ────────────────────────────────────

def __getattr__(name: str):
    if name in (
        "TOOL_DEFINITIONS", "TOOL_REGISTRY", "ToolRegistry",
        "make_handler", "register_tools", "shutdown",
    ):
        from skills.opensai.tools import (
            TOOL_DEFINITIONS,
            TOOL_REGISTRY,
            ToolRegistry,
            make_handler,
            register_tools,
            shutdown,
        )
        return locals()[name]
    raise AttributeError(f"module 'skills.opensai' has no attribute {name!r}")
