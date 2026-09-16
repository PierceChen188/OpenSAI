"""
OpenSAI CLI — command-line interface for OpenSAI compute offloading (APP-1.8).

Subcommands: ``algorithms`` / ``devices`` / ``cluster`` / ``dag`` / ``result`` /
``task``. Output is machine-readable JSON on stdout; diagnostics go to stderr.

Exit codes (§2.2.5)
-------------------
  0   success (QUEUED/RUNNING is success)
  2   usage / config / input / DAG validation error
  3   network / Redis / control-plane timeout / protocol error
  4   server rejection / unknown job_id / terminal failure
  5   ``result wait`` timeout (server job continues)
  6   Agent permission / rate-limit rejection
  10  unclassified internal error (request_id emitted)
  130 user Ctrl-C
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import uuid
from pathlib import Path
from typing import Any, TextIO

__all__ = [
    "main", "build_parser", "load_dag", "parse_params",
    "dispatch", "emit_output", "emit_error",
    "EXIT_SUCCESS", "EXIT_USAGE", "EXIT_NETWORK", "EXIT_SERVER",
    "EXIT_TIMEOUT", "EXIT_AGENT_DENIED", "EXIT_INTERNAL", "EXIT_INTERRUPT",
]

__version__ = "0.1.0"

# ── exit codes ─────────────────────────────────────────────────────────────

EXIT_SUCCESS = 0
EXIT_USAGE = 2
EXIT_NETWORK = 3
EXIT_SERVER = 4
EXIT_TIMEOUT = 5
EXIT_AGENT_DENIED = 6
EXIT_INTERNAL = 10
EXIT_INTERRUPT = 130

_MAX_DAG_BYTES = 4 * 1024 * 1024  # 4 MiB input cap

log = logging.getLogger(__name__)


# ── argument parser ────────────────────────────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    """Build the OpenSAI CLI parser (server/redis/timeout/json/version + subcommands)."""
    parser = argparse.ArgumentParser(
        prog="opensai",
        description="OpenSAI CLI — distributed computation offloading",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--version", action="version", version=f"opensai {__version__}")
    parser.add_argument("--host", "--server", dest="host",
                        default=os.environ.get("OPENSAI_HOST", "127.0.0.1"),
                        help="OpenSAI server host (env OPENSAI_HOST, default 127.0.0.1)")
    parser.add_argument("--port", type=int,
                        default=int(os.environ.get("OPENSAI_PORT", "8888")),
                        help="OpenSAI server port (env OPENSAI_PORT, default 8888)")
    parser.add_argument("--redis", default=os.environ.get("OPENSAI_REDIS_HOST", "127.0.0.1"),
                        help="Redis host (default 127.0.0.1)")
    parser.add_argument("--redis-port", type=int,
                        default=int(os.environ.get("OPENSAI_REDIS_PORT", "6379")),
                        help="Redis port (default 6379)")
    parser.add_argument("--redis-db", type=int,
                        default=int(os.environ.get("OPENSAI_REDIS_DB", "0")),
                        help="Redis DB index (default 0)")
    parser.add_argument("--config", help="Path to a JSON/YAML config file")
    parser.add_argument("--timeout", type=float,
                        default=float(os.environ.get("OPENSAI_TIMEOUT", "30")),
                        help="Default timeout in seconds (default 30)")
    parser.add_argument("--poll-interval", type=float, default=1.0,
                        help="Poll interval for result wait (default 1.0)")
    parser.add_argument("--json", action="store_true", default=True,
                        help="Output machine-readable JSON (always on)")

    sub = parser.add_subparsers(dest="command", title="subcommands")

    sub.add_parser("algorithms", help="Algorithm operations").add_subparsers(
        dest="algo_action").add_parser("list", help="List registered algorithms")

    sub.add_parser("devices", help="Device operations").add_subparsers(
        dest="dev_action").add_parser("list", help="List online devices")

    sub.add_parser("cluster", help="Cluster operations").add_subparsers(
        dest="cluster_action").add_parser("status", help="Show cluster health")

    dag_p = sub.add_parser("dag", help="DAG operations")
    dag_sub = dag_p.add_subparsers(dest="dag_action")
    validate_p = dag_sub.add_parser("validate", help="Validate a DAG specification")
    validate_p.add_argument("--file", "-f", help="Path to DAG JSON/YAML file (or stdin)")
    submit_p = dag_sub.add_parser("submit", help="Submit a DAG for execution")
    submit_p.add_argument("--file", "-f", help="Path to DAG JSON/YAML file (or stdin)")

    res_p = sub.add_parser("result", help="Result operations")
    res_sub = res_p.add_subparsers(dest="result_action")
    get_p = res_sub.add_parser("get", help="Non-blocking result query")
    get_p.add_argument("job_id", help="Job identifier")
    wait_p = res_sub.add_parser("wait", help="Block until result is ready")
    wait_p.add_argument("job_id", help="Job identifier")
    wait_p.add_argument("--timeout", "-t", type=float, default=60, help="Max seconds to wait")

    task_p = sub.add_parser("task", help="Task operations")
    task_sub = task_p.add_subparsers(dest="task_action")
    task_submit_p = task_sub.add_parser("submit", help="Submit a single algorithm task")
    task_submit_p.add_argument("algorithm", help="Algorithm name")
    task_submit_p.add_argument("--params", "-p", default="{}", help="Algorithm params JSON or @file")
    task_submit_p.add_argument("--timeout", "-t", type=float, default=60, help="Max seconds to wait")

    return parser


# Backward-compatible alias used by earlier tests.
_build_parser = build_parser


# ── input helpers ──────────────────────────────────────────────────────────

def load_dag(path: str | None = None, stdin: TextIO | None = None,
             format: str = "auto") -> Any:
    """Read a DAG from a file (JSON/YAML) or stdin and parse it into a DAGSpec.

    ``format`` is accepted for interface parity ('auto' sniffs JSON vs YAML by
    extension / first byte). Raises ``ValueError`` on invalid input.
    """
    from opensai_contracts import parse_dag

    stdin = stdin if stdin is not None else sys.stdin
    if path:
        p = Path(path)
        try:
            raw = p.read_bytes()
        except FileNotFoundError as exc:
            raise ValueError(f"file not found: {path}") from exc
    else:
        raw = stdin.buffer.read() if hasattr(stdin, "buffer") else stdin.read().encode("utf-8")
        if not raw.strip():
            raise ValueError("no DAG input provided (use --file or pipe JSON to stdin)")
    if len(raw) > _MAX_DAG_BYTES:
        raise ValueError(f"DAG input exceeds {_MAX_DAG_BYTES} bytes")

    data = _decode_dag(raw, path or "stdin", format)
    return parse_dag(data, allow_wire_format=True)


def _decode_dag(raw: bytes, source: str, format: str) -> dict:
    text = raw.decode("utf-8")
    if format in ("yaml", "yml") or (format == "auto" and source.endswith((".yaml", ".yml"))):
        try:
            import yaml
            data = yaml.safe_load(text)
        except Exception as exc:
            raise ValueError(f"invalid YAML in {source}: {exc}") from exc
    else:
        try:
            data = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError(f"invalid JSON in {source}: {exc}") from exc
    if not isinstance(data, dict):
        raise ValueError(f"DAG root must be an object, got {type(data).__name__}")
    return data


def parse_params(value: str) -> dict:
    """Parse algorithm params from inline JSON or a ``@file`` reference.

    Rejects ``eval``/expression forms and non-object roots.
    """
    value = (value or "{}").strip()
    if value.startswith("@"):
        path = value[1:]
        try:
            value = Path(path).read_text(encoding="utf-8")
        except FileNotFoundError as exc:
            raise ValueError(f"params file not found: {path}") from exc
    try:
        data = json.loads(value)
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid params JSON: {exc}") from exc
    if not isinstance(data, dict):
        raise ValueError(f"params must be a JSON object, got {type(data).__name__}")
    return data


# ── output helpers ─────────────────────────────────────────────────────────

def _jsonable(value: Any) -> Any:
    if hasattr(value, "to_dict"):
        return value.to_dict()
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    if isinstance(value, dict):
        return {k: _jsonable(v) for k, v in value.items()}
    return value


def emit_output(value: Any, json_mode: bool = True, stdout: TextIO | None = None) -> None:
    """Write one stable JSON value (or a redacted table) to stdout."""
    stdout = stdout if stdout is not None else sys.stdout
    if json_mode:
        json.dump(_jsonable(value), stdout, ensure_ascii=False, default=str)
        stdout.write("\n")
    else:  # pragma: no cover - table mode optional
        print(value, file=stdout)


def emit_error(error: BaseException, json_mode: bool = True,
               stderr: TextIO | None = None) -> int:
    """Map an exception to a diagnostic and a stable exit code."""
    stderr = stderr if stderr is not None else sys.stderr
    exit_code, request_id = _classify(error)
    machine_code = getattr(error, "code", None)
    if not machine_code:
        machine_code = "INVALID_ARGUMENT" if isinstance(error, ValueError) else "INTERNAL_ERROR"
    if json_mode:
        json.dump({"error": {"code": machine_code, "message": str(error),
                             "request_id": request_id, "exit_code": exit_code}},
                  stderr, default=str)
        stderr.write("\n")
    else:  # pragma: no cover
        print(f"error ({exit_code}): {error}", file=stderr)
    return exit_code


def _classify(error: BaseException) -> tuple[int, str]:
    from opensai_contracts.errors import ConfigError, ContractValidationError

    request_id = uuid.uuid4().hex
    if isinstance(error, KeyboardInterrupt):
        return EXIT_INTERRUPT, request_id
    if isinstance(error, (ConfigError, ContractValidationError, ValueError)):
        return EXIT_USAGE, request_id
    # WAIT timeout (server job continues) vs network timeout
    code_attr = getattr(error, "code", None)
    if code_attr == "WAIT_TIMEOUT":
        return EXIT_TIMEOUT, request_id
    from opensai_client.errors import (
        AGENT_DENIAL_CODES,
        AgentControlError,
        TransportError,
        TransportTimeoutError,
        RemoteError,
        ClientClosedError,
    )
    # Agent permission / rate-limit rejection → exit 6 (§2.2.5). Checked before
    # the generic server-rejection mapping so a denial code never leaks to exit 4.
    if isinstance(error, AgentControlError) or code_attr in AGENT_DENIAL_CODES:
        return EXIT_AGENT_DENIED, request_id
    if isinstance(error, RemoteError):
        return EXIT_SERVER, request_id
    if isinstance(error, ClientClosedError):
        return EXIT_INTERNAL, request_id
    if isinstance(error, (TransportTimeoutError, TransportError)):
        return EXIT_NETWORK, request_id
    return EXIT_INTERNAL, request_id


# ── dispatch ───────────────────────────────────────────────────────────────

def dispatch(args: argparse.Namespace, client: Any) -> Any:
    """Route a parsed command to the corresponding SDK method."""
    cmd = args.command
    if cmd == "algorithms":
        if args.algo_action == "list":
            return {"algorithms": client.list_algorithms(refresh=args.refresh if hasattr(args, "refresh") else False)}
    elif cmd == "devices":
        if args.dev_action == "list":
            devices = client.list_devices()
            return {"devices": devices, "count": len(devices)}
    elif cmd == "cluster":
        if args.cluster_action == "status":
            return client.cluster_status()
    elif cmd == "dag":
        spec = load_dag(args.file)
        if args.dag_action == "validate":
            report = client.validate_dag(spec)
            return report.to_dict() if hasattr(report, "to_dict") else report
        if args.dag_action == "submit":
            job = client.submit_dag(spec)
            return job.to_dict() if hasattr(job, "to_dict") else job
    elif cmd == "result":
        if args.result_action == "get":
            result = client.get_result(args.job_id)
            return result.to_dict() if hasattr(result, "to_dict") else result
        if args.result_action == "wait":
            return client.wait_result(args.job_id, timeout=args.timeout)
    elif cmd == "task":
        if args.task_action == "submit":
            params = parse_params(args.params)
            result = client.submit_task(args.algorithm, params=params, wait=True)
            return result.to_dict() if hasattr(result, "to_dict") else result
    raise ValueError(f"unknown command: {cmd}")


# ── main ───────────────────────────────────────────────────────────────────

def main(argv: list[str] | None = None) -> int:
    """CLI entry point; returns a stable exit code (does not call ``sys.exit``)."""
    parser = build_parser()
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.WARNING, format="%(levelname)s: %(message)s",
                        stream=sys.stderr)

    if not args.command:
        parser.print_help()
        return EXIT_SUCCESS

    from opensai_client import Config, OpenSAIClient

    try:
        config = _resolve_config(args)
        client = OpenSAIClient(config=config)
        try:
            result = dispatch(args, client)
        finally:
            client.close()
        emit_output(result)
        return EXIT_SUCCESS
    except KeyboardInterrupt:
        return EXIT_INTERRUPT
    except Exception as exc:
        return emit_error(exc)


def _resolve_config(args: argparse.Namespace):
    from opensai_client import Config
    overrides = {
        "server_host": args.host,
        "server_port": args.port,
        "redis_host": args.redis,
        "redis_port": args.redis_port,
        "redis_db": args.redis_db,
        "timeout": args.timeout,
        "poll_interval": args.poll_interval,
    }
    return Config.load(path=args.config, overrides=overrides)


if __name__ == "__main__":
    sys.exit(main())
