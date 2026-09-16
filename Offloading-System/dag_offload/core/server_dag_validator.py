"""服务端校验钩子（WP-02 · 新开发）。

纵深防御：在 dagData 进入调度管线前，用共享的
``opensai_contracts.DAGValidator`` 做服务端二次校验（结构 / 图 / 算法 / 参数），
并编排两个跨 Owner 的权威校验：

  * 资源四维校验 —— ``adaptation.validate_resource_hints``（ADP 权威，WP-06）；
  * 放置目标校验 —— ``cluster_manager.validate_placement_targets``（CLU 权威，WP-05）。

ADR D6：本模块仅**编排**调用，不内联任何 Owner 的校验逻辑。placement 校验在
wire 层单独编排（cluster_manager 期望 wire ``dagData`` 格式），而非通过
``DAGValidator`` 的 ``placement_validator`` 注入（避免 DAGSpec↔wire 与
``ValidationIssue``↔dict 的脆弱类型桥接）。

返回结构化错误（``ErrorDetail.to_dict()`` 形状），与旧 ``ServerDAGValidator``
返回 ``(bool, str)`` 的行为兼容（旧实现保留，本模块提供增强钩子）。
"""

from __future__ import annotations

import os
import sys

# 路径引导：让本模块无论被 server.py（dag_offload 已在 sys.path）还是被
# 测试（显式加 path）还是被直接运行，都能解析 opensai_contracts 与 core.*。
# __file__ = .../OpenSAI/Offloading-System/dag_offload/core/server_dag_validator.py
_DAILY = os.path.dirname(os.path.abspath(__file__))          # .../dag_offload/core
_DAG_OFFLOAD = os.path.dirname(_DAILY)                        # .../dag_offload
_REPO_ROOT = os.path.dirname(os.path.dirname(_DAG_OFFLOAD))   # .../OpenSAI
for _p in (_DAG_OFFLOAD, _REPO_ROOT):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from core.cluster_manager import ClusterManager  # noqa: E402

from opensai_contracts.adaptation import validate_resource_hints  # noqa: E402
from opensai_contracts.errors import ErrorCode  # noqa: E402
from opensai_contracts.schema import (  # noqa: E402
    AlgorithmInfo,
    AlgorithmRegistry,
    ValidationIssue,
)
from opensai_contracts.validator import DAGValidator  # noqa: E402

__all__ = [
    "validate_server_dag",
    "build_runtime_registry",
]


def build_runtime_registry(runtime_names) -> AlgorithmRegistry:
    """用运行时算法名构造 AlgorithmRegistry（无参数 schema，全部 submittable）。

    供服务端在缺少版本化 manifest 时做「算法名白名单」校验，与旧
    ``ServerDAGValidator._check_algorithms`` 的语义对齐（向后兼容）。
    """
    algorithms = {
        str(name): AlgorithmInfo(name=str(name), submittable=True, schema_available=False)
        for name in runtime_names
    }
    return AlgorithmRegistry(algorithms=algorithms)


def _issue_to_dict(issue) -> dict:
    """把 ValidationIssue（或 dict）规整为可 JSON 的 dict。"""
    if isinstance(issue, dict):
        return issue
    if hasattr(issue, "to_dict"):
        return issue.to_dict()
    return {"code": getattr(issue, "code", ErrorCode.INTERNAL_ERROR), "message": str(issue)}


def _placement_issue_to_validation_issue(p: dict) -> ValidationIssue:
    """把 cluster_manager 的 placement issue dict 转成 ValidationIssue。"""
    node_id = p.get("node_id")
    path = f"$.nodes[{node_id - 1}].form.directAllocationId" if isinstance(node_id, int) else "$"
    return ValidationIssue(
        code=p.get("code", "INVALID_DIRECT_ALLOCATION"),
        path=path,
        message=p.get("message", ""),
        details={"node_id": node_id, "value": p.get("value")},
    )


def validate_server_dag(
    payload,
    *,
    algorithm_registry: AlgorithmRegistry | None = None,
    algorithm_whitelist: set | None = None,
    limits=None,
    available_devices=None,
) -> tuple[bool, dict | None]:
    """服务端二次校验 wire ``dagData`` 请求。

    参数
    ----
    payload : dict
        完整 NDJSON 信封（必须含 ``dagData``；可含 request_id/idempotency_key）。
    algorithm_registry : AlgorithmRegistry | None
        版本化 manifest（优先）；缺省时可用 ``algorithm_whitelist`` 降级为名字白名单。
    algorithm_whitelist : set | None
        运行时算法名集合（仅当 algorithm_registry 未提供时生效）。
    limits : ValidationLimits | None
        节点数/边数/负载上限（缺省用默认）。
    available_devices : dict | None
        在线设备快照；提供时 placement 校验额外校验 allocation_index 范围。

    返回
    ----
    (bool, dict | None)
        ``(True, None)`` 合法；``(False, error_detail_dict)`` 非法，dict 为
        ``ErrorDetail.to_dict()`` 形状（code/message/retryable/details）。
    """
    registry = algorithm_registry
    if registry is None and algorithm_whitelist is not None:
        registry = build_runtime_registry(algorithm_whitelist)

    validator = DAGValidator(algorithm_registry=registry, limits=limits)
    report = validator.validate(
        payload,
        resource_validator=validate_resource_hints,
    )

    errors: list[ValidationIssue] = list(report.errors)

    # placement 校验（wire 格式，CLU 权威）：返回 list[dict]，桥接为 ValidationIssue。
    placement = ClusterManager.validate_placement_targets(payload, available_devices)
    errors.extend(_placement_issue_to_validation_issue(p) for p in placement)

    if not errors:
        return True, None

    first = errors[0]
    error_detail = {
        "code": first.code,
        "message": first.message,
        "retryable": False,
        "details": {"issues": [_issue_to_dict(e) for e in errors]},
    }
    return False, error_detail
