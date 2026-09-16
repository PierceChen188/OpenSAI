"""适配优化组件·资源声明契约接口（WP-06 · 新开发 2 项）。

按设计文档 §2.5「适配优化组件」落地两个 ADP 侧新开发接口：

    validate_resource_hints(dag, limits) -> list[ValidationIssue]   （§2.5.2）
    resource_hint_to_wire(resource, params) -> dict                 （§2.5.2 / §2.5.3）

复用 WP-01（Leo）已落地的中立契约类型（不重复定义）：

    opensai_contracts.schema.ResourceHint / ResourceLimits / ValidationIssue / Severity
    opensai_contracts.errors.ErrorCode / ContractError

为什么独立成 adaptation.py（而非设计文档 §8.3「2.5」提示的 validator.py / serializer.py）：
  WP-01 已把 `validator.py`（DAGValidator，含中立 `validate_resources`）与 `serializer.py`
  （parse_dag / single_task_dag / public_structure_to_dict）占为共享核心。为避免并行合并
  互相覆盖，ADP 侧两项接口收敛到本模块，并复用上述共享类型，语义与 §2.5 冻结契约一致。

单位语义（§2.1.3）：cores=核 / mem_mb·storage_mb=MiB / bw_mbps=MB/s（仅校验与透传，不换算）。
"""

from __future__ import annotations

from typing import Any, Optional

from .errors import ErrorCode, ContractError
from .schema import (
    ResourceLimits,
    Severity,
    ValidationIssue,
)

__all__ = [
    'validate_resource_hints',
    'resource_hint_to_wire',
    'RESERVED_KEYS',
    'RESOURCE_FIELDS',
    'RESOURCE_WIRE_KEYS',
    'RESOURCE_LIMIT_ATTRS',
]

# ResourceHint 四维字段（§2.5.1）
RESOURCE_FIELDS = ('cores', 'mem_mb', 'bw_mbps', 'storage_mb')

# 每维字段 -> ResourceLimits 上的静态上限属性（Leo schema.ResourceLimits 采用 <dim>_max 约定）
RESOURCE_LIMIT_ATTRS = {
    'cores': 'cores_max',
    'mem_mb': 'mem_mb_max',
    'bw_mbps': 'bw_mbps_max',
    'storage_mb': 'storage_mb_max',
}

# §2.6.3 设备执行配置保留键：由 COM/RSC 注入，算法业务参数不得覆盖。
RESERVED_KEYS = ('_alloc', '_artifact_in', '_artifact_out')

# 追加到 taskInputInfo 根部的四维资源字段（键名与 ResourceHint 字段一致）。
RESOURCE_WIRE_KEYS = ('cores', 'mem_mb', 'storage_mb', 'bw_mbps')


def _is_int(v: Any) -> bool:
    return isinstance(v, int) and not isinstance(v, bool)


def _is_number(v: Any) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def _getattr(obj: Any, name: str, default: Any = None) -> Any:
    """同时兼容 dict 与 dataclass 的字段取值。"""
    if obj is None:
        return default
    if isinstance(obj, dict):
        return obj.get(name, default)
    return getattr(obj, name, default)


def _issue(code: str, path: str, message: str, severity=Severity.ERROR,
           hint: Optional[str] = None, details: Optional[dict] = None) -> ValidationIssue:
    return ValidationIssue(code=code, path=path, message=message,
                           severity=severity, hint=hint, details=details or {})


def _type_ok(dim: str, value: Any) -> bool:
    if dim == 'bw_mbps':
        return _is_number(value)
    return _is_int(value)


def _type_name(dim: str) -> str:
    return 'int|float' if dim == 'bw_mbps' else 'int'


def _check_dimension(dim: str, value: Any, path: str,
                     limits: Optional[ResourceLimits]) -> list[ValidationIssue]:
    """校验单个资源维度的类型、正值与静态上限（§2.5.2）。"""
    field_path = f"{path}.{dim}"
    if value is None:                      # 显式 None = 未声明，合法（§2.5.1）
        return []
    issues: list[ValidationIssue] = []

    if not _type_ok(dim, value):           # 1) 类型
        issues.append(_issue(
            ErrorCode.TYPE_ERROR, field_path,
            f"resource '{dim}' 必须为 {_type_name(dim)}（单位见 2.1.3），得到 {type(value).__name__}",
            details={'field': dim, 'expected': _type_name(dim), 'actual': type(value).__name__},
        ))
        return issues                      # 类型错误后不再做数值比较

    if value <= 0:                          # 2) 正值（§2.5.1）
        issues.append(_issue(
            ErrorCode.VALUE_ERROR, field_path,
            f"resource '{dim}' 必须大于 0，得到 {value}",
            details={'field': dim, 'value': value},
        ))
        return issues

    if limits is not None:                  # 3) 静态上限
        cap = getattr(limits, RESOURCE_LIMIT_ATTRS[dim], None)
        if cap is not None and value > cap:
            issues.append(_issue(
                ErrorCode.VALUE_ERROR, field_path,
                f"resource '{dim}'={value} 超出静态上限 {cap}",
                details={'field': dim, 'value': value, 'limit': cap},
            ))
    return issues


def validate_resource_hints(dag: Any, limits: Optional[ResourceLimits] = None) -> list[ValidationIssue]:
    """校验 DAG 中每个节点的四维 ResourceHint（§2.5.2）。

    检查 cores/mem_mb/bw_mbps/storage_mb 的**类型、正值与静态上限**；不检查固定设备
    （direct_allocation 由 CLU/COM 侧 `validate_placement_targets` 负责）。

    参数
    ----
    dag : DAGSpec | dict
        SDK 公开 DAG 结构（nodes[i].resources 为 ResourceHint），或等价 dict。兼容
        既有 spec 的 ``resource_hint`` 字段名。
    limits : ResourceLimits | None
        静态上限（schema.ResourceLimits，cores_max/mem_mb_max/bw_mbps_max/storage_mb_max）。

    返回
    ----
    list[ValidationIssue]
        空列表 = 全部合法。每个 issue 复用 schema.ValidationIssue，code 取 errors.ErrorCode。

    说明
    ----
    本函数是 §2.5.2 的**独立入口**；与 WP-01 的 `DAGValidator.validate_resources`（只做
    DAGSpec 上的正值+上限，依赖先解析）互补：本函数额外提供 dict 形态与四维**类型**校验。
    """
    issues: list[ValidationIssue] = []
    nodes = _getattr(dag, 'nodes', None)
    if nodes is None:
        return issues
    if not isinstance(nodes, (list, tuple)):
        issues.append(_issue(
            ErrorCode.TYPE_ERROR, 'nodes',
            f"DAG.nodes 必须为数组，得到 {type(nodes).__name__}",
            details={'actual': type(nodes).__name__},
        ))
        return issues

    for i, node in enumerate(nodes):
        prefix = f"nodes[{i}]"
        resource = _getattr(node, 'resources', None)
        if resource is None:
            resource = _getattr(node, 'resource_hint', None)   # 兼容既有 spec 字段名
        if resource is None:
            continue

        # 规整为 {字段: 值}：dict 直接使用（并做未知字段检查）；否则按鸭子类型抽取四维字段
        # （兼容 schema.ResourceHint 及任意带 cores/mem_mb/bw_mbps/storage_mb 的对象）。
        rh: dict = {}
        if isinstance(resource, dict):
            rh = resource
        else:
            for dim in RESOURCE_FIELDS:
                v = getattr(resource, dim, None)
                if v is not None:
                    rh[dim] = v
        if not rh:
            continue

        # 严格拒绝未知资源字段（契约封闭：仅四维）
        for k in rh:
            if k not in RESOURCE_FIELDS:
                issues.append(_issue(
                    ErrorCode.UNKNOWN_FIELD, f"{prefix}.resources.{k}",
                    f"未知资源字段 '{k}'（契约仅允许 {', '.join(RESOURCE_FIELDS)}）",
                    severity=Severity.WARNING,
                    details={'field': k},
                ))
        for dim in RESOURCE_FIELDS:
            if dim in rh:
                issues.extend(_check_dimension(dim, rh[dim], f"{prefix}.resources", limits))

    return issues


def _resource_dict(resource: Any) -> dict:
    """把 ResourceHint（dict 或 dataclass）规整为 {字段: 值}，只取四维字段。"""
    if resource is None:
        return {}
    if isinstance(resource, dict):
        return {k: v for k, v in resource.items() if v is not None}
    out = {}
    for k in RESOURCE_WIRE_KEYS:
        v = getattr(resource, k, None)
        if v is not None:
            out[k] = v
    return out


def resource_hint_to_wire(resource: Any, params: Optional[dict] = None) -> dict:
    """把算法参数 + 资源声明合成为 device 侧 taskInputInfo（§2.5.2 / §2.5.3）。

    参数
    ----
    resource : ResourceHint | None
        调用方资源声明（cores/mem_mb/bw_mbps/storage_mb）。
    params : dict | None
        算法业务参数，直接展开到返回字典根部。

    返回
    ----
    dict
        taskInputInfo：params 根部展开 + 非空资源字段追加。绝不写入 ``details`` 子对象，
        绝不覆盖保留键（``_alloc`` / ``_artifact_in`` / ``_artifact_out``）。

    异常
    ----
    ContractError
        - params 非 dict（code=TYPE_ERROR）；
        - params 或资源字段与保留键冲突（code=RESERVED_KEY）。
    """
    if params is None:
        params = {}
    if not isinstance(params, dict):
        raise ContractError(
            f"params 必须为 dict，得到 {type(params).__name__}",
            code=ErrorCode.TYPE_ERROR,
            details={'actual': type(params).__name__},
        )

    # 禁止保留键覆盖：算法业务参数不得占用设备执行配置保留键。
    for rk in RESERVED_KEYS:
        if rk in params:
            raise ContractError(
                f"算法参数不得覆盖保留键 '{rk}'（由 COM/RSC 注入）",
                code=ErrorCode.RESERVED_KEY,
                details={'reserved_key': rk},
            )

    # 根部展开 params（浅拷贝，不修改调用方 dict）。
    out = dict(params)

    # 追加四维资源字段（仅非空）；禁止写入 details 子对象。
    rh = _resource_dict(resource)
    for k in RESOURCE_WIRE_KEYS:
        if k not in rh:
            continue
        if k in RESERVED_KEYS:
            raise ContractError(
                f"资源字段 '{k}' 与保留键冲突",
                code=ErrorCode.RESERVED_KEY,
                details={'reserved_key': k},
            )
        out[k] = rh[k]

    return out
