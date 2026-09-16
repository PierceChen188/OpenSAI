"""Nora 独立复核脚本（WP-06-TEST）：不依赖 Noah 测试，直接对契约行为做独立断言。

按验收条件 #2/#3/#4/#5 逐条核验 §2.5 契约语义。
"""
import sys
from pathlib import Path

root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(root))
sys.path.insert(0, str(root / "Offloading-System"))
sys.path.insert(0, str(root / "Offloading-System" / "dag_offload"))

from dataclasses import dataclass

from opensai_contracts import (
    ResourceLimits,
    Severity,
    validate_resource_hints,
    resource_hint_to_wire,
    ContractError,
    ErrorCode,
    RESERVED_KEYS,
    RESOURCE_FIELDS,
    RESOURCE_WIRE_KEYS,
)

PASS = []
FAIL = []


def check(name, cond, detail=""):
    if cond:
        PASS.append(name)
    else:
        FAIL.append((name, detail))


# 鸭子类型 ResourceHint（与 schema.ResourceHint 同四维）
@dataclass
class RH:
    cores: int = None
    mem_mb: int = None
    bw_mbps: float = None
    storage_mb: int = None


@dataclass
class Node:
    id: int
    resources: RH = None


@dataclass
class DAG:
    nodes: list


def dag_of(*res):
    return DAG(nodes=[Node(id=i + 1, resources=r) for i, r in enumerate(res)])


def codes(issues):
    return [i.code for i in issues]


# ── #2 validate_resource_hints ──────────────────────────────────────────

# 类型：cores/mem_mb/storage_mb 仅 int；bw_mbps 仅 int|float
issues = validate_resource_hints(dag_of(RH(cores=2.5)))
check("type: cores 拒绝 float", codes(issues) == [ErrorCode.TYPE_ERROR], str(issues))
issues = validate_resource_hints(dag_of(RH(mem_mb="x")))
check("type: mem_mb 拒绝 str", codes(issues) == [ErrorCode.TYPE_ERROR], str(issues))
issues = validate_resource_hints(dag_of(RH(storage_mb=True)))
check("type: storage_mb 拒绝 bool", codes(issues) == [ErrorCode.TYPE_ERROR], str(issues))
issues = validate_resource_hints(dag_of(RH(bw_mbps="fast")))
check("type: bw_mbps 拒绝 str", codes(issues) == [ErrorCode.TYPE_ERROR], str(issues))
# bw_mbps 接受 int 与 float（透传）
check("type: bw_mbps 接受 int", validate_resource_hints(dag_of(RH(bw_mbps=100))) == [])
check("type: bw_mbps 接受 float", validate_resource_hints(dag_of(RH(bw_mbps=100.5))) == [])

# 正值：<=0 拒绝
issues = validate_resource_hints(dag_of(RH(cores=0)))
check("positive: cores=0 拒绝", codes(issues) == [ErrorCode.VALUE_ERROR], str(issues))
issues = validate_resource_hints(dag_of(RH(mem_mb=-5)))
check("positive: mem_mb<0 拒绝", codes(issues) == [ErrorCode.VALUE_ERROR], str(issues))
issues = validate_resource_hints(dag_of(RH(bw_mbps=-1.5)))
check("positive: bw_mbps<0 拒绝", codes(issues) == [ErrorCode.VALUE_ERROR], str(issues))
issues = validate_resource_hints(dag_of(RH(bw_mbps=0.0)))
check("positive: bw_mbps=0.0 拒绝", codes(issues) == [ErrorCode.VALUE_ERROR], str(issues))

# 静态上限
lim = ResourceLimits(cores_max=8, mem_mb_max=4096, bw_mbps_max=100.0, storage_mb_max=1024)
issues = validate_resource_hints(dag_of(RH(cores=16)), limits=lim)
check("limit: cores 超上限拒绝", codes(issues) == [ErrorCode.VALUE_ERROR], str(issues))
issues = validate_resource_hints(dag_of(RH(bw_mbps=500.0)), limits=lim)
check("limit: bw_mbps 超上限拒绝", codes(issues) == [ErrorCode.VALUE_ERROR], str(issues))
check("limit: 等于上限合法", validate_resource_hints(dag_of(RH(cores=8, mem_mb=4096, bw_mbps=100.0, storage_mb=1024)), limits=lim) == [])
# 部分上限（None 不设限）
partial = ResourceLimits(cores_max=8)
check("limit: 未设限维度不误报", validate_resource_hints(dag_of(RH(mem_mb=999999)), limits=partial) == [])

# dict 与 dataclass 兼容
check("compat: dict 形态合法", validate_resource_hints({"nodes": [{"id": 1, "resources": {"cores": 2, "mem_mb": 1024}}]}) == [])
check("compat: dataclass 形态合法", validate_resource_hints(dag_of(RH(cores=4, mem_mb=2048, bw_mbps=50.0, storage_mb=512))) == [])
check("compat: legacy resource_hint 别名", validate_resource_hints({"nodes": [{"id": 1, "resource_hint": {"cores": 2}}]}) == [])

# 未知字段 → UNKNOWN_FIELD (WARNING)
issues = validate_resource_hints({"nodes": [{"id": 1, "resources": {"cores": 2, "gpu": 4}}]})
warn = [i for i in issues if i.code == ErrorCode.UNKNOWN_FIELD]
check("unknown: gpu 记 UNKNOWN_FIELD WARNING", len(warn) == 1 and warn[0].severity == Severity.WARNING, str(issues))

# ── #3 resource_hint_to_wire ────────────────────────────────────────────

out = resource_hint_to_wire(None, {"a": 1, "b": "x"})
check("wire: params 根部展开", out == {"a": 1, "b": "x"}, str(out))
out = resource_hint_to_wire(RH(cores=4, mem_mb=2048, bw_mbps=50.0, storage_mb=512), {"p": 1})
check("wire: 四维资源注入根部", out == {"p": 1, "cores": 4, "mem_mb": 2048, "bw_mbps": 50.0, "storage_mb": 512}, str(out))
# 空资源字段不注入（None 字段跳过）
out = resource_hint_to_wire(RH(cores=2), {"a": 1})
check("wire: 空资源字段不注入", out == {"a": 1, "cores": 2}, str(out))
# 禁 details 子对象
out = resource_hint_to_wire(RH(cores=2), {"a": 1})
check("wire: 不写 details 子对象", "details" not in out, str(out))
# 保留键抛错
for rk in RESERVED_KEYS:
    try:
        resource_hint_to_wire(None, {rk: "x"})
        check(f"wire: 保留键 {rk} 抛错", False, "未抛错")
    except ContractError as e:
        check(f"wire: 保留键 {rk} 抛 RESERVED_KEY", e.code == ErrorCode.RESERVED_KEY, str(e.code))
# params 非 dict 抛 TYPE_ERROR
try:
    resource_hint_to_wire(None, "nope")
    check("wire: params 非 dict 抛 TYPE_ERROR", False, "未抛错")
except ContractError as e:
    check("wire: params 非 dict 抛 TYPE_ERROR", e.code == ErrorCode.TYPE_ERROR, str(e.code))
# dict 形态 resource
out = resource_hint_to_wire({"cores": 2, "mem_mb": 512}, {"x": 1})
check("wire: dict resource 注入", out == {"x": 1, "cores": 2, "mem_mb": 512}, str(out))

# ── #4 单位仅透传不换算 ─────────────────────────────────────────────────

# mem_mb / storage_mb 保持 MiB 原值，bw_mbps 保持 MB/s 原值，无任何 *1024 或 /8 换算
out = resource_hint_to_wire(RH(mem_mb=2048, storage_mb=512, bw_mbps=50.0), {})
check("unit: mem_mb 透传 2048（无换算）", out["mem_mb"] == 2048, str(out))
check("unit: storage_mb 透传 512（无换算）", out["storage_mb"] == 512, str(out))
check("unit: bw_mbps 透传 50.0（无换算）", out["bw_mbps"] == 50.0, str(out))
check("unit: cores 透传为核数", out.get("cores") is None or isinstance(out.get("cores"), int), str(out))

# ── #5 __init__.py 显式导入无侧漏 ────────────────────────────────────────

import opensai_contracts as oc
check("init: 包可导入", True)
check("init: validate_resource_hints 可导出", hasattr(oc, "validate_resource_hints"))
check("init: resource_hint_to_wire 可导出", hasattr(oc, "resource_hint_to_wire"))
check("init: ResourceLimits 可导出", hasattr(oc, "ResourceLimits"))
check("init: ContractError/ErrorCode 可导出", hasattr(oc, "ContractError") and hasattr(oc, "ErrorCode"))
# 无 import 侧漏：opensai_contracts 不得反向依赖 opensai_client
import opensai_contracts.adaptation as adp
src_imports = (root / "opensai_contracts" / "adaptation.py").read_text()
check("init: adaptation 不 import opensai_client", "opensai_client" not in src_imports)

print("=" * 60)
print(f"独立复核：{len(PASS)} PASS, {len(FAIL)} FAIL")
for name, detail in FAIL:
    print(f"  FAIL {name} — {detail}")
print("=" * 60)
sys.exit(0 if not FAIL else 1)
