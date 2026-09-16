"""opensai_contracts —— 中立契约包（APP 与 COM 共同依赖，禁止 COM 反向依赖 opensai_client）。

多工作包共享的中立契约，统一出口：

  * errors.py      —— 统一 error（ErrorCode / ErrorDetail / ContractError 等），WP-01（Leo）
  * schema.py      —— 契约数据结构（ResourceHint / DAGSpec / ValidationIssue / …），WP-01（Leo）
  * validator.py   —— 共享 DAGValidator（跨 APP/COM 校验，仅编排、不跨 Owner 内联），WP-01（Leo）
  * serializer.py  —— parse_dag / dag_spec_to_dag_data / node_to_wire 等 SDK↔wire 转换，WP-01（Leo）
  * protocol.py    —— 控制面信封（Request/Response/EventEnvelope），WP-01（Leo）
  * adaptation.py  —— 适配优化组件两项新开发接口，WP-06（Noah / ADP）

ADP 侧（WP-06）在 adaptation.py 落地：validate_resource_hints / resource_hint_to_wire。
统一 error 以 errors.py 为权威；ResourceHint / ResourceLimits / ValidationIssue 等以
schema.py 为权威。CLU 的 validate_placement_targets 权威实现在 cluster_manager.py
（WP-05），由编排方注入 DAGValidator.validate（ADR D6），不在本包内联。
"""

from .errors import *
from .errors import __all__ as _errors_all
from .schema import *
from .schema import __all__ as _schema_all
from .protocol import *
from .protocol import __all__ as _protocol_all

from .validator import (
    DAGValidator,
    ValidationLimits,
    AlgorithmRegistry,
    ParameterSpec,
    ResourceLimits,
    load_algorithm_registry,
    merge_runtime_registry,
    validate_dag_request,
    find_cycle_path,
    topological_order,
    is_acyclic,
)

from .serializer import (
    parse_dag,
    dag_spec_to_dag_data,
    node_to_wire,
    wire_to_dag_spec,
    job_ack_from_wire,
    remote_error_from_wire,
    node_result_from_wire,
    dag_result_to_job_result,
    encode_legacy_request,
)

from .adaptation import (
    validate_resource_hints,
    resource_hint_to_wire,
    RESERVED_KEYS,
    RESOURCE_FIELDS,
    RESOURCE_WIRE_KEYS,
    RESOURCE_LIMIT_ATTRS,
)

__all__ = list(dict.fromkeys(
    list(_errors_all) + list(_schema_all) + list(_protocol_all)
    + [
        'DAGValidator', 'ValidationLimits', 'AlgorithmRegistry', 'ParameterSpec',
        'ResourceLimits', 'load_algorithm_registry', 'merge_runtime_registry',
        'validate_dag_request', 'find_cycle_path', 'topological_order', 'is_acyclic',
        'parse_dag', 'dag_spec_to_dag_data', 'node_to_wire', 'wire_to_dag_spec',
        'job_ack_from_wire', 'remote_error_from_wire', 'node_result_from_wire',
        'dag_result_to_job_result', 'encode_legacy_request',
        'validate_resource_hints', 'resource_hint_to_wire',
        'RESERVED_KEYS', 'RESOURCE_FIELDS', 'RESOURCE_WIRE_KEYS', 'RESOURCE_LIMIT_ATTRS',
    ]
))

__version__ = "0.1.0"
