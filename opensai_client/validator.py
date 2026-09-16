"""
OpenSAI SDK client-side validator.

Delegates entirely to :class:`opensai_contracts.validator.DAGValidator` — the
single authoritative validation implementation shared by the client and the
server. This class only adds the client-side concern of obtaining a live
algorithm registry; it never re-implements structural / graph / algorithm /
parameter checks (acceptance: no duplicate or dangling logic).
"""

from __future__ import annotations

from typing import Any

from opensai_contracts import (
    AlgorithmInfo,
    AlgorithmRegistry,
    DAGSpec,
    DAGValidator,
    ValidationLimits,
    ValidationReport,
)
from opensai_contracts.errors import ContractValidationError, ErrorCode

__all__ = ["ClientValidator", "ValidationError"]


class ValidationError(ContractValidationError):
    """Raised when a DAG fails shared validation."""

    default_code = ErrorCode.DAG_INVALID


def _build_registry(algos: Any) -> AlgorithmRegistry:
    algorithms: dict[str, AlgorithmInfo] = {}
    for item in algos or []:
        if isinstance(item, AlgorithmInfo):
            algorithms[item.name] = item
        elif isinstance(item, str):
            algorithms[item] = AlgorithmInfo(name=item, schema_available=False, submittable=False)
        elif isinstance(item, dict):
            try:
                info = AlgorithmInfo.from_dict(item)
                algorithms[info.name] = info
            except Exception:
                continue
    return AlgorithmRegistry(algorithms=algorithms)


class ClientValidator:
    """Client-side DAG validator backed by the shared :class:`DAGValidator`.

    Parameters
    ----------
    client:
        Optional ``OpenSAIClient`` used to lazily fetch the live algorithm
        registry on first validation.
    algorithm_registry:
        Optional pre-built :class:`AlgorithmRegistry`; takes precedence over
        the client-fetched registry.
    limits:
        Optional :class:`ValidationLimits` (payload/node caps).
    """

    def __init__(
        self,
        client: Any = None,
        algorithm_registry: AlgorithmRegistry | None = None,
        limits: ValidationLimits | None = None,
    ) -> None:
        self._client = client
        self._algorithm_registry = algorithm_registry
        self._validator = DAGValidator(algorithm_registry=algorithm_registry, limits=limits)

    @property
    def validator(self) -> DAGValidator:
        """The shared validator this client validator delegates to."""
        return self._validator

    def _ensure_registry(self) -> None:
        if self._algorithm_registry is not None:
            return
        if self._client is None:
            return
        if not hasattr(self._client, "list_algorithms"):
            return
        self._algorithm_registry = _build_registry(self._client.list_algorithms())
        self._validator.algorithm_registry = self._algorithm_registry

    def validate(self, dag: DAGSpec | dict) -> ValidationReport:
        """Validate ``dag`` via the shared :class:`DAGValidator`.

        Returns a :class:`ValidationReport`; never raises on invalid input.
        """
        self._ensure_registry()
        return self._validator.validate(dag)

    def validate_or_raise(self, dag: DAGSpec | dict) -> DAGSpec:
        """Validate and return the normalized :class:`DAGSpec`, or raise."""
        report = self.validate(dag)
        if not report.valid:
            raise ValidationError(
                "; ".join(e.message for e in report.errors),
                code=ErrorCode.DAG_INVALID,
                details={"errors": [e.to_dict() for e in report.errors]},
            )
        return report.normalized_dag
