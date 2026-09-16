"""COM 作业注册表（WP-02 · 新开发）。

按设计文档 §2.7「作业注册表 / jobQuery / 幂等」在服务端落地：

  * ``new_ulid``           —— 生成 job_id 的 ULID 后缀（26 字符 Crockford base32，可排序）。
  * ``compute_dag_digest`` —— 规范化 DAG 内容的 SHA-256 摘要（幂等键的判重依据）。
  * ``JobRegistry``        —— job_id → JobRecord 权威账本，负责：
        - 幂等：同 (client_scope, idempotency_key) 重复提交，同 payload 返回同一 job_id；
                不同 payload 抛 ``IdempotencyConflictError``。
        - 终态不可覆盖：SUCCEEDED/FAILED/CANCELLED 一旦进入，任何不同状态的转移抛
          ``TerminalStateError``。
        - jobQuery：``JobRecord`` → 公开 ``JobResult`` 映射（绝不直接暴露内部记录）。

纯 Python / 无 Redis 依赖，便于在 WSL 单测（Redis 不可用是预期）。
线程安全由调用方（server.py）持 ``data_lock`` 保证；本类自身不引入锁，避免与
协调器既有锁语义叠加。
"""

from __future__ import annotations

import datetime
import hashlib
import json
import os
import sys
import time

# 让本模块可被独立导入（server.py 运行 / 测试直跑）时都能解析 opensai_contracts。
# __file__ = .../OpenSAI/Offloading-System/dag_offload/core/job_registry.py
_REPO_ROOT = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from opensai_contracts.errors import (  # noqa: E402
    ErrorCode,
    IdempotencyConflictError,
    JobNotFoundError,
    TerminalStateError,
)
from opensai_contracts.schema import (  # noqa: E402
    JobRecord,
    JobResult,
    JobStatus,
    is_terminal,
    utc_now,
)

__all__ = [
    "new_ulid",
    "compute_dag_digest",
    "derive_legacy_task_id",
    "JobRegistry",
]

# Crockford base32（排除 I / L / O / U，避免歧义）。
_CROCKFORD = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

# job_id 对外前缀（契约样例：``job_01ARZ3NDEKTSV4RRFFQ69G5FAV``）。
JOB_ID_PREFIX = "job_"


def new_ulid() -> str:
    """生成一个 26 字符 Crockford base32 ULID（时间戳 48bit + 随机 80bit）。

    与标准 ULID 语义一致：前 10 字符编码毫秒时间戳、后 16 字符编码随机数，
    因此字典序近似时间序（可排序）。唯一性由 ``os.urandom`` 保证（80bit 随机）。
    """
    ms = int(time.time() * 1000)
    rand = int.from_bytes(os.urandom(10), "big")
    value = (ms << 80) | rand
    chars = []
    for i in range(25, -1, -1):
        chars.append(_CROCKFORD[(value >> (i * 5)) & 0x1F])
    return "".join(chars)


def compute_dag_digest(dag_data) -> str:
    """返回 DAG 内容的稳定 SHA-256 摘要（幂等判重依据）。

    ``dag_data`` 为 wire ``dagData`` 内部结构（即 ``payload['dagData']``），
    哈希前做键排序 + 紧凑分隔，使等价 JSON 得到一致摘要。不含 request_id /
    idempotency_key（它们属于信封、不应参与 DAG 内容判重）。
    """
    canonical = json.dumps(dag_data, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def derive_legacy_task_id(dag_data) -> str:
    """从 wire ``dagData`` 推导 legacy task_id（与 server.py 历史口径严格一致）。

    task_id = ``taskName`` + ``str(timestamp)``，其中 timestamp 来自 taskCreateTime：
      - 字符串（"%Y-%m-%d %H:%M:%S"）→ 解析为 UTC 秒级 float 时间戳；
      - int/float → 原样使用；
      - 其它/缺失 → 兜底当前时间戳（不抛异常）。
    """
    inner = dag_data.get("dagData", dag_data) if isinstance(dag_data, dict) else {}
    task_name = inner.get("taskName", "")
    tct = inner.get("taskCreateTime")
    if isinstance(tct, str):
        try:
            dt = datetime.datetime.strptime(tct, "%Y-%m-%d %H:%M:%S")
            timestamp = dt.replace(tzinfo=datetime.timezone.utc).timestamp()
        except ValueError:
            timestamp = tct
    elif isinstance(tct, (int, float)) and not isinstance(tct, bool):
        timestamp = tct
    else:
        timestamp = time.time()
    return f"{task_name}{timestamp}"


class JobRegistry:
    """权威作业注册表：job_id → JobRecord + 幂等键 + legacy 映射 + 过期清理。"""

    def __init__(self, ttl: float = 3600.0):
        self._records: dict[str, JobRecord] = {}
        self._by_legacy: dict[str, str] = {}
        # (client_scope, idempotency_key) -> (job_id, dag_digest)
        self._idempotency: dict[tuple[str, str], tuple[str, str]] = {}
        self._deadlines: dict[str, float] = {}
        self._ttl = ttl

    # ── 注册（幂等）───────────────────────────────────────────────────────

    def register(
        self,
        legacy_task_id: str,
        dag_data=None,
        *,
        owner: str = "",
        idempotency_key: str | None = None,
        request_id: str | None = None,
        dag_digest: str | None = None,
    ) -> tuple[JobRecord, bool]:
        """注册一个作业，返回 ``(record, created)``。

        ``created=True`` 表示新建；``created=False`` 表示幂等命中（返回既有记录）。
        同 idempotency_key 但 DAG 内容摘要不同 → 抛 ``IdempotencyConflictError``。
        """
        if dag_digest is None and dag_data is not None:
            dag_digest = compute_dag_digest(dag_data)

        if idempotency_key:
            scope = owner or "default"
            hit = self._idempotency.get((scope, idempotency_key))
            if hit is not None:
                existing_job_id, existing_digest = hit
                if dag_digest and existing_digest and dag_digest != existing_digest:
                    raise IdempotencyConflictError(
                        f"idempotency_key '{idempotency_key}' 已用于不同的 DAG 负载",
                        details={
                            "idempotency_key": idempotency_key,
                            "existing_job_id": existing_job_id,
                        },
                    )
                rec = self._records.get(existing_job_id)
                if rec is not None:
                    return rec, False
                # 记录被清理但幂等键残留 → 落到下方新建并覆盖幂等键。

        job_id = JOB_ID_PREFIX + new_ulid()
        record = JobRecord(
            job_id=job_id,
            legacy_task_id=legacy_task_id,
            owner=owner,
            status=JobStatus.QUEUED,
            request_id=request_id,
            idempotency_key=idempotency_key,
            dag_digest=dag_digest,
            submitted_at=utc_now(),
            expires_at=None,
        )
        self._records[job_id] = record
        self._by_legacy[legacy_task_id] = job_id
        if idempotency_key:
            self._idempotency[(owner or "default", idempotency_key)] = (job_id, dag_digest)
        self._deadlines[job_id] = time.time() + self._ttl
        return record, True

    # ── 查询 ──────────────────────────────────────────────────────────────

    def get(self, job_id: str) -> JobRecord:
        """按 job_id 取记录；未知 → ``JobNotFoundError``。"""
        rec = self._records.get(job_id)
        if rec is None:
            raise JobNotFoundError(
                f"job_id '{job_id}' 不存在或已过期",
                details={"job_id": job_id},
            )
        return rec

    def get_by_legacy(self, legacy_task_id: str) -> JobRecord:
        """按 legacy task_id 取记录；未知 → ``JobNotFoundError``。"""
        job_id = self._by_legacy.get(legacy_task_id)
        if job_id is None:
            raise JobNotFoundError(
                f"legacy_task_id '{legacy_task_id}' 不存在或已过期",
                details={"legacy_task_id": legacy_task_id},
            )
        return self.get(job_id)

    def resolve(self, job_id: str | None = None, legacy_task_id: str | None = None) -> JobRecord:
        """按 job_id（优先）或 legacy_task_id 解析记录。两者都缺 → ``JobNotFoundError``。"""
        if job_id:
            return self.get(job_id)
        if legacy_task_id:
            return self.get_by_legacy(legacy_task_id)
        raise JobNotFoundError(
            "jobQuery 需要 job_id 或 legacy_task_id 之一",
            details={"job_id": job_id, "legacy_task_id": legacy_task_id},
        )

    # ── 状态转移（终态不可覆盖）───────────────────────────────────────────

    def transition(
        self,
        job_id: str,
        status: JobStatus | str,
        *,
        started_at: str | None = None,
        completed_at: str | None = None,
        duration_s: float | None = None,
        nodes: list | None = None,
        error=None,
    ) -> JobRecord:
        """转移作业状态；终态（SUCCEEDED/FAILED/CANCELLED）不可覆盖。

        允许对同一终态的幂等重复设置（无害）；禁止终态 → 任何不同状态（含另一终态）。
        """
        rec = self.get(job_id)
        if is_terminal(rec.status) and str(status) != str(rec.status):
            raise TerminalStateError(
                f"作业 {job_id} 已处于终态 {rec.status}，不可转移到 {status}",
                details={"job_id": job_id, "current": rec.status, "requested": str(status)},
            )
        rec.status = JobStatus(status) if not isinstance(status, JobStatus) else status
        if started_at is not None:
            rec.started_at = started_at
        if completed_at is not None:
            rec.completed_at = completed_at
        if duration_s is not None:
            rec.duration_s = duration_s
        if nodes is not None:
            rec.nodes = nodes
        if error is not None:
            rec.error = error
        return rec

    def transition_by_legacy(self, legacy_task_id: str, status: JobStatus | str, **kwargs) -> JobRecord | None:
        """按 legacy task_id 转移状态；未注册时返回 ``None``（不抛异常，兼容旧任务）。

        终态不可覆盖语义由 :meth:`transition` 保证；服务端在任务完成/早退/丢弃时
        调用本方法更新作业状态，重复终态上报会被安全忽略（TerminalStateError）。
        """
        job_id = self._by_legacy.get(legacy_task_id)
        if job_id is None:
            return None
        return self.transition(job_id, status, **kwargs)

    # ── 公开映射（jobQuery）───────────────────────────────────────────────

    def to_job_result(self, job_id: str) -> JobResult:
        """把内部 JobRecord 映射为公开 JobResult（绝不直接暴露内部记录）。"""
        rec = self.get(job_id)
        return JobResult(
            job_id=rec.job_id,
            status=rec.status,
            submitted_at=rec.submitted_at,
            started_at=rec.started_at,
            completed_at=rec.completed_at,
            duration_s=rec.duration_s,
            nodes=list(rec.nodes) if rec.nodes else [],
            error=rec.error,
        )

    def ack_payload(self, record: JobRecord, request_id: str | None = None) -> dict:
        """构造 dagData 提交回执（向后兼容 + job_id 回执）。

        保留 ``dagRspStatus: True``（旧客户端兼容），并追加 job_id /
        legacy_task_id / status / accepted_at；request_id 非空时回显。
        """
        status = record.status.value if isinstance(record.status, JobStatus) else str(record.status)
        ack = {
            "dagRspStatus": True,
            "job_id": record.job_id,
            "legacy_task_id": record.legacy_task_id,
            "status": status,
            "accepted_at": record.submitted_at,
        }
        if request_id is not None:
            ack["request_id"] = request_id
        return ack

    # ── 维护 ──────────────────────────────────────────────────────────────

    def list_records(self) -> list[JobRecord]:
        """返回当前全部记录（按注册先后，供调试/审计）。"""
        return list(self._records.values())

    def __len__(self) -> int:
        return len(self._records)

    def cleanup_expired(self, now: float | None = None) -> int:
        """清理超过 TTL 的作业记录，返回移除条数。"""
        now = time.time() if now is None else now
        expired = [job_id for job_id, dl in self._deadlines.items() if now > dl]
        for job_id in expired:
            rec = self._records.pop(job_id, None)
            if rec is not None:
                self._by_legacy.pop(rec.legacy_task_id, None)
                if rec.idempotency_key:
                    self._idempotency.pop((rec.owner or "default", rec.idempotency_key), None)
            self._deadlines.pop(job_id, None)
        return len(expired)
