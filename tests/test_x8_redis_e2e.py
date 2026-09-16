"""X8-R · 真实 Redis 运行时 E2E（固化测试，可提交）。

WP-09 复审裁定 X8「WSL 无 redis 故不可自建」前提不成立：本测试在 WSL 本机
（无需 sudo、无需真实 device、无需 QUIC）补跑 server.py 真实 Redis 运行时路径：

  * ``import server`` —— 完整 server.py 的 Redis 依赖导入可解析；
  * ``RedisUtils``（predis 层）连接真实 Redis 并 set/get；
  * ``validate_server_dag`` —— dagData 服务端校验钩子；
  * ``JobRegistry.register`` → ULID ``job_…``；``ack_payload`` → 回执；``to_job_result`` → JobResult。

运行约定：
  * 有真实 Redis + redis-py 时：跑完整 E2E 断言。
  * 无 redis-py（系统 python3）或 Redis 未启动时：打印 SKIP 并返回（自研
    runner 无 skip 原语，软跳过不污染全量套件）。

真实 Redis 自建配方见 deliverables/REQ-OpenClaw/08-com/x8r-evidence.md。
"""

import json
import os
import sys
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))
_offloading = _project_root / 'Offloading-System'
if _offloading.is_dir() and str(_offloading) not in sys.path:
    sys.path.insert(0, str(_offloading))
_dag_offload = _offloading / 'dag_offload'
if _dag_offload.is_dir() and str(_dag_offload) not in sys.path:
    sys.path.insert(0, str(_dag_offload))

# RedisUtils 在 import 时读取该环境变量；默认 6380（避开系统默认 6379）。
os.environ.setdefault('OPENSAI_REDIS_PORT', '6380')
_REDIS_PORT = int(os.environ.get('OPENSAI_REDIS_PORT', '6380'))


def _ping_real_redis():
    """Return (available: bool, reason: str). PING 真实 Redis（非 mock）。"""
    try:
        import redis
    except ImportError:
        return False, 'redis-py not installed (system python)'
    try:
        r = redis.Redis(host='127.0.0.1', port=_REDIS_PORT, db=0, socket_connect_timeout=2)
        return r.ping() is True, 'ok'
    except Exception as e:  # noqa: BLE001
        return False, f'{type(e).__name__}: {e}'


def test_x8_real_redis_e2e():
    """真实 Redis 运行时 E2E：import server + RedisUtils + dagData/jobQuery 路径。"""
    available, reason = _ping_real_redis()
    if not available:
        print(f"  SKIP  test_x8_real_redis_e2e: Redis 不可用（{reason}）；"
              f"需 venv + 真实 Redis（配方见 08-com/x8r-evidence.md）")
        return

    import redis  # noqa: F401

    # 1) 完整 server.py 导入（Redis 依赖导入可解析）
    import server
    assert hasattr(server.UserServer, 'job_registry')

    # 2) predis RedisUtils 连接真实 Redis 并 set/get
    from predis.utils.redis_utils import RedisUtils
    ru = RedisUtils()
    assert ru.set_key_value('x8r_e2e_key', 'hello-real-redis') is True
    assert ru.get('x8r_e2e_key') == 'hello-real-redis'

    # 3) dagData 服务端校验钩子（真实 fixture）
    from core.server_dag_validator import validate_server_dag
    fixture = json.loads(
        (_project_root / 'opensai_contracts' / 'fixtures' / 'valid_wire_dag.json')
        .read_text(encoding='utf-8')
    )
    ok, err = validate_server_dag(
        fixture, algorithm_whitelist={'Algorithm_Pi_Calculation', 'Algorithm_Sleep_1'}
    )
    assert ok is True and err is None

    # 4) dagData 注册 → ULID
    from core.job_registry import JobRegistry
    reg = JobRegistry()
    rec, created = reg.register(
        legacy_task_id='x8r_task', dag_data=fixture['dagData'],
        owner='e2e', idempotency_key='x8r-key-1', request_id='req-x8r',
    )
    assert created is True
    assert rec.job_id.startswith('job_')

    # 5) dagData → ACK 回执
    ack = reg.ack_payload(rec, request_id='req-x8r')
    assert ack['dagRspStatus'] is True
    assert ack['job_id'] == rec.job_id
    assert ack['request_id'] == 'req-x8r'

    # 6) jobQuery → JobResult（顶层 `job` 键）
    jr = reg.to_job_result(rec.job_id)
    d = jr.to_dict()
    assert d['job_id'] == rec.job_id
    # JobStatus 为 str-Enum，与字符串值相等；wire 经 json.dumps 后为 "QUEUED"
    assert d['status'] == 'QUEUED'
    assert set(d.keys()) == {'job_id', 'status', 'submitted_at', 'started_at',
                             'completed_at', 'duration_s', 'nodes', 'error'}

    print(f"  PASS  test_x8_real_redis_e2e: real Redis :{_REDIS_PORT} "
          f"register={rec.job_id} ack+to_job_result OK")
