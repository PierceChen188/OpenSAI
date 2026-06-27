"""决策事件流（§14.2）：放置 / 资源 / 传输三类结构化事件的有界环形缓冲，供前端轮询展示"决策过程"。

从 `UserServer` 抽出的横切模块（P1）。行为与原 `_emit_event`/`_events_snapshot` 完全一致：
只在内存、有界（防膨胀）、线程安全（快照与追加可能跨线程）、异常不影响主流程。
"""
import logging
import threading
import time
from collections import deque


class EventBus:
    def __init__(self, maxlen: int = 400):
        self._events = deque(maxlen=maxlen)
        self._seq = 0
        self._lock = threading.Lock()

    def emit(self, kind: str, payload: dict) -> None:
        """追加一条决策事件（kind ∈ {placement, resource, transfer}）。容错：异常不影响主流程。"""
        try:
            with self._lock:
                self._seq += 1
                ev = {'seq': self._seq, 'ts': round(time.time(), 3), 'kind': kind}
                ev.update(payload)
                self._events.append(ev)
        except Exception as e:
            logging.debug(f"[event] emit 忽略: {e}")

    def snapshot(self) -> list:
        """返回决策事件快照（列表），供桥接经 eventsInfo 回传前端。"""
        with self._lock:
            return list(self._events)
