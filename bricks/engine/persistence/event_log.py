"""只追加的执行历史协议。"""

from __future__ import annotations

import copy
import uuid
import threading
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from typing import Any, Optional, Protocol


@dataclass(frozen=True, slots=True)
class EventRecord:
    run_id: str
    name: str
    payload: Any = None
    record_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    event_id: Optional[str] = None
    source: Optional[str] = None
    target_run_id: Optional[str] = None
    kind: str = "event"
    graph_id: Optional[str] = None
    sequence: Optional[int] = None
    node_id: Optional[str] = None
    status: Optional[str] = None
    transition_id: Optional[str] = None
    parent_run_id: Optional[str] = None
    graph_version: Optional[str] = None

    def __post_init__(self) -> None:
        # 日志记录必须与后续 Action 对 payload 的修改隔离。
        object.__setattr__(self, "payload", copy.deepcopy(self.payload))


class EventLog(Protocol):
    def append(self, record: EventRecord) -> None: ...

    def read(self, run_id: str) -> list[EventRecord]: ...


class AsyncEventLog(Protocol):
    async def append(self, record: EventRecord) -> None: ...

    async def read(self, run_id: str) -> list[EventRecord]: ...


class InMemoryEventLog:
    def __init__(self) -> None:
        self._records: list[EventRecord] = []
        self._lock = threading.RLock()

    def append(self, record: EventRecord) -> None:
        with self._lock:
            self._records.append(record)

    def read(self, run_id: str) -> list[EventRecord]:
        with self._lock:
            return [
                replace(record)
                for record in self._records
                if record.run_id == run_id
            ]
