"""每次运行独立的上下文，与领域上下文刻意隔离。"""

from __future__ import annotations

import copy
import uuid
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional

from ..events.messages import Event
from ..types import thaw_value
from .lifecycle import Status


ROOT_RUN_ID_METADATA = "_bricks_root_run_id"


@dataclass
class Context:
    """属于一次 Graph 执行的完整可变上下文。"""

    graph_id: str
    run_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    node_id: Optional[str] = None
    status: Status = Status.CREATED
    data: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)
    last_event: Optional[Event] = None
    attempt: int = 0
    waiting: Optional[dict[str, Any]] = None
    graph_version: str = "1"

    def get(self, name: str, default: Any = None) -> Any:
        return self.data.get(name, default)

    def set(self, name: str, value: Any) -> Any:
        self.data[name] = value
        return value

    def update(self, values: Mapping[str, Any] | None = None, **kwargs: Any) -> None:
        """合并一组业务数据，供 Action 使用。"""
        if values is not None:
            self.data.update(values)
        self.data.update(kwargs)

    def snapshot(self) -> dict[str, Any]:
        """只返回恢复运行实例所需的状态。

        可调用对象、图定义和临时资源对象不应放入快照；它们应该由图和执行器重新解析。
        """
        return {
            "graph_id": self.graph_id,
            "graph_version": self.graph_version,
            "run_id": self.run_id,
            "node_id": self.node_id,
            "status": self.status.value,
            "data": copy.deepcopy(self.data),
            "metadata": copy.deepcopy(self.metadata),
            "last_event": _event_snapshot(self.last_event),
            "attempt": self.attempt,
            "waiting": copy.deepcopy(self.waiting) if self.waiting else None,
        }

    @classmethod
    def from_snapshot(cls, snapshot: Mapping[str, Any]) -> Context:
        waiting = snapshot.get("waiting")
        return cls(
            graph_id=snapshot["graph_id"],
            graph_version=str(snapshot.get("graph_version", "1")),
            run_id=snapshot["run_id"],
            node_id=snapshot.get("node_id"),
            status=Status(snapshot.get("status", Status.CREATED.value)),
            data=thaw_value(snapshot.get("data") or {}),
            metadata=thaw_value(snapshot.get("metadata") or {}),
            last_event=_event_from_snapshot(snapshot.get("last_event")),
            attempt=int(snapshot.get("attempt", 0)),
            waiting=thaw_value(waiting) if waiting else None,
        )


def _event_snapshot(event: Optional[Event]) -> Optional[dict[str, Any]]:
    if event is None:
        return None
    return {
        "name": event.name,
        "payload": thaw_value(event.payload),
        "source": event.source,
        "event_id": event.event_id,
        "created_at": event.created_at.isoformat(),
        "target_run_id": event.target_run_id,
    }


def _event_from_snapshot(value: Any) -> Optional[Event]:
    if not value:
        return None
    created_at = value.get("created_at")
    if isinstance(created_at, str):
        created_at = datetime.fromisoformat(created_at)
    if created_at is None:
        return Event(
            value["name"],
            copy.deepcopy(value.get("payload")),
            value.get("source"),
            event_id=value.get("event_id") or uuid.uuid4().hex,
            target_run_id=value.get("target_run_id"),
        )
    return Event(
        value["name"],
        copy.deepcopy(value.get("payload")),
        value.get("source"),
        event_id=value.get("event_id") or uuid.uuid4().hex,
        created_at=created_at,
        target_run_id=value.get("target_run_id"),
    )
