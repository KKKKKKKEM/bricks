"""图运行实例之间交换的消息。"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional

from ..types import freeze_value, thaw_value


def _now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True, slots=True)
class Event:
    name: str
    payload: Any = None
    source: Optional[str] = None
    event_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    created_at: datetime = field(default_factory=_now)
    target_run_id: Optional[str] = None

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or not self.name:
            raise ValueError("event name cannot be empty")
        if not isinstance(self.event_id, str) or not self.event_id:
            raise ValueError("event_id cannot be empty")
        object.__setattr__(self, "payload", freeze_value(self.payload))

    def to_dict(self) -> dict[str, Any]:
        """Return a detached, JSON-friendly message representation."""
        return {
            "name": self.name,
            "payload": thaw_value(self.payload),
            "source": self.source,
            "event_id": self.event_id,
            "created_at": self.created_at.isoformat(),
            "target_run_id": self.target_run_id,
        }


@dataclass(frozen=True, slots=True)
class RuntimeEvent:
    """一次运行的可观察事实；它不携带 Context 或可执行对象。"""

    name: str
    run_id: str
    graph_id: str
    sequence: int
    timestamp: datetime = field(default_factory=_now)
    parent_run_id: Optional[str] = None
    node_id: Optional[str] = None
    status: Optional[str] = None
    event_name: Optional[str] = None
    event_id: Optional[str] = None
    event_source: Optional[str] = None
    target_run_id: Optional[str] = None
    payload: Any = None
    transition_id: Optional[str] = None
    outcome: Optional[str] = None
    error_type: Optional[str] = None
    error_message: Optional[str] = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "payload", freeze_value(self.payload))

    def to_dict(self) -> dict[str, Any]:
        """返回适合日志和指标适配器使用的普通字典。"""
        return {
            "name": self.name,
            "run_id": self.run_id,
            "graph_id": self.graph_id,
            "sequence": self.sequence,
            "timestamp": self.timestamp.isoformat(),
            "parent_run_id": self.parent_run_id,
            "node_id": self.node_id,
            "status": self.status,
            "event_name": self.event_name,
            "event_id": self.event_id,
            "event_source": self.event_source,
            "target_run_id": self.target_run_id,
            "payload": thaw_value(self.payload),
            "transition_id": self.transition_id,
            "outcome": self.outcome,
            "error_type": self.error_type,
            "error_message": self.error_message,
        }
