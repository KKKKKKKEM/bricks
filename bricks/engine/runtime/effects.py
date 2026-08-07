"""Durable effect intents staged by domain Outcome handlers."""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from ..types import freeze_value, thaw_value


def _now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True, slots=True)
class StagedEffect:
    """An outbox-ready intent; executing it is an external adapter concern."""

    run_id: str
    topic: str
    payload: Any = None
    id: str = field(default_factory=lambda: uuid.uuid4().hex)
    created_at: datetime = field(default_factory=_now)

    def __post_init__(self) -> None:
        if not self.id or not self.run_id or not self.topic:
            raise ValueError("staged effect identity fields cannot be empty")
        if self.created_at.tzinfo is None:
            raise ValueError("staged effect created_at must be timezone-aware")
        object.__setattr__(self, "payload", freeze_value(self.payload))

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "run_id": self.run_id,
            "topic": self.topic,
            "payload": thaw_value(self.payload),
            "created_at": self.created_at.isoformat(),
        }
