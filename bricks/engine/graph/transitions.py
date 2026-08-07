"""边和迁移定义。"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Optional

from ..types import Action, EventName, FrozenDict, Guard, NodeId


@dataclass(frozen=True, slots=True)
class Transition:
    """两个图节点之间由事件驱动的边。"""

    id: str
    source: NodeId
    event: EventName
    target: Optional[NodeId]
    guard: Optional[Guard] = None
    action: Optional[Action] = None
    priority: int = 0
    metadata: Mapping[str, Any] = field(default_factory=dict)
    order: int = 0

    def __post_init__(self) -> None:
        object.__setattr__(self, "metadata", FrozenDict(self.metadata))
