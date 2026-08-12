"""连接不同 Graph 的领域事件。"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from .core import require_non_empty_string
from .slots import Slot, _SlotLease


@dataclass(frozen=True, slots=True)
class Event:
    """按类型路由并携带领域 payload 的不可变消息。"""

    type: str
    payload: Any = None
    _slot_lease: _SlotLease | None = field(
        default=None,
        compare=False,
        repr=False,
        kw_only=True,
    )

    def __post_init__(self) -> None:
        """校验事件类型是非空字符串。"""

        require_non_empty_string(self.type, "event type")


Emit = Callable[[Event], None]


class Context:
    """只向当前 Node 暴露跨 Graph 事件发布能力。"""

    __slots__ = ("_emit", "_slot")

    def __init__(self, emit: Emit, slot: Slot | None = None) -> None:
        """绑定 Runtime 的内部事件接收函数。"""

        if not callable(emit):
            raise TypeError("context emitter must be callable")
        if slot is not None and not isinstance(slot, Slot):
            raise TypeError("context slot must be a Slot or None")
        self._emit = emit
        self._slot = slot

    @property
    def slot(self) -> Slot | None:
        """返回随当前逻辑执行链传递的状态槽。"""

        return self._slot

    def emit(self, event_type: str, payload: Any = None) -> Event:
        """向 Runtime 提交一项跨 Graph 领域事件。"""

        event = Event(event_type, payload)
        self._emit(event)
        return event
