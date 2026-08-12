"""连接不同 Graph 的领域事件。"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from .core import require_non_empty_string


@dataclass(frozen=True, slots=True)
class Event:
    """按类型路由并携带领域 payload 的不可变消息。"""

    type: str
    payload: Any = None

    def __post_init__(self) -> None:
        """校验事件类型是非空字符串。"""

        require_non_empty_string(self.type, "event type")


Emit = Callable[[Event], None]


class Context:
    """只向当前 Node 暴露跨 Graph 事件发布能力。"""

    __slots__ = ("_emit",)

    def __init__(self, emit: Emit) -> None:
        """绑定 Runtime 的内部事件接收函数。"""

        if not callable(emit):
            raise TypeError("context emitter must be callable")
        self._emit = emit

    def emit(self, event_type: str, payload: Any = None) -> Event:
        """向 Runtime 提交一项跨 Graph 领域事件。"""

        event = Event(event_type, payload)
        self._emit(event)
        return event
