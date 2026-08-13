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
    """向当前 Node 暴露事件、Slot 和协作式执行检查点。"""

    __slots__ = ("_checkpoint", "_emit", "_is_cancelled", "_slot")

    def __init__(
        self,
        emit: Emit,
        slot: Slot | None = None,
        *,
        checkpoint: Callable[[], None] | None = None,
        is_cancelled: Callable[[], bool] | None = None,
    ) -> None:
        """绑定 Runtime 的内部事件接收函数。"""

        if not callable(emit):
            raise TypeError("context emitter must be callable")
        if slot is not None and not isinstance(slot, Slot):
            raise TypeError("context slot must be a Slot or None")
        self._emit = emit
        self._slot = slot
        self._checkpoint = _noop if checkpoint is None else checkpoint
        self._is_cancelled = _false if is_cancelled is None else is_cancelled
        if not callable(self._checkpoint) or not callable(self._is_cancelled):
            raise TypeError("context control callbacks must be callable")

    @property
    def slot(self) -> Slot | None:
        """返回随当前逻辑执行链传递的状态槽。"""

        return self._slot

    def emit(self, event_type: str, payload: Any = None) -> Event:
        """向 Runtime 提交一项跨 Graph 领域事件。"""

        event = Event(event_type, payload)
        self._emit(event)
        return event

    @property
    def cancelled(self) -> bool:
        """返回当前 execution 是否已经收到取消请求。"""

        return self._is_cancelled()

    def checkpoint(self) -> None:
        """让同步 Node 协作式响应取消、总超时和单 Node 超时。"""

        self._checkpoint()


def _noop() -> None:
    return None


def _false() -> bool:
    return False
