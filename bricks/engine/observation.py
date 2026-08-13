"""只读、不可干预执行结果的 Runtime 观测事件。"""

from __future__ import annotations

import enum
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from threading import RLock
from types import MappingProxyType
from typing import Any, Protocol


class RuntimeEventKind(str, enum.Enum):
    EXECUTION_STARTED = "execution.started"
    EXECUTION_FINISHED = "execution.finished"
    NODE_STARTED = "node.started"
    NODE_FINISHED = "node.finished"
    EVENT_PUBLISHED = "event.published"
    WORK_SUBMITTED = "work.submitted"
    WORK_FINISHED = "work.finished"


@dataclass(frozen=True, slots=True)
class RuntimeEvent:
    """不携带业务输入输出的轻量生命周期事实。"""

    kind: RuntimeEventKind
    graph: str | None = None
    execution_id: str | None = None
    node: str | None = None
    work_id: str | None = None
    event_type: str | None = None
    status: str | None = None
    error_type: str | None = None
    attributes: Mapping[str, Any] = field(default_factory=dict)
    occurred_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def __post_init__(self) -> None:
        if not isinstance(self.kind, RuntimeEventKind):
            raise TypeError("runtime event kind must be RuntimeEventKind")
        object.__setattr__(self, "attributes", MappingProxyType(dict(self.attributes)))


class RuntimeObserver(Protocol):
    def __call__(self, event: RuntimeEvent) -> None:
        """观察事件；返回值被忽略。"""


class ObserverHandle:
    __slots__ = ("_hub", "_observer")

    def __init__(self, hub: ObservationHub, observer: RuntimeObserver) -> None:
        self._hub = hub
        self._observer = observer

    def detach(self) -> None:
        self._hub._detach(self._observer)


class CompositeObserverHandle:
    __slots__ = ("_handles",)

    def __init__(self, handles: tuple[ObserverHandle, ...]) -> None:
        self._handles = handles

    def detach(self) -> None:
        for handle in self._handles:
            handle.detach()


class ObservationHub:
    """线程安全地分发只读事件；观察者失败不会影响业务执行。"""

    def __init__(self) -> None:
        self._observers: tuple[RuntimeObserver, ...] = ()
        self._lock = RLock()

    def attach(self, observer: RuntimeObserver) -> ObserverHandle:
        if not callable(observer):
            raise TypeError("runtime observer must be callable")
        with self._lock:
            self._observers = (*self._observers, observer)
        return ObserverHandle(self, observer)

    def publish(self, event: RuntimeEvent) -> None:
        with self._lock:
            observers = self._observers
        for observer in observers:
            try:
                observer(event)
            except Exception:  # noqa: BLE001
                # Telemetry must never alter graph semantics.
                continue

    def _detach(self, observer: RuntimeObserver) -> None:
        with self._lock:
            self._observers = tuple(item for item in self._observers if item is not observer)
