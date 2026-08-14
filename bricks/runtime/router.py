"""Event 发布、观察与跨图 Work 路由。"""

from __future__ import annotations

from collections.abc import Callable
from functools import partial
from threading import RLock
from typing import Any

from ..adapters import memory
from ..engine.core import require_non_empty_string
from ..engine.errors import BricksRuntimeError, EventDispatchError, RuntimeClosedError
from ..engine.events import Event
from ..engine.execution import ExecutionLimits
from ..engine.observation import ObservationHub, RuntimeEvent, RuntimeEventKind
from ..spi import EventBus, TaskPublisher, Work
from ._utils import _close_components, _unique

EventHandler = Callable[[Event], None]


class EventRouter:
    """发布和订阅 Event，并把匹配的 Event 转成队列 Work。"""

    def __init__(
        self,
        *,
        publisher: TaskPublisher,
        events: EventBus | None = None,
        close_injected: bool = False,
        observations: ObservationHub | None = None,
    ) -> None:
        if type(close_injected) is not bool:
            raise TypeError("close_injected must be a boolean")
        owned: list[object] = []
        if events is None:
            events = memory.EventBus()
            owned.append(events)
        self._events = events
        self._publisher = publisher
        self._owned_components = owned
        self._close_injected = close_injected
        self._observations = ObservationHub() if observations is None else observations
        self._routes: set[tuple[str, str, str]] = set()
        self._lock = RLock()
        self._closed = False

    @property
    def idle(self) -> bool:
        """返回当前 Router 是否没有正在投递的 Event。"""

        return self._events.idle

    def observe(self, event_type: str, handler: EventHandler) -> EventRouter:
        """注册一个相互独立的 Event 观察者。"""

        self._ensure_open()
        event_type = require_non_empty_string(event_type, "subscription event type")
        if not callable(handler):
            raise TypeError("event handler must be callable")
        self._events.subscribe(event_type, handler)
        return self

    def route(
        self,
        event_type: str,
        *,
        graph: str,
        queue: str,
        subscription: str | None = None,
        limits: ExecutionLimits | None = None,
    ) -> EventRouter:
        """订阅 Event，并向队列投递目标 Graph 的 Work。"""

        self._ensure_open()
        event_type = require_non_empty_string(event_type, "subscription event type")
        graph = require_non_empty_string(graph, "route graph")
        queue = require_non_empty_string(queue, "route queue")
        if limits is None:
            limits = ExecutionLimits()
        if not isinstance(limits, ExecutionLimits):
            raise TypeError("route limits must be ExecutionLimits or None")
        if subscription is None:
            subscription = f"route:{event_type}:{graph}:{queue}"
        else:
            subscription = require_non_empty_string(
                subscription, "route subscription"
            )
        with self._lock:
            route = (event_type, graph, queue)
            if route in self._routes:
                raise BricksRuntimeError(f"duplicate event route {route!r}")
            self._events.subscribe(
                event_type,
                partial(self._submit, graph, queue, limits),
                subscription=subscription,
            )
            self._routes.add(route)
        return self

    def emit(self, event_or_type: Event | str, payload: Any = None) -> Event:
        """发布完整 Event，或从 type 和 payload 创建后发布。"""

        if isinstance(event_or_type, Event):
            if payload is not None:
                raise TypeError("complete Event must not be combined with payload")
            event = event_or_type
        else:
            event = Event(event_or_type, payload)
        self.publish(event)
        return event

    def publish(self, event: Event) -> None:
        """向 EventBus 发布一项 Event，供 GraphWorker emitter 使用。"""

        self._ensure_open()
        try:
            self._events.publish(event)
        except BaseException as exc:
            # EventBus consumed its reference even when dispatch failed. Restore
            # it so a rejected emitter call leaves ownership with its caller.
            if event._slot_lease is not None:
                event._slot_lease.retain()
            if isinstance(exc, EventDispatchError):
                raise
            if isinstance(exc, Exception):
                raise EventDispatchError(event, exc) from exc
            raise
        self._observations.publish(
            RuntimeEvent(RuntimeEventKind.EVENT_PUBLISHED, event_type=event.type)
        )

    def wait_idle(self, timeout: float | None = None) -> None:
        """等待当前 Router 已接受的 Event 投递完成。"""

        self._events.wait_idle(timeout)

    def close(self) -> None:
        """等待投递结束，并关闭当前 Router 拥有的组件。"""

        with self._lock:
            if self._closed:
                return
        failure: BaseException | None = None
        try:
            self.wait_idle()
        except Exception as exc:  # noqa: BLE001
            failure = exc
        with self._lock:
            self._closed = True
        components = (
            _unique(self._events, self._publisher)
            if self._close_injected
            else tuple(self._owned_components)
        )
        failure = _close_components(reversed(components), failure)
        if failure is not None:
            raise failure

    def __enter__(self):
        self._ensure_open()
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        del exc_type, traceback
        try:
            self.close()
        except Exception:
            if exc_value is None:
                raise

    def _submit(
        self,
        graph: str,
        queue: str,
        limits: ExecutionLimits,
        event: Event,
    ) -> None:
        lease = event._slot_lease
        if lease is not None:
            lease.retain()
        try:
            work = Work(
                graph,
                event.payload,
                trigger=event,
                _slot_lease=lease,
                limits=limits,
            )
            self._publisher.submit(queue, work)
            self._observations.publish(
                RuntimeEvent(
                    RuntimeEventKind.WORK_SUBMITTED,
                    graph=graph,
                    work_id=work.id,
                    event_type=event.type,
                    attributes={"queue": queue},
                )
            )
        except BaseException:
            if lease is not None:
                lease.release()
            raise

    def _ensure_open(self) -> None:
        with self._lock:
            if self._closed:
                raise RuntimeClosedError("EventRouter is closed")
