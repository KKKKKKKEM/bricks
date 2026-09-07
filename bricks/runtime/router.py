"""Event 发布、观察与跨图 Work 路由。"""

from __future__ import annotations

from collections.abc import Callable
from contextvars import ContextVar
from functools import partial
from threading import RLock
from typing import Any

from ..adapters import memory
from ..engine.core import require_non_empty_string
from ..engine.errors import BricksRuntimeError, EventDispatchError, RuntimeClosedError
from ..engine.events import Event
from ..engine.execution import ExecutionLimits
from ..engine.observation import (
    ObservationHub,
    ObserverHandle,
    RuntimeEvent,
    RuntimeEventKind,
    RuntimeObserver,
)
from ..spi import EventBus, SlotLease, TaskPublisher, Work
from ._utils import _close_components, _unique

EventHandler = Callable[[Event], None]
_LOCAL_LEASE: ContextVar[SlotLease | None] = ContextVar(
    "bricks_local_event_lease",
    default=None,
)


class EventRouter:
    """发布和订阅 Event，并把匹配的 Event 转成队列 Work。

    Attributes:
        _events: 事件传输实现。
        _publisher: 向命名通道提交工作的发布能力。
        _owned_components: 当前组件负责关闭的底层资源集合。
        _close_injected: 是否负责关闭调用方注入的底层组件。
        _observations: 当前组件的只读生命周期事件分发中心。
        _routes: 已注册的事件到 Graph 路由关系。
        _lock: 保护当前组件共享状态的进程内互斥锁。
        _closed: 当前组件是否已停止接受新工作。
    """

    def __init__(
        self,
        *,
        publisher: TaskPublisher,
        events: EventBus | None = None,
        close_injected: bool = False,
        observations: ObservationHub | None = None,
    ) -> None:
        """装配事件传输、任务发布和观察能力，明确注入资源的所有权。

        Args:
            publisher: 负责向命名通道投递工作的发布能力。
            events: 注入的事件传输实现。
            close_injected: 是否由当前组件关闭注入的底层资源。
            observations: 负责分发生命周期事件的观察中心。

        Raises:
            TypeError: 参数类型或接口实现不符合当前契约。
        """

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
        """返回当前 Router 是否没有正在投递的 Event。

        Returns:
            没有未完成工作时为 True，否则为 False。
        """

        return self._events.idle

    def observe(self, event_type: str, handler: EventHandler) -> EventRouter:
        """注册一个相互独立的 Event 观察者。

        Args:
            event_type: 用于订阅或路由匹配的事件类型。
            handler: 接收事件或投递的处理函数。

        Returns:
            当前实例，可继续进行链式组合。

        Raises:
            TypeError: 参数类型或接口实现不符合当前契约。
        """

        self._ensure_open()
        event_type = require_non_empty_string(event_type, "subscription event type")
        if not callable(handler):
            raise TypeError("event handler must be callable")
        self._events.subscribe(event_type, handler)
        return self

    def observe_runtime(self, observer: RuntimeObserver) -> ObserverHandle:
        """订阅当前 Router 发布的只读生命周期事件。

        Args:
            observer: 接收只读生命周期事件的观察者。

        Returns:
            用于卸载本次注册的句柄。
        """

        return self._observations.attach(observer)

    def route(
        self,
        event_type: str,
        *,
        graph: str,
        queue: str,
        subscription: str | None = None,
        limits: ExecutionLimits | None = None,
    ) -> EventRouter:
        """订阅 Event，并向队列投递目标 Graph 的 Work。

        Args:
            event_type: 用于订阅或路由匹配的事件类型。
            graph: 目标 Graph 定义或其注册名称，以类型声明为准。
            queue: 命名消费通道。
            subscription: 竞争消费组名称，None 创建独立订阅。
            limits: 本次执行独立使用的步数和时长限制。

        Returns:
            当前实例，可继续进行链式组合。

        Raises:
            TypeError: 参数类型或接口实现不符合当前契约。
        """

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
            subscription = require_non_empty_string(subscription, "route subscription")
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
        """发布完整 Event，或从 type 和 payload 创建后发布。

        Args:
            event_or_type: 已有事件实例，或用于构造事件的类型字符串。
            payload: 事件携带的领域数据。

        Returns:
            事件传输已经接受的 Event 实例。

        Raises:
            TypeError: 参数类型或接口实现不符合当前契约。
        """

        if isinstance(event_or_type, Event):
            if payload is not None:
                raise TypeError("complete Event must not be combined with payload")
            event = event_or_type
        else:
            event = Event(event_or_type, payload)
        self.publish(event)
        return event

    def publish(self, event: Event) -> None:
        """向 EventBus 发布一项 Event，供 GraphWorker emitter 使用。

        Args:
            event: 需要发布、观察或处理的事件。

        Raises:
            EventDispatchError: 事件投递失败，保留原始原因。
        """

        self._ensure_open()
        try:
            self._events.publish(event)
        except BaseException as exc:
            if isinstance(exc, EventDispatchError):
                raise
            if isinstance(exc, Exception):
                raise EventDispatchError(event, exc) from exc
            raise
        self._observations.publish(
            RuntimeEvent(RuntimeEventKind.EVENT_PUBLISHED, event_type=event.type)
        )

    def publish_local(self, event: Event, lease: SlotLease) -> None:
        """发布 Event，并仅为同步本地路由关联当前 Slot lease。

        Args:
            event: 需要发布、观察或处理的事件。
            lease: 当前进程内执行槽的引用与串行执行能力。
        """

        token = _LOCAL_LEASE.set(lease)
        try:
            self.publish(event)
        finally:
            _LOCAL_LEASE.reset(token)

    def wait_idle(self, timeout: float | None = None) -> None:
        """等待当前 Router 已接受的 Event 投递完成。

        Args:
            timeout: 等待或执行时限，单位秒；None 表示不设置时限。
        """

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
        """进入资源作用域并返回当前句柄。

        Returns:
            当前资源管理对象。
        """

        self._ensure_open()
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        """退出资源作用域，执行对应的关闭或卸载操作。

        Args:
            exc_type: 离开上下文时的异常类型，没有异常时为 None。
            exc_value: 离开上下文时的异常实例，没有异常时为 None。
            traceback: 离开上下文时的异常栈，没有异常时为 None。
        """

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
        """将已路由的事件转换为目标通道的工作投递。

        Args:
            graph: 目标 Graph 定义或其注册名称，以类型声明为准。
            queue: 命名消费通道。
            limits: 本次执行独立使用的步数和时长限制。
            event: 需要发布、观察或处理的事件。
        """

        lease = _LOCAL_LEASE.get()
        if lease is not None:
            lease.retain()
        try:
            work = Work(
                graph,
                event.payload,
                trigger=event,
                limits=limits,
            )
            submit_local = getattr(self._publisher, "submit_local", None)
            if lease is not None and callable(submit_local):
                submit_local(queue, work, lease)
                lease = None
            else:
                self._publisher.submit(queue, work)
                if lease is not None:
                    lease.release()
                    lease = None
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
        """拒绝对已经关闭的组件继续提交工作。

        Raises:
            RuntimeClosedError: 当前运行时角色已经关闭。
        """

        with self._lock:
            if self._closed:
                raise RuntimeClosedError("EventRouter is closed")
