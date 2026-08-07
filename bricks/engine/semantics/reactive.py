"""把 EventBus 事件路由到图运行实例的响应式语义。"""

from __future__ import annotations

import inspect
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Optional

from ..errors import AmbiguousEventRoute, MachineNotRunnable
from ..events.bus import EventBus, Subscription
from ..events.messages import Event
from ..runtime.lifecycle import Status
from ..runtime.machine import Machine


@dataclass
class ReactiveBinding:
    """一个 Machine 在 EventBus 上的响应式绑定。"""

    runtime: "ReactiveRuntime"
    machine: Machine
    subscription: Subscription
    event_names: frozenset[str]
    async_mode: bool = False

    def close(self) -> bool:
        """解除绑定；重复调用是安全的。"""
        return self.runtime.detach(self)


class ReactiveRuntime:
    """将外部事件转换为 Machine 的 dispatch/resume 调用。

    EventBus 不知道 Machine 的生命周期，ReactiveRuntime 负责判断运行实例
    当前是可运行、等待外部事件还是已经结束，并据此选择正确的入口。
    """

    def __init__(self, events: Optional[EventBus] = None) -> None:
        self.events = events or EventBus()
        self._bindings: dict[str, ReactiveBinding] = {}

    def attach(
        self,
        machine: Machine,
        event_names: Optional[Iterable[str]] = None,
        *,
        auto_start: bool = True,
        async_mode: bool = False,
        on_error: Optional[Callable[[Exception, Event, Machine], Any]] = None,
    ) -> ReactiveBinding:
        """绑定一个同步 Machine。

        未显式提供事件名时，订阅图中声明的全部迁移事件。异步 Machine 应使用
        ``attach_async``，这样启动和事件处理都能在同一个事件循环中完成。
        """
        if async_mode:
            raise TypeError("异步绑定请使用 attach_async()")
        names = self._normalize_names(machine, event_names)

        def handle(event: Event):
            return self._handle_sync(machine, event, names, on_error)

        subscription = self.events.subscribe_any(
            handle, match=lambda event: self._matches(machine, names, event)
        )
        binding = ReactiveBinding(self, machine, subscription, names)
        self._bindings[subscription.token] = binding
        self.events._register_reactive_route(
            subscription.token, machine.context.run_id, names
        )
        try:
            if auto_start and machine.status is Status.CREATED:
                machine.start()
        except Exception:
            self.detach(binding)
            raise
        return binding

    async def attach_async(
        self,
        machine: Machine,
        event_names: Optional[Iterable[str]] = None,
        *,
        auto_start: bool = True,
        on_error: Optional[Callable[[Exception, Event, Machine], Any]] = None,
    ) -> ReactiveBinding:
        """绑定一个异步 Machine，并可选地启动它。"""
        names = self._normalize_names(machine, event_names)

        async def handle(event: Event):
            return await self._handle_async(machine, event, names, on_error)

        subscription = self.events.subscribe_any(
            handle, match=lambda event: self._matches(machine, names, event)
        )
        binding = ReactiveBinding(self, machine, subscription, names, async_mode=True)
        self._bindings[subscription.token] = binding
        self.events._register_reactive_route(
            subscription.token, machine.context.run_id, names
        )
        try:
            if auto_start and machine.status is Status.CREATED:
                await machine.start_async()
        except Exception:
            self.detach(binding)
            raise
        return binding

    def detach(self, binding: ReactiveBinding | Subscription | str) -> bool:
        """解除一个响应式绑定。"""
        if isinstance(binding, ReactiveBinding):
            token = binding.subscription.token
        elif isinstance(binding, Subscription):
            token = binding.token
        else:
            token = binding
        self._bindings.pop(token, None)
        self.events._unregister_reactive_route(token)
        return self.events.unsubscribe(token)

    def route(self, run_id: str, event: Event | str, payload: Any = None) -> list[Any]:
        """向一个已绑定运行发布定向事件，避免多实例广播。"""
        if not any(
            item.machine.context.run_id == run_id
            for item in self._bindings.values()
        ):
            raise MachineNotRunnable(f"reactive runtime has no run {run_id!r}")
        return self.events.publish(self._directed_event(run_id, event, payload))

    async def route_async(
        self, run_id: str, event: Event | str, payload: Any = None
    ) -> list[Any]:
        """Asynchronously publish a directed event to one attached run."""
        if not any(
            item.machine.context.run_id == run_id
            for item in self._bindings.values()
        ):
            raise MachineNotRunnable(f"reactive runtime has no run {run_id!r}")
        return await self.events.publish_async(
            self._directed_event(run_id, event, payload)
        )

    def _matches(
        self,
        machine: Machine,
        names: frozenset[str],
        event: Event,
    ) -> bool:
        if event.name not in names:
            return False
        if event.target_run_id is not None:
            return event.target_run_id == machine.context.run_id
        candidates = self.events._reactive_candidates(event.name)
        if len(candidates) > 1:
            raise AmbiguousEventRoute(
                f"event {event.name!r} matches multiple runs; "
                "use ReactiveRuntime.route()"
            )
        return candidates == frozenset({machine.context.run_id})

    @staticmethod
    def _directed_event(
        run_id: str, event: Event | str, payload: Any = None
    ) -> Event:
        received = event if isinstance(event, Event) else Event(event, payload)
        return Event(
            received.name,
            received.payload,
            received.source,
            event_id=received.event_id,
            created_at=received.created_at,
            target_run_id=run_id,
        )

    @staticmethod
    def _normalize_names(
        machine: Machine, event_names: Optional[Iterable[str]]
    ) -> frozenset[str]:
        if event_names is None:
            return machine.graph.event_names
        return frozenset(event_names)

    @staticmethod
    def _handle_sync(
        machine: Machine,
        event: Event,
        names: frozenset[str],
        on_error: Optional[Callable[[Exception, Event, Machine], Any]],
    ):
        try:
            if machine.status is Status.RUNNING:
                return machine.dispatch(event)
            if machine.status is Status.WAITING:
                waiting = machine.context.waiting or {}
                if waiting.get("kind") == "retry":
                    return None
                expected = waiting.get("resume_event")
                if expected is None or expected == event.name:
                    return machine.resume(event)
            return None
        except Exception as error:
            if on_error is None:
                raise
            return on_error(error, event, machine)

    @staticmethod
    async def _handle_async(
        machine: Machine,
        event: Event,
        names: frozenset[str],
        on_error: Optional[Callable[[Exception, Event, Machine], Any]],
    ):
        try:
            if machine.status is Status.RUNNING:
                return await machine.dispatch_async(event)
            if machine.status is Status.WAITING:
                waiting = machine.context.waiting or {}
                if waiting.get("kind") == "retry":
                    return None
                expected = waiting.get("resume_event")
                if expected is None or expected == event.name:
                    return await machine.resume_async(event)
            return None
        except Exception as error:
            if on_error is None:
                raise
            value = on_error(error, event, machine)
            if inspect.isawaitable(value):
                return await value
            return value
