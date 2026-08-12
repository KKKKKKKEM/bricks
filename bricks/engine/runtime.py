"""组合 Event 路由、Graph Worker 和完整 Runtime。"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from functools import partial
from threading import RLock
from typing import Any

from .backends import (
    Emit,
    EventBus,
    GraphExecutor,
    HookableGraphExecutor,
    MemoryEventBus,
    MemoryTaskBackend,
    TaskConsumer,
    TaskPublisher,
    Work,
)
from .core import Output, _validate_timeout, require_non_empty_string
from .errors import (
    BricksRuntimeError,
    EventDispatchError,
    ExecutionError,
    RuntimeClosedError,
    UnknownGraphError,
)
from .events import Event
from .executor import Engine
from .graph import ExecutionPlan, Graph
from .hooks import HookHandle, HookPhase, NodeHook
from .slots import SlotPool, _SlotLease

EventHandler = Callable[[Event], None]


class EventRouter:
    """发布和订阅 Event，并把匹配的 Event 转成队列 Work。"""

    def __init__(
        self,
        *,
        publisher: TaskPublisher,
        events: EventBus | None = None,
        close_injected: bool = False,
    ) -> None:
        if type(close_injected) is not bool:
            raise TypeError("close_injected must be a boolean")
        owned: list[object] = []
        if events is None:
            events = MemoryEventBus()
            owned.append(events)
        self._events = events
        self._publisher = publisher
        self._owned_components = owned
        self._close_injected = close_injected
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
    ) -> EventRouter:
        """订阅 Event，并向队列投递目标 Graph 的 Work。"""

        self._ensure_open()
        event_type = require_non_empty_string(event_type, "subscription event type")
        graph = require_non_empty_string(graph, "route graph")
        queue = require_non_empty_string(queue, "route queue")
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
                partial(self._submit, graph, queue),
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

        try:
            self._ensure_open()
        except BaseException:
            if event._slot_lease is not None:
                event._slot_lease.release()
            raise
        try:
            self._events.publish(event)
        except EventDispatchError:
            raise
        except Exception as exc:
            raise EventDispatchError(event, exc) from exc

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

    def _submit(self, graph: str, queue: str, event: Event) -> None:
        lease = event._slot_lease
        if lease is not None:
            lease.retain()
        try:
            self._publisher.submit(
                queue,
                Work(
                    graph,
                    event.payload,
                    trigger=event,
                    _slot_lease=lease,
                ),
            )
        except BaseException:
            if lease is not None:
                lease.release()
            raise

    def _ensure_open(self) -> None:
        with self._lock:
            if self._closed:
                raise RuntimeClosedError("EventRouter is closed")


class GraphWorker:
    """注册 Graph、消费队列 Work，并执行完整 Graph。"""

    def __init__(
        self,
        *,
        consumer: TaskConsumer,
        executor: GraphExecutor | None = None,
        emit: Emit | None = None,
        close_injected: bool = False,
    ) -> None:
        if type(close_injected) is not bool:
            raise TypeError("close_injected must be a boolean")
        owned: list[object] = []
        if executor is None:
            executor = Engine()
            owned.append(executor)
        self._consumer = consumer
        self._executor = executor
        self._emit = _reject_emit if emit is None else emit
        if not callable(self._emit):
            raise TypeError("worker emitter must be callable")
        self._owned_components = owned
        self._close_injected = close_injected
        self._graphs: dict[str, Graph] = {}
        self._queues: dict[str, tuple[int, SlotPool]] = {}
        self._owned_slot_pools: list[SlotPool] = []
        self._lock = RLock()
        self._closed = False

    @property
    def idle(self) -> bool:
        """返回当前 Worker 是否没有正在执行或排队的 Work。"""

        return self._consumer.idle

    def register(self, name: str, graph: Graph) -> GraphWorker:
        """以稳定名称注册并冻结一张 Graph。"""

        self._ensure_open()
        name = require_non_empty_string(name, "registered graph name")
        if not isinstance(graph, Graph):
            raise TypeError("graph must be a Graph")
        if not graph.frozen:
            graph.freeze()
        with self._lock:
            if name in self._graphs:
                raise BricksRuntimeError(f"duplicate registered graph {name!r}")
            self._graphs[name] = graph
        return self

    def consume(
        self,
        queue: str,
        *,
        concurrency: int = 1,
        slots: SlotPool | None = None,
    ) -> GraphWorker:
        """消费队列；concurrency 控制本地执行，slots 控制逻辑执行链。"""

        self._ensure_open()
        queue = require_non_empty_string(queue, "task queue")
        if type(concurrency) is not int:
            raise TypeError("queue concurrency must be an integer")
        if concurrency < 1:
            raise ValueError("queue concurrency must be at least 1")
        if slots is not None and not isinstance(slots, SlotPool):
            raise TypeError("slots must be a SlotPool or None")
        with self._lock:
            configured = self._queues.get(queue)
            if configured is not None:
                configured_concurrency, configured_slots = configured
                if configured_concurrency != concurrency:
                    raise BricksRuntimeError(
                        f"queue {queue!r} already uses local concurrency "
                        f"{configured_concurrency}"
                    )
                if slots is not None and slots is not configured_slots:
                    raise BricksRuntimeError(
                        f"queue {queue!r} already uses a different SlotPool"
                    )
                return self
            if slots is None:
                slots = SlotPool(concurrency)
                owned_slots = slots
            else:
                owned_slots = None
            try:
                self._consumer.bind(
                    queue,
                    self._execute_work,
                    concurrency=concurrency,
                    slots=slots,
                )
            except BaseException:
                if owned_slots is not None:
                    owned_slots.close()
                raise
            if owned_slots is not None:
                self._owned_slot_pools.append(owned_slots)
            self._queues[queue] = (concurrency, slots)
        return self

    def run(
        self,
        graph: str,
        inputs: Any = None,
        *,
        plan: ExecutionPlan | None = None,
    ) -> tuple[Output, ...]:
        """同步直接执行一个已注册 Graph。"""

        self._ensure_open()
        registered = self._get_graph(graph)
        if plan is None:
            return self._executor.execute(graph, registered, inputs, self._emit)
        return self._executor.execute(
            graph, registered, inputs, self._emit, plan=plan
        )

    async def arun(
        self,
        graph: str,
        inputs: Any = None,
        *,
        plan: ExecutionPlan | None = None,
    ) -> tuple[Output, ...]:
        """在线程中直接执行 Graph，避免阻塞异步调用方。"""

        return await asyncio.to_thread(self.run, graph, inputs, plan=plan)

    def attach(
        self,
        hook: NodeHook | Callable[..., object],
        *,
        phase: HookPhase | str | None = None,
        graph: str | None = None,
        node: str | None = None,
    ) -> HookHandle:
        """动态挂载 Node Hook；变更从下一次 Graph execution 生效。"""

        self._ensure_open()
        if graph is not None:
            graph = require_non_empty_string(graph, "hook graph")
            registered = self._get_graph(graph)
            if node is not None:
                node = require_non_empty_string(node, "hook node")
                if node not in registered.nodes:
                    raise ValueError(f"graph {graph!r} has no node {node!r}")
        if not isinstance(self._executor, HookableGraphExecutor):
            raise TypeError("the configured GraphExecutor does not support hooks")
        return self._executor.attach(
            hook,
            phase=phase,
            graph=graph,
            node=node,
        )

    def wait_idle(self, timeout: float | None = None) -> None:
        """等待当前 Worker 已接受的 Work 完成。"""

        self._consumer.wait_idle(timeout)

    def close(self) -> None:
        """排空本地 Work，并关闭当前 Worker 拥有的组件。"""

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
            _unique(self._consumer, self._executor)
            if self._close_injected
            else tuple(self._owned_components)
        )
        failure = _close_components(reversed(components), failure)
        failure = _close_components(reversed(self._owned_slot_pools), failure)
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

    def _execute_work(self, work: Work) -> None:
        graph = self._get_graph(work.graph)
        lease = work._slot_lease
        if lease is None:
            raise BricksRuntimeError("TaskConsumer dispatched Work without a Slot")
        try:
            with lease.slot._execution_lock:
                emit = partial(self._emit_with_lease, lease)
                self._executor.execute(
                    work.graph,
                    graph,
                    work.inputs,
                    emit,
                    slot=lease.slot,
                )
        except ExecutionError as exc:
            if exc.event is None:
                exc.event = work.trigger
            raise

    def _emit_with_lease(self, lease: _SlotLease, event: Event) -> None:
        # EventBus owns this reference until it finishes dispatching the Event.
        lease.retain()
        forwarded = Event(
            event.type,
            event.payload,
            _slot_lease=lease,
        )
        self._emit(forwarded)

    def _get_graph(self, name: str) -> Graph:
        name = require_non_empty_string(name, "registered graph name")
        with self._lock:
            try:
                return self._graphs[name]
            except KeyError as exc:
                raise UnknownGraphError(
                    f"unknown registered graph {name!r}"
                ) from exc

    def _ensure_open(self) -> None:
        with self._lock:
            if self._closed:
                raise RuntimeClosedError("GraphWorker is closed")


class Runtime:
    """组合 EventRouter 与 GraphWorker 的单进程便利门面。"""

    def __init__(
        self,
        *,
        router: EventRouter | None = None,
        worker: GraphWorker | None = None,
    ) -> None:
        if (router is None) != (worker is None):
            raise TypeError("Runtime requires both router and worker")
        owned: tuple[object, ...] = ()
        if router is None:
            events = MemoryEventBus()
            backend = MemoryTaskBackend()
            executor = Engine()
            router = EventRouter(
                events=events,
                publisher=backend,
            )
            worker = GraphWorker(
                consumer=backend,
                executor=executor,
                emit=router.publish,
            )
            owned = (events, backend, executor)
        if not isinstance(router, EventRouter):
            raise TypeError("router must be an EventRouter")
        if not isinstance(worker, GraphWorker):
            raise TypeError("worker must be a GraphWorker")
        self.router = router
        self.worker = worker
        self._owned_components = owned
        self._closed = False
        self._lock = RLock()

    def register(self, name: str, graph: Graph) -> Runtime:
        self.worker.register(name, graph)
        return self

    def observe(self, event_type: str, handler: EventHandler) -> Runtime:
        self.router.observe(event_type, handler)
        return self

    def route(
        self,
        event_type: str,
        *,
        graph: str,
        queue: str,
        subscription: str | None = None,
    ) -> Runtime:
        self.router.route(
            event_type,
            graph=graph,
            queue=queue,
            subscription=subscription,
        )
        return self

    def consume(
        self,
        queue: str,
        *,
        concurrency: int = 1,
        slots: SlotPool | None = None,
    ) -> Runtime:
        self.worker.consume(queue, concurrency=concurrency, slots=slots)
        return self

    def emit(self, event_or_type: Event | str, payload: Any = None) -> Event:
        return self.router.emit(event_or_type, payload)

    def run(
        self,
        graph: str,
        inputs: Any = None,
        *,
        plan: ExecutionPlan | None = None,
    ) -> tuple[Output, ...]:
        return self.worker.run(graph, inputs, plan=plan)

    async def arun(
        self,
        graph: str,
        inputs: Any = None,
        *,
        plan: ExecutionPlan | None = None,
    ) -> tuple[Output, ...]:
        return await self.worker.arun(graph, inputs, plan=plan)

    def attach(
        self,
        hook: NodeHook | Callable[..., object],
        *,
        phase: HookPhase | str | None = None,
        graph: str | None = None,
        node: str | None = None,
    ) -> HookHandle:
        return self.worker.attach(hook, phase=phase, graph=graph, node=node)

    def on(
        self,
        event_type: str,
        *,
        graph: str,
        queue: str,
        concurrency: int = 1,
        slots: SlotPool | None = None,
        subscription: str | None = None,
    ) -> Runtime:
        """组合注册 Event route，并启动对应 queue 的本地消费者。"""

        # Expose the route only after its local consumer is ready.
        self.consume(queue, concurrency=concurrency, slots=slots)
        return self.route(
            event_type,
            graph=graph,
            queue=queue,
            subscription=subscription,
        )

    def wait_idle(self, timeout: float | None = None) -> None:
        """等待 Router 与 Worker 的级联网络在当前实例内静止。"""

        _validate_timeout(timeout)
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            self.router.wait_idle(_remaining(deadline))
            self.worker.wait_idle(_remaining(deadline))
            if self.router.idle and self.worker.idle:
                return
            if deadline is not None and time.monotonic() >= deadline:
                raise TimeoutError("Runtime did not become idle before timeout")

    def close(self) -> None:
        """排空组合角色，并关闭 Runtime 拥有的底层组件。"""

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
        failure = _close_components((self.worker, self.router), failure)
        failure = _close_components(reversed(self._owned_components), failure)
        if failure is not None:
            raise failure

    def __enter__(self):
        with self._lock:
            if self._closed:
                raise RuntimeClosedError("Runtime is closed")
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        del exc_type, traceback
        try:
            self.close()
        except Exception:
            if exc_value is None:
                raise


def _reject_emit(event: Event) -> None:
    if event._slot_lease is not None:
        event._slot_lease.release()
    raise BricksRuntimeError("GraphWorker has no Event emitter")


def _unique(*components: object) -> tuple[object, ...]:
    unique: list[object] = []
    for component in components:
        if all(component is not item for item in unique):
            unique.append(component)
    return tuple(unique)


def _close_components(
    components: Any,
    failure: BaseException | None,
) -> BaseException | None:
    for component in components:
        try:
            component.close()
        except Exception as exc:  # noqa: BLE001
            if failure is None:
                failure = exc
    return failure


def _remaining(deadline: float | None) -> float | None:
    if deadline is None:
        return None
    return max(0.0, deadline - time.monotonic())
