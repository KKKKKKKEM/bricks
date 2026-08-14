"""组合 Event 路由、Graph Worker 和完整 Runtime。"""

from __future__ import annotations

import time
from collections import OrderedDict
from collections.abc import AsyncIterator, Callable, Iterable, Iterator
from concurrent.futures import Future, ThreadPoolExecutor, wait
from functools import partial
from threading import RLock
from typing import Any

from .backends import (
    Delivery,
    DeliveryResult,
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
from .execution import Execution, ExecutionLimits, ExecutionStatus
from .executor import Engine
from .graph import ExecutionPlan, Graph
from .hooks import HookHandle, HookPhase, NodeHook
from .observation import (
    CompositeObserverHandle,
    ObservationHub,
    ObserverHandle,
    RuntimeEvent,
    RuntimeEventKind,
    RuntimeObserver,
)
from .policies import InputSelector, PolicyRegistry
from .plugins import (
    CAP_EVENT_BUS,
    CAP_EVENT_ROUTER,
    CAP_GRAPH_EXECUTOR,
    CAP_GRAPH_WORKER,
    CAP_INPUT_SELECTOR,
    CAP_NODE_HOOK,
    CAP_RUNTIME_OBSERVER,
    CAP_TASK_BACKEND,
    Plugin,
    PluginContext,
    PluginDescriptor,
    PluginHost,
    NodeHookContribution,
)
from .slots import Slot, SlotPool, _SlotLease

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
            events = MemoryEventBus()
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


class GraphWorker:
    """注册 Graph、消费队列 Work，并执行完整 Graph。"""

    def __init__(
        self,
        *,
        consumer: TaskConsumer,
        executor: GraphExecutor | None = None,
        emit: Emit | None = None,
        close_injected: bool = False,
        observations: ObservationHub | None = None,
        policies: PolicyRegistry | None = None,
    ) -> None:
        if type(close_injected) is not bool:
            raise TypeError("close_injected must be a boolean")
        owned: list[object] = []
        if observations is None:
            observations = ObservationHub()
        if executor is None:
            executor = Engine(observations=observations)
            owned.append(executor)
        self._consumer = consumer
        self._executor = executor
        self._emit = _reject_emit if emit is None else emit
        if not callable(self._emit):
            raise TypeError("worker emitter must be callable")
        self._owned_components = owned
        self._close_injected = close_injected
        self._observations = observations
        self._policies = PolicyRegistry() if policies is None else policies
        self._graphs: dict[str, Graph] = {}
        self._queues: dict[str, tuple[int, SlotPool]] = {}
        self._owned_slot_pools: list[SlotPool] = []
        self._lock = RLock()
        self._direct_executor = ThreadPoolExecutor(
            thread_name_prefix="bricks-direct",
        )
        self._direct_pending: set[Future[tuple[Output, ...]]] = set()
        self._executions: OrderedDict[str, Execution] = OrderedDict()
        self._pending_hooks: dict[
            str,
            list[tuple[NodeHook | Callable[..., object], HookPhase | str | None, str | None]],
        ] = {}
        self._history_limit = 1000
        self._closed = False

    @property
    def idle(self) -> bool:
        """返回当前 Worker 是否没有正在执行或排队的 Work。"""

        with self._lock:
            return (
                self._consumer.idle
                and not self._direct_pending
                and all(execution.done for execution in self._executions.values())
            )

    def register(self, name: str, graph: Graph) -> GraphWorker:
        """以稳定名称注册并冻结一张 Graph。"""

        self._ensure_open()
        name = require_non_empty_string(name, "registered graph name")
        if not isinstance(graph, Graph):
            raise TypeError("graph must be a Graph")
        if not graph.frozen:
            graph.freeze(self._policies)
        with self._lock:
            if name in self._graphs:
                raise BricksRuntimeError(f"duplicate registered graph {name!r}")
            pending = tuple(self._pending_hooks.get(name, ()))
            for _, _, node in pending:
                if node is not None and node not in graph.nodes:
                    raise ValueError(f"graph {name!r} has no node {node!r}")
            self._graphs[name] = graph
            self._pending_hooks.pop(name, None)
        for hook, phase, node in pending:
            self._attach_executor_hook(
                hook,
                phase=phase,
                graph=name,
                node=node,
            )
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
                    self._execute_delivery,
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
        max_steps: int = 0,
        timeout: float | None = None,
        output_buffer: int = 64,
    ) -> tuple[Output, ...]:
        """同步直接执行一个已注册 Graph。"""

        return self.start(
            graph,
            inputs,
            plan=plan,
            max_steps=max_steps,
            timeout=timeout,
            output_buffer=output_buffer,
        ).result()

    def start(
        self,
        graph: str,
        inputs: Any = None,
        *,
        plan: ExecutionPlan | None = None,
        max_steps: int = 0,
        timeout: float | None = None,
        output_buffer: int = 64,
    ) -> Execution:
        """在后台启动 Graph，并立即返回可等待和取消的 Execution。"""

        execution = self._new_execution(
            graph,
            max_steps=max_steps,
            timeout=timeout,
            output_buffer=output_buffer,
        )
        self._submit_direct(execution, inputs, plan)
        return execution

    def _submit_direct(
        self,
        execution: Execution,
        inputs: Any,
        plan: ExecutionPlan | None,
    ) -> None:
        with self._lock:
            try:
                future = self._direct_executor.submit(
                    self._execute_direct,
                    execution,
                    inputs,
                    plan,
                )
            except BaseException as exc:
                execution._fail(exc)
                raise
            self._direct_pending.add(future)
            future.add_done_callback(self._direct_done)

    async def arun(
        self,
        graph: str,
        inputs: Any = None,
        *,
        plan: ExecutionPlan | None = None,
        max_steps: int = 0,
        timeout: float | None = None,
        output_buffer: int = 64,
    ) -> tuple[Output, ...]:
        """在线程中直接执行 Graph，避免阻塞异步调用方。"""

        execution = self.start(
            graph,
            inputs,
            plan=plan,
            max_steps=max_steps,
            timeout=timeout,
            output_buffer=output_buffer,
        )
        return await execution

    def iter(
        self,
        graph: str,
        inputs: Any = None,
        *,
        plan: ExecutionPlan | None = None,
        max_steps: int = 0,
        timeout: float | None = None,
        output_buffer: int = 64,
    ) -> Iterator[Output]:
        """启动 Graph 并同步迭代 terminal Output。"""

        execution = self._new_execution(
            graph,
            max_steps=max_steps,
            timeout=timeout,
            output_buffer=output_buffer,
        )
        stream = iter(execution)
        self._submit_direct(execution, inputs, plan)
        return stream

    def aiter(
        self,
        graph: str,
        inputs: Any = None,
        *,
        plan: ExecutionPlan | None = None,
        max_steps: int = 0,
        timeout: float | None = None,
        output_buffer: int = 64,
    ) -> AsyncIterator[Output]:
        """启动 Graph 并异步迭代 terminal Output。"""

        execution = self._new_execution(
            graph,
            max_steps=max_steps,
            timeout=timeout,
            output_buffer=output_buffer,
        )
        stream = execution.__aiter__()
        self._submit_direct(execution, inputs, plan)
        return stream

    def get_execution(self, execution_id: str) -> Execution:
        """按 ID 返回当前进程保留的 Execution。"""

        execution_id = require_non_empty_string(execution_id, "execution id")
        with self._lock:
            try:
                return self._executions[execution_id]
            except KeyError as exc:
                raise BricksRuntimeError(
                    f"unknown execution {execution_id!r}"
                ) from exc

    def executions(self) -> tuple[Execution, ...]:
        """返回当前进程保留的 Execution 快照。"""

        with self._lock:
            return tuple(self._executions.values())

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
        return self._attach_executor_hook(
            hook,
            phase=phase,
            graph=graph,
            node=node,
        )

    def contribute_hook(
        self,
        hook: NodeHook | Callable[..., object],
        *,
        phase: HookPhase | str | None = None,
        graph: str | None = None,
        node: str | None = None,
    ) -> HookHandle | None:
        """安装插件 Hook；目标 Graph 尚未注册时延迟绑定。"""

        self._ensure_open()
        if graph is None:
            if node is not None:
                raise ValueError("node-scoped hook requires graph")
            return self._attach_executor_hook(hook, phase=phase)
        graph = require_non_empty_string(graph, "hook graph")
        with self._lock:
            registered = self._graphs.get(graph)
            if registered is None:
                if node is not None:
                    node = require_non_empty_string(node, "hook node")
                self._pending_hooks.setdefault(graph, []).append((hook, phase, node))
                return None
        if node is not None:
            node = require_non_empty_string(node, "hook node")
            if node not in registered.nodes:
                raise ValueError(f"graph {graph!r} has no node {node!r}")
        return self._attach_executor_hook(
            hook, phase=phase, graph=graph, node=node
        )

    def _attach_executor_hook(
        self,
        hook: NodeHook | Callable[..., object],
        *,
        phase: HookPhase | str | None = None,
        graph: str | None = None,
        node: str | None = None,
    ) -> HookHandle:
        if not isinstance(self._executor, HookableGraphExecutor):
            raise TypeError("the configured GraphExecutor does not support hooks")
        return self._executor.attach(
            hook, phase=phase, graph=graph, node=node
        )

    def register_policy(self, name: str, selector: InputSelector) -> GraphWorker:
        """注册 selector contribution；使用它的 Graph 必须尚未冻结。"""

        self._ensure_open()
        self._policies.register(name, selector)
        return self

    def observe_runtime(self, observer: RuntimeObserver) -> ObserverHandle:
        return self._observations.attach(observer)

    def wait_idle(self, timeout: float | None = None) -> None:
        """等待当前 Worker 已接受的 Work 完成。"""

        _validate_timeout(timeout)
        deadline = None if timeout is None else time.monotonic() + timeout
        failure: BaseException | None = None
        while True:
            try:
                self._consumer.wait_idle(_remaining(deadline))
            except BaseException as exc:
                if not self._consumer.idle:
                    raise
                if failure is None:
                    failure = exc
            with self._lock:
                pending = tuple(self._direct_pending)
                active = tuple(
                    execution
                    for execution in self._executions.values()
                    if not execution.done
                )
            if pending:
                _, unfinished = wait(pending, timeout=_remaining(deadline))
                if unfinished:
                    raise TimeoutError("GraphWorker did not become idle")
            for execution in active:
                if not execution.wait(_remaining(deadline)):
                    raise TimeoutError("GraphWorker did not become idle")
            if self.idle:
                if failure is not None:
                    raise failure
                return

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
        self._direct_executor.shutdown(wait=True)
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

    def _execute_delivery(self, delivery: Delivery) -> DeliveryResult:
        """执行可确认交付并返回明确的 backend 决定。"""

        work = delivery.work
        try:
            self._execute_work(work)
        except BaseException as exc:
            self._observations.publish(
                RuntimeEvent(
                    RuntimeEventKind.WORK_FINISHED,
                    graph=work.graph,
                    execution_id=work.id,
                    work_id=work.id,
                    status="rejected",
                    error_type=type(exc).__name__,
                    attributes={"attempt": delivery.attempt},
                )
            )
            return DeliveryResult.reject(exc)
        self._observations.publish(
            RuntimeEvent(
                RuntimeEventKind.WORK_FINISHED,
                graph=work.graph,
                execution_id=work.id,
                work_id=work.id,
                status="acked",
                attributes={"attempt": delivery.attempt},
            )
        )
        return DeliveryResult.ack()

    def _execute_work(self, work: Work) -> None:
        execution = Execution(
            work.graph,
            limits=work.limits,
            id=work.id,
        )
        self._record_execution(execution)
        try:
            graph = self._get_graph(work.graph)
            lease = work._slot_lease
            if lease is None:
                raise BricksRuntimeError("TaskConsumer dispatched Work without a Slot")
            with lease.slot._execution_lock:
                emit = partial(self._emit_with_lease, lease)
                self._execute(
                    execution,
                    graph,
                    work.inputs,
                    emit,
                    slot=lease.slot,
                )
        except BaseException as exc:
            if execution.status is ExecutionStatus.PENDING:
                execution._start()
            execution._fail(exc)
            if isinstance(exc, ExecutionError) and exc.event is None:
                exc.event = work.trigger
            raise

    def _new_execution(
        self,
        graph: str,
        *,
        max_steps: int,
        timeout: float | None,
        output_buffer: int,
    ) -> Execution:
        self._ensure_open()
        self._get_graph(graph)
        execution = Execution(
            graph,
            limits=ExecutionLimits(max_steps, timeout),
            output_buffer=output_buffer,
        )
        self._record_execution(execution)
        return execution

    def _execute_direct(
        self,
        execution: Execution,
        inputs: Any,
        plan: ExecutionPlan | None,
    ) -> tuple[Output, ...]:
        graph = self._get_graph(execution.graph)
        return self._execute(
            execution,
            graph,
            inputs,
            self._emit,
            plan=plan,
        )

    def _execute(
        self,
        execution: Execution,
        graph: Graph,
        inputs: Any,
        emit: Emit,
        *,
        plan: ExecutionPlan | None = None,
        slot: Slot | None = None,
    ) -> tuple[Output, ...]:
        execution._bind_node_timeouts(graph._execution_timeouts())
        if not execution._start():
            return execution.result(0)
        self._observations.publish(
            RuntimeEvent(
                RuntimeEventKind.EXECUTION_STARTED,
                graph=execution.graph,
                execution_id=execution.id,
            )
        )
        try:
            outputs = self._executor.execute(
                execution.graph,
                graph,
                inputs,
                emit,
                plan=plan,
                slot=slot,
                execution=execution,
            )
            execution._complete_outputs(outputs)
        except BaseException as exc:
            execution._fail(exc)
            self._observations.publish(
                RuntimeEvent(
                    RuntimeEventKind.EXECUTION_FINISHED,
                    graph=execution.graph,
                    execution_id=execution.id,
                    status=execution.status.value,
                    error_type=type(exc).__name__,
                    attributes={"steps": execution.steps},
                )
            )
            raise
        execution._succeed(outputs)
        self._observations.publish(
            RuntimeEvent(
                RuntimeEventKind.EXECUTION_FINISHED,
                graph=execution.graph,
                execution_id=execution.id,
                status=execution.status.value,
                attributes={"steps": execution.steps},
            )
        )
        return execution.result(0)

    def _record_execution(self, execution: Execution) -> None:
        with self._lock:
            if execution.id in self._executions:
                raise BricksRuntimeError(f"duplicate execution {execution.id!r}")
            self._executions[execution.id] = execution
            while len(self._executions) > self._history_limit:
                for execution_id, retained in self._executions.items():
                    if retained.done:
                        del self._executions[execution_id]
                        break
                else:
                    break

    def _direct_done(self, future: Future[tuple[Output, ...]]) -> None:
        with self._lock:
            self._direct_pending.discard(future)

    def _emit_with_lease(self, lease: _SlotLease, event: Event) -> None:
        # A successful emitter call transfers this reference to the EventBus.
        lease.retain()
        forwarded = Event(
            event.type,
            event.payload,
            _slot_lease=lease,
        )
        try:
            self._emit(forwarded)
        except BaseException:
            lease.release()
            raise

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


class LocalRuntimePlugin:
    """使用标准能力协议组装单进程 Runtime 的内建插件。"""

    descriptor = PluginDescriptor(
        "bricks.core/local-runtime",
        "1.0.0",
        provides=(
            CAP_EVENT_BUS,
            CAP_TASK_BACKEND,
            CAP_GRAPH_EXECUTOR,
            CAP_EVENT_ROUTER,
            CAP_GRAPH_WORKER,
        ),
    )

    def __init__(
        self,
        *,
        events: EventBus | None = None,
        tasks: TaskPublisher | TaskConsumer | None = None,
        executor: GraphExecutor | None = None,
        close_injected: bool = False,
    ) -> None:
        if type(close_injected) is not bool:
            raise TypeError("close_injected must be a boolean")
        if tasks is not None and not (
            callable(getattr(tasks, "submit", None))
            and callable(getattr(tasks, "bind", None))
        ):
            raise TypeError("tasks must implement TaskPublisher and TaskConsumer")
        self._events = MemoryEventBus() if events is None else events
        self._tasks = MemoryTaskBackend() if tasks is None else tasks
        self._observations = ObservationHub()
        self._policies = PolicyRegistry()
        self._executor = (
            Engine(observations=self._observations) if executor is None else executor
        )
        self._owned = (
            events is None,
            tasks is None,
            executor is None,
        )
        self._close_injected = close_injected
        self.router: EventRouter | None = None
        self.worker: GraphWorker | None = None

    def setup(self, context: PluginContext) -> None:
        self.router = EventRouter(
            events=self._events,
            publisher=self._tasks,  # type: ignore[arg-type]
            observations=self._observations,
        )
        self.worker = GraphWorker(
            consumer=self._tasks,  # type: ignore[arg-type]
            executor=self._executor,
            emit=self.router.publish,
            observations=self._observations,
            policies=self._policies,
        )
        context.provide(CAP_EVENT_BUS, self._events)
        context.provide(CAP_TASK_BACKEND, self._tasks)
        context.provide(CAP_GRAPH_EXECUTOR, self._executor)
        context.provide(CAP_EVENT_ROUTER, self.router)
        context.provide(CAP_GRAPH_WORKER, self.worker)

    def start(self, context: PluginContext) -> None:
        assert self.worker is not None
        for contribution in context.contributions(CAP_INPUT_SELECTOR):
            self.worker.register_policy(contribution.name, contribution.value)
        for contribution in context.contributions(CAP_NODE_HOOK):
            value = contribution.value
            if isinstance(value, NodeHookContribution):
                self.worker.contribute_hook(
                    value.hook,
                    phase=value.phase,
                    graph=value.graph,
                    node=value.node,
                )
            else:
                self.worker.attach(value)
        for contribution in context.contributions(CAP_RUNTIME_OBSERVER):
            self._observations.attach(contribution.value)

    def stop(self, context: PluginContext) -> None:
        del context
        failure: BaseException | None = None
        if self.worker is not None:
            failure = _close_components((self.worker,), failure)
        if self.router is not None:
            failure = _close_components((self.router,), failure)
        components = tuple(
            component
            for component, owned in zip(
                (self._events, self._tasks, self._executor), self._owned
            )
            if owned or self._close_injected
        )
        failure = _close_components(reversed(_unique(*components)), failure)
        if failure is not None:
            raise failure


class Runtime:
    """组合 EventRouter 与 GraphWorker 的单进程便利门面。"""

    def __init__(
        self,
        *,
        router: EventRouter | None = None,
        worker: GraphWorker | None = None,
        plugins: Iterable[Plugin] | None = None,
    ) -> None:
        if plugins is not None and (router is not None or worker is not None):
            raise TypeError("plugins cannot be combined with router or worker")
        if (router is None) != (worker is None):
            raise TypeError("Runtime requires both router and worker")
        owned: tuple[object, ...] = ()
        host: PluginHost | None = None
        if router is None:
            selected = () if plugins is None else tuple(plugins)
            provides = {
                capability
                for plugin in selected
                for capability in plugin.descriptor.provides
            }
            role_capabilities = {CAP_EVENT_ROUTER, CAP_GRAPH_WORKER}
            if provides & role_capabilities and not role_capabilities <= provides:
                raise TypeError("plugins must provide both EventRouter and GraphWorker")
            if not role_capabilities <= provides:
                selected = (LocalRuntimePlugin(), *selected)
            host = PluginHost(selected).start()
            router = host.require(CAP_EVENT_ROUTER)
            worker = host.require(CAP_GRAPH_WORKER)
        if not isinstance(router, EventRouter):
            raise TypeError("router must be an EventRouter")
        if not isinstance(worker, GraphWorker):
            raise TypeError("worker must be a GraphWorker")
        self.router = router
        self.worker = worker
        self._owned_components = owned
        self._plugin_host = host
        self._closed = False
        self._lock = RLock()

    @property
    def plugin_host(self) -> PluginHost | None:
        """返回插件装配宿主；显式角色组合没有宿主。"""

        return self._plugin_host

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
        max_steps: int = 0,
        timeout: float | None = None,
    ) -> Runtime:
        limits = ExecutionLimits(max_steps, timeout)
        self.router.route(
            event_type,
            graph=graph,
            queue=queue,
            subscription=subscription,
            limits=limits,
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
        max_steps: int = 0,
        timeout: float | None = None,
        output_buffer: int = 64,
    ) -> tuple[Output, ...]:
        return self.worker.run(
            graph,
            inputs,
            plan=plan,
            max_steps=max_steps,
            timeout=timeout,
            output_buffer=output_buffer,
        )

    def iter(
        self,
        graph: str,
        inputs: Any = None,
        *,
        plan: ExecutionPlan | None = None,
        max_steps: int = 0,
        timeout: float | None = None,
        output_buffer: int = 64,
    ) -> Iterator[Output]:
        return self.worker.iter(
            graph,
            inputs,
            plan=plan,
            max_steps=max_steps,
            timeout=timeout,
            output_buffer=output_buffer,
        )

    def aiter(
        self,
        graph: str,
        inputs: Any = None,
        *,
        plan: ExecutionPlan | None = None,
        max_steps: int = 0,
        timeout: float | None = None,
        output_buffer: int = 64,
    ) -> AsyncIterator[Output]:
        return self.worker.aiter(
            graph,
            inputs,
            plan=plan,
            max_steps=max_steps,
            timeout=timeout,
            output_buffer=output_buffer,
        )

    def start(
        self,
        graph: str,
        inputs: Any = None,
        *,
        plan: ExecutionPlan | None = None,
        max_steps: int = 0,
        timeout: float | None = None,
        output_buffer: int = 64,
    ) -> Execution:
        return self.worker.start(
            graph,
            inputs,
            plan=plan,
            max_steps=max_steps,
            timeout=timeout,
            output_buffer=output_buffer,
        )

    async def arun(
        self,
        graph: str,
        inputs: Any = None,
        *,
        plan: ExecutionPlan | None = None,
        max_steps: int = 0,
        timeout: float | None = None,
        output_buffer: int = 64,
    ) -> tuple[Output, ...]:
        return await self.worker.arun(
            graph,
            inputs,
            plan=plan,
            max_steps=max_steps,
            timeout=timeout,
            output_buffer=output_buffer,
        )

    def get_execution(self, execution_id: str) -> Execution:
        return self.worker.get_execution(execution_id)

    def executions(self) -> tuple[Execution, ...]:
        return self.worker.executions()

    def attach(
        self,
        hook: NodeHook | Callable[..., object],
        *,
        phase: HookPhase | str | None = None,
        graph: str | None = None,
        node: str | None = None,
    ) -> HookHandle:
        return self.worker.attach(hook, phase=phase, graph=graph, node=node)

    def register_policy(self, name: str, selector: InputSelector) -> Runtime:
        self.worker.register_policy(name, selector)
        return self

    def observe_runtime(self, observer: RuntimeObserver) -> CompositeObserverHandle:
        """订阅只读执行、Node、Event 和 Work 生命周期事件。"""

        hubs = {id(self.router._observations): self.router._observations}
        hubs[id(self.worker._observations)] = self.worker._observations
        return CompositeObserverHandle(tuple(hub.attach(observer) for hub in hubs.values()))

    def on(
        self,
        event_type: str,
        *,
        graph: str,
        queue: str,
        concurrency: int = 1,
        slots: SlotPool | None = None,
        subscription: str | None = None,
        max_steps: int = 0,
        timeout: float | None = None,
    ) -> Runtime:
        """组合注册 Event route，并启动对应 queue 的本地消费者。"""

        # Expose the route only after its local consumer is ready.
        self.consume(queue, concurrency=concurrency, slots=slots)
        return self.route(
            event_type,
            graph=graph,
            queue=queue,
            subscription=subscription,
            max_steps=max_steps,
            timeout=timeout,
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
        if self._plugin_host is None:
            failure = _close_components((self.worker, self.router), failure)
            failure = _close_components(reversed(self._owned_components), failure)
        else:
            failure = _close_components((self._plugin_host,), failure)
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
    del event
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
