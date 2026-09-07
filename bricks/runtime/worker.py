"""Graph 注册、Work 消费与 execution 管理。"""

from __future__ import annotations

import time
from collections import OrderedDict
from collections.abc import AsyncIterator, Callable, Iterator
from concurrent.futures import Future, ThreadPoolExecutor, wait
from functools import partial
from threading import RLock
from typing import Any

from ..engine.core import Output, _validate_timeout, require_non_empty_string
from ..engine.errors import (
    BricksRuntimeError,
    ExecutionError,
    RuntimeClosedError,
    UnknownGraphError,
)
from ..engine.events import Event
from ..engine.execution import Execution, ExecutionLimits, ExecutionStatus
from ..engine.executor import Engine
from ..engine.graph import ExecutionPlan, Graph
from ..engine.hooks import HookHandle, HookPhase, NodeHook
from ..engine.observation import (
    ObservationHub,
    ObserverHandle,
    RuntimeEvent,
    RuntimeEventKind,
    RuntimeObserver,
)
from ..engine.policies import InputSelector, PolicyRegistry
from ..engine.slots import Slot, SlotPool
from ..spi import (
    Delivery,
    DeliveryResult,
    Emit,
    GraphExecutor,
    HookableGraphExecutor,
    SlotLease,
    TaskConsumer,
    Work,
)
from ._utils import _close_components, _remaining, _unique


class GraphWorker:
    """注册 Graph、消费队列 Work，并执行完整 Graph。"""

    def __init__(
        self,
        *,
        consumer: TaskConsumer,
        executor: GraphExecutor | None = None,
        emit: Emit | None = None,
        emit_local: Callable[[Event, SlotLease], None] | None = None,
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
        self._emit_local = emit_local
        if self._emit_local is not None and not callable(self._emit_local):
            raise TypeError("worker local emitter must be callable or None")
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
            list[
                tuple[
                    NodeHook | Callable[..., object], HookPhase | str | None, str | None
                ]
            ],
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
                raise BricksRuntimeError(f"unknown execution {execution_id!r}") from exc

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
        return self._attach_executor_hook(hook, phase=phase, graph=graph, node=node)

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
        return self._executor.attach(hook, phase=phase, graph=graph, node=node)

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
            self._execute_work(work, delivery.slot_lease)
        except BaseException as exc:  # noqa: BLE001
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

    def _execute_work(self, work: Work, lease: SlotLease | None) -> None:
        execution = Execution(
            work.graph,
            limits=work.limits,
            id=work.id,
        )
        self._record_execution(execution)
        try:
            graph = self._get_graph(work.graph)
            if lease is None:
                raise BricksRuntimeError("TaskConsumer dispatched Work without a Slot")
            with lease.execution() as slot:
                emit = partial(self._emit_with_lease, lease)
                self._execute(
                    execution,
                    graph,
                    work.inputs,
                    emit,
                    slot=slot,
                )
        except BaseException as exc:
            if execution.status is ExecutionStatus.PENDING:
                execution._start()
            execution._fail(exc)
            if isinstance(exc, ExecutionError) and exc.event is None:
                exc.event = work.trigger
            raise
        finally:
            with self._lock:
                self._trim_execution_history_locked()

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
            self._trim_execution_history_locked()

    def _direct_done(self, future: Future[tuple[Output, ...]]) -> None:
        with self._lock:
            self._direct_pending.discard(future)
            self._trim_execution_history_locked()

    def _trim_execution_history_locked(self) -> None:
        while len(self._executions) > self._history_limit:
            for execution_id, retained in self._executions.items():
                if retained.done:
                    del self._executions[execution_id]
                    break
            else:
                break

    def _emit_with_lease(self, lease: SlotLease, event: Event) -> None:
        if self._emit_local is None:
            self._emit(event)
            return
        self._emit_local(event, lease)

    def _get_graph(self, name: str) -> Graph:
        name = require_non_empty_string(name, "registered graph name")
        with self._lock:
            try:
                return self._graphs[name]
            except KeyError as exc:
                raise UnknownGraphError(f"unknown registered graph {name!r}") from exc

    def _ensure_open(self) -> None:
        with self._lock:
            if self._closed:
                raise RuntimeClosedError("GraphWorker is closed")


def _reject_emit(event: Event) -> None:
    del event
    raise BricksRuntimeError("GraphWorker has no Event emitter")
