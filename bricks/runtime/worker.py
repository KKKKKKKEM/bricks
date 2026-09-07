"""Graph 注册、Work 消费与 execution 管理。"""

from __future__ import annotations

import time
import inspect
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
from ..engine.runner import LocalRunner
from ..engine.slots import Slot, SlotPool
from ..spi import (
    Delivery,
    DeliveryResult,
    Emit,
    ExecutionFactory,
    GraphExecutor,
    HookableGraphExecutor,
    SlotLease,
    SlotProvider,
    TaskConsumer,
    Work,
)
from ._utils import _close_components, _remaining, _unique


class GraphWorker:
    """注册 Graph、消费队列 Work，并执行完整 Graph。

    Attributes:
        _consumer: 命名通道的工作消费能力。
        _executor: 执行 Graph 的实现。
        _execution_factory: 创建可替换 Execution 句柄的工厂。
        _executor_runner: 等待自定义异步执行器的调用运行器。
        _emit: 事件发布回调。
        _emit_local: 能够延续当前进程 Slot 链的发布回调。
        _owned_components: 当前组件负责关闭的底层资源集合。
        _close_injected: 是否负责关闭调用方注入的底层组件。
        _observations: 当前组件的只读生命周期事件分发中心。
        _policies: 当前角色使用的输入策略注册能力。
        _graphs: 已冻结并注册的 Graph 定义。
        _queues: 当前 Worker 已绑定的消费通道。
        _owned_slot_pools: 由当前组件创建并负责关闭的 Slot 池。
        _lock: 保护当前组件共享状态的进程内互斥锁。
        _direct_executor: 承载直接执行任务的线程池。
        _direct_pending: 已经直接提交但尚未完成的执行任务。
        _executions: 按执行 ID 保存的 Execution 历史。
        _pending_hooks: 等待挂载到可替换执行器的 Hook 注册。
        _history_limit: 终态执行记录保留数量上限。
        _closed: 当前组件是否已停止接受新工作。
    """

    def __init__(
        self,
        *,
        consumer: TaskConsumer,
        executor: GraphExecutor | None = None,
        execution_factory: ExecutionFactory | None = None,
        emit: Emit | None = None,
        emit_local: Callable[[Event, SlotLease], None] | None = None,
        close_injected: bool = False,
        observations: ObservationHub | None = None,
        policies: PolicyRegistry | None = None,
    ) -> None:
        """装配任务消费、Graph 执行、输出工厂和本地执行记录。

        Args:
            consumer: 本次消费对应的消费者记录或消费能力。
            executor: 实际执行任务或 Graph 的实现。
            execution_factory: 创建独立 Execution 句柄的工厂能力。
            emit: 发布跨图事件的回调。
            emit_local: 能够延续本地 Slot 链的事件发布回调。
            close_injected: 是否由当前组件关闭注入的底层资源。
            observations: 负责分发生命周期事件的观察中心。
            policies: 注册并绑定输入选择策略的容器。

        Raises:
            TypeError: 参数类型或接口实现不符合当前契约。
        """

        if type(close_injected) is not bool:
            raise TypeError("close_injected must be a boolean")
        if execution_factory is not None and not callable(execution_factory):
            raise TypeError("execution_factory must be callable")
        owned: list[object] = []
        if observations is None:
            observations = ObservationHub()
        if executor is None:
            executor = Engine(observations=observations)
            owned.append(executor)
        self._consumer = consumer
        if not isinstance(executor, GraphExecutor):
            raise TypeError("executor must implement GraphExecutor")
        self._executor = executor
        self._execution_factory = (
            Execution if execution_factory is None else execution_factory
        )
        self._executor_runner: LocalRunner | None = None
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
        self._queues: dict[str, tuple[int, SlotProvider]] = {}
        self._owned_slot_pools: list[SlotPool] = []
        self._lock = RLock()
        self._direct_executor = ThreadPoolExecutor(
            thread_name_prefix="bricks-direct",
        )
        self._direct_pending: set[Future[None]] = set()
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
        """返回当前 Worker 是否没有正在执行或排队的 Work。

        Returns:
            没有未完成工作时为 True，否则为 False。
        """

        with self._lock:
            return (
                self._consumer.idle
                and not self._direct_pending
                and all(execution.done for execution in self._executions.values())
            )

    def register(self, name: str, graph: Graph) -> GraphWorker:
        """以稳定名称注册并冻结一张 Graph。

        Args:
            name: 注册或查找使用的名称。
            graph: 目标 Graph 定义或其注册名称，以类型声明为准。

        Returns:
            当前实例，可继续进行链式组合。

        Raises:
            TypeError: 参数类型或接口实现不符合当前契约。
            ValueError: 参数值或字段组合不合法。
        """

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
        slots: SlotProvider | None = None,
    ) -> GraphWorker:
        """消费队列；concurrency 控制本地执行，slots 控制逻辑执行链。

        Args:
            queue: 命名消费通道。
            concurrency: 当前消费者允许并行执行的完整 Graph 数量。
            slots: 提供本地执行槽的资源池能力。

        Returns:
            本次操作得到的 GraphWorker 实例。

        Raises:
            TypeError: 参数类型或接口实现不符合当前契约。
            ValueError: 参数值或字段组合不合法。
        """

        self._ensure_open()
        queue = require_non_empty_string(queue, "task queue")
        if type(concurrency) is not int:
            raise TypeError("queue concurrency must be an integer")
        if concurrency < 1:
            raise ValueError("queue concurrency must be at least 1")
        if slots is not None and not isinstance(slots, SlotProvider):
            raise TypeError("slots must implement SlotProvider or be None")
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
        """同步直接执行一个已注册 Graph。

        Args:
            graph: 目标 Graph 定义或其注册名称，以类型声明为准。
            inputs: 入口数据或按端口名称组织的输入映射。
            plan: 限定本次执行范围的计划，None 使用完整 Graph。
            max_steps: 节点触发次数上限，0 表示不限制。
            timeout: 等待或执行时限，单位秒；None 表示不设置时限。
            output_buffer: 每个活跃输出流允许积压的输出条数。

        Returns:
            符合声明端口契约的 Output 集合。
        """

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
        """在后台启动 Graph，并立即返回可等待和取消的 Execution。

        Args:
            graph: 目标 Graph 定义或其注册名称，以类型声明为准。
            inputs: 入口数据或按端口名称组织的输入映射。
            plan: 限定本次执行范围的计划，None 使用完整 Graph。
            max_steps: 节点触发次数上限，0 表示不限制。
            timeout: 等待或执行时限，单位秒；None 表示不设置时限。
            output_buffer: 每个活跃输出流允许积压的输出条数。

        Returns:
            本次操作得到的 Execution 实例。
        """

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
        """提交直接执行任务，并将其纳入空闲等待范围。

        Args:
            execution: 记录当前执行状态、控制限制及输出的句柄。
            inputs: 入口数据或按端口名称组织的输入映射。
            plan: 限定本次执行范围的计划，None 使用完整 Graph。
        """

        with self._lock:
            try:
                future = self._direct_executor.submit(
                    self._execute_direct,
                    execution,
                    inputs,
                    plan,
                )
            except BaseException as exc:
                execution.fail(exc)
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
        """在线程中直接执行 Graph，避免阻塞异步调用方。

        Args:
            graph: 目标 Graph 定义或其注册名称，以类型声明为准。
            inputs: 入口数据或按端口名称组织的输入映射。
            plan: 限定本次执行范围的计划，None 使用完整 Graph。
            max_steps: 节点触发次数上限，0 表示不限制。
            timeout: 等待或执行时限，单位秒；None 表示不设置时限。
            output_buffer: 每个活跃输出流允许积压的输出条数。

        Returns:
            符合声明端口契约的 Output 集合。
        """

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
        """启动 Graph 并同步迭代 terminal Output。

        Args:
            graph: 目标 Graph 定义或其注册名称，以类型声明为准。
            inputs: 入口数据或按端口名称组织的输入映射。
            plan: 限定本次执行范围的计划，None 使用完整 Graph。
            max_steps: 节点触发次数上限，0 表示不限制。
            timeout: 等待或执行时限，单位秒；None 表示不设置时限。
            output_buffer: 每个活跃输出流允许积压的输出条数。

        Returns:
            按产生顺序交付终端 Output 的迭代入口。
        """

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
        """启动 Graph 并异步迭代 terminal Output。

        Args:
            graph: 目标 Graph 定义或其注册名称，以类型声明为准。
            inputs: 入口数据或按端口名称组织的输入映射。
            plan: 限定本次执行范围的计划，None 使用完整 Graph。
            max_steps: 节点触发次数上限，0 表示不限制。
            timeout: 等待或执行时限，单位秒；None 表示不设置时限。
            output_buffer: 每个活跃输出流允许积压的输出条数。

        Returns:
            按产生顺序交付终端 Output 的迭代入口。
        """

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
        """按 ID 返回当前进程保留的 Execution。

        Args:
            execution_id: 已登记执行记录的唯一标识。

        Returns:
            本次操作得到的 Execution 实例。
        """

        execution_id = require_non_empty_string(execution_id, "execution id")
        with self._lock:
            try:
                return self._executions[execution_id]
            except KeyError as exc:
                raise BricksRuntimeError(f"unknown execution {execution_id!r}") from exc

    def executions(self) -> tuple[Execution, ...]:
        """返回当前进程保留的 Execution 快照。

        Returns:
            当前保存的 Execution 句柄快照。
        """

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
        """动态挂载 Node Hook；变更从下一次 Graph execution 生效。

        Args:
            hook: 节点 Hook 对象或单阶段回调。
            phase: 函数 Hook 对应的执行阶段。
            graph: 目标 Graph 定义或其注册名称，以类型声明为准。
            node: 节点实例或作用域中的节点 ID，以接口类型为准。

        Returns:
            用于卸载本次注册的句柄。

        Raises:
            ValueError: 参数值或字段组合不合法。
        """

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
        """安装插件 Hook；目标 Graph 尚未注册时延迟绑定。

        Args:
            hook: 节点 Hook 对象或单阶段回调。
            phase: 函数 Hook 对应的执行阶段。
            graph: 目标 Graph 定义或其注册名称，以类型声明为准。
            node: 节点实例或作用域中的节点 ID，以接口类型为准。

        Returns:
            用于卸载本次注册的句柄。

        Raises:
            ValueError: 参数值或字段组合不合法。
        """

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
        """将挂起的 Hook 挂载到支持该能力的执行器。

        Args:
            hook: 节点 Hook 对象或单阶段回调。
            phase: 函数 Hook 对应的执行阶段。
            graph: 目标 Graph 定义或其注册名称，以类型声明为准。
            node: 节点实例或作用域中的节点 ID，以接口类型为准。

        Returns:
            用于卸载本次注册的句柄。

        Raises:
            TypeError: 参数类型或接口实现不符合当前契约。
        """

        if not isinstance(self._executor, HookableGraphExecutor):
            raise TypeError("the configured GraphExecutor does not support hooks")
        return self._executor.attach(hook, phase=phase, graph=graph, node=node)

    def register_policy(self, name: str, selector: InputSelector) -> GraphWorker:
        """注册 selector contribution；使用它的 Graph 必须尚未冻结。

        Args:
            name: 注册或查找使用的名称。
            selector: 仅依据端口和 token 数量选择输入的实现。

        Returns:
            当前实例，可继续进行链式组合。
        """

        self._ensure_open()
        self._policies.register(name, selector)
        return self

    def observe_runtime(self, observer: RuntimeObserver) -> ObserverHandle:
        """注册只读运行时生命周期观察者。

        Args:
            observer: 接收只读生命周期事件的观察者。

        Returns:
            用于卸载本次注册的句柄。
        """

        return self._observations.attach(observer)

    def wait_idle(self, timeout: float | None = None) -> None:
        """等待当前 Worker 已接受的 Work 完成。

        Args:
            timeout: 等待或执行时限，单位秒；None 表示不设置时限。

        Raises:
            TimeoutError: 等待未在指定时限内完成。
        """

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
        if self._executor_runner is not None:
            failure = _close_components((self._executor_runner,), failure)
        failure = _close_components(reversed(self._owned_slot_pools), failure)
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

    def _execute_delivery(self, delivery: Delivery) -> DeliveryResult:
        """执行可确认交付并返回明确的 backend 决定。

        Args:
            delivery: 携带尝试次数和可选 Slot lease 的本次投递。

        Returns:
            包含交付决定和可选异常的 DeliveryResult。
        """

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
        """消费投递并将执行结果转换为后端可识别的交付决定。

        Args:
            work: 需要投递或执行的工作请求。
            lease: 当前进程内执行槽的引用与串行执行能力。
        """

        execution = self._make_execution(
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
            execution.fail(exc)
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
        """为一次工作创建并登记新的 Execution。

        Args:
            graph: 目标 Graph 定义或其注册名称，以类型声明为准。
            max_steps: 节点触发次数上限，0 表示不限制。
            timeout: 等待或执行时限，单位秒；None 表示不设置时限。
            output_buffer: 每个活跃输出流允许积压的输出条数。

        Returns:
            本次操作得到的 Execution 实例。
        """

        self._ensure_open()
        self._get_graph(graph)
        execution = self._make_execution(
            graph,
            limits=ExecutionLimits(max_steps, timeout),
            output_buffer=output_buffer,
        )
        self._record_execution(execution)
        return execution

    def _make_execution(
        self,
        graph: str,
        *,
        limits: ExecutionLimits,
        id: str | None = None,
        output_buffer: int = 64,
    ) -> Execution:
        """通过执行工厂创建句柄并校验其限制与调用参数一致。

        Args:
            graph: 目标 Graph 定义或其注册名称，以类型声明为准。
            limits: 本次执行独立使用的步数和时长限制。
            id: 对象标识，允许缺省时由实现生成。
            output_buffer: 每个活跃输出流允许积压的输出条数。

        Returns:
            本次操作得到的 Execution 实例。

        Raises:
            TypeError: 参数类型或接口实现不符合当前契约。
            ValueError: 参数值或字段组合不合法。
        """

        execution = self._execution_factory(
            graph,
            limits=limits,
            id=id,
            output_buffer=output_buffer,
        )
        if not isinstance(execution, Execution):
            raise TypeError("execution_factory must return Execution")
        if (
            execution.graph != graph
            or execution.limits != limits
            or execution.output_buffer != output_buffer
            or (id is not None and execution.id != id)
            or execution.status is not ExecutionStatus.PENDING
        ):
            raise ValueError("execution_factory changed the execution contract")
        return execution

    def _execute_direct(
        self,
        execution: Execution,
        inputs: Any,
        plan: ExecutionPlan | None,
    ) -> None:
        """执行直接提交的 Graph，并完成执行记录的终态处理。

        Args:
            execution: 记录当前执行状态、控制限制及输出的句柄。
            inputs: 入口数据或按端口名称组织的输入映射。
            plan: 限定本次执行范围的计划，None 使用完整 Graph。
        """

        graph = self._get_graph(execution.graph)
        self._execute(
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
    ) -> None:
        """在执行宿主的控制边界内调用 Graph 执行器。

        Args:
            execution: 记录当前执行状态、控制限制及输出的句柄。
            graph: 目标 Graph 定义或其注册名称，以类型声明为准。
            inputs: 入口数据或按端口名称组织的输入映射。
            emit: 发布跨图事件的回调。
            plan: 限定本次执行范围的计划，None 使用完整 Graph。
            slot: 当前逻辑执行链使用的本地执行槽。

        Raises:
            TypeError: 参数类型或接口实现不符合当前契约。
            ValueError: 参数值或字段组合不合法。
        """

        if not execution.start(graph):
            return
        self._observations.publish(
            RuntimeEvent(
                RuntimeEventKind.EXECUTION_STARTED,
                graph=execution.graph,
                execution_id=execution.id,
            )
        )
        try:
            if plan is not None:
                if not isinstance(plan, ExecutionPlan):
                    raise TypeError("plan must be an ExecutionPlan")
                if plan.graph is not graph:
                    raise ValueError("execution plan belongs to a different Graph")
            result = self._executor.execute(
                execution.graph,
                graph,
                inputs,
                emit,
                plan=plan,
                slot=slot,
                execution=execution,
            )
            if inspect.isawaitable(result):
                with self._lock:
                    if self._executor_runner is None:
                        self._executor_runner = LocalRunner()
                    runner = self._executor_runner
                result = runner.resolve(
                    result,
                    checkpoint=execution.checkpoint,
                    wait_timeout=execution.wait_timeout,
                )
            if result is not None:
                raise TypeError("GraphExecutor must publish outputs and return None")
            execution.succeed()
        except BaseException as exc:
            execution.fail(exc)
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
        self._observations.publish(
            RuntimeEvent(
                RuntimeEventKind.EXECUTION_FINISHED,
                graph=execution.graph,
                execution_id=execution.id,
                status=execution.status.value,
                attributes={"steps": execution.steps},
            )
        )

    def _record_execution(self, execution: Execution) -> None:
        """保存执行记录，并按历史保留上限清理已结束记录。

        Args:
            execution: 记录当前执行状态、控制限制及输出的句柄。
        """

        with self._lock:
            if execution.id in self._executions:
                raise BricksRuntimeError(f"duplicate execution {execution.id!r}")
            self._executions[execution.id] = execution
            self._trim_execution_history_locked()

    def _direct_done(self, future: Future[None]) -> None:
        """完成直接执行的收尾，并清理待完成任务记录。

        Args:
            future: 线程池或事件循环提交返回的结果句柄。
        """

        with self._lock:
            self._direct_pending.discard(future)
            self._trim_execution_history_locked()

    def _trim_execution_history_locked(self) -> None:
        """持锁清理超过历史上限的终态执行，保留仍在运行的记录。"""

        while len(self._executions) > self._history_limit:
            for execution_id, retained in self._executions.items():
                if retained.done:
                    del self._executions[execution_id]
                    break
            else:
                break

    def _emit_with_lease(self, lease: SlotLease, event: Event) -> None:
        """为跨图事件保留本地 Slot 引用，并在提交失败时归还引用。

        Args:
            lease: 当前进程内执行槽的引用与串行执行能力。
            event: 需要发布、观察或处理的事件。
        """

        if self._emit_local is None:
            self._emit(event)
            return
        self._emit_local(event, lease)

    def _get_graph(self, name: str) -> Graph:
        """查找已注册 Graph，名称不存在时明确失败。

        Args:
            name: 注册或查找使用的名称。

        Returns:
            本次操作得到的 Graph 实例。

        Raises:
            UnknownGraphError: 指定的 Graph 未注册。
        """

        name = require_non_empty_string(name, "registered graph name")
        with self._lock:
            try:
                return self._graphs[name]
            except KeyError as exc:
                raise UnknownGraphError(f"unknown registered graph {name!r}") from exc

    def _ensure_open(self) -> None:
        """拒绝对已经关闭的组件继续提交工作。

        Raises:
            RuntimeClosedError: 当前运行时角色已经关闭。
        """

        with self._lock:
            if self._closed:
                raise RuntimeClosedError("GraphWorker is closed")


def _reject_emit(event: Event) -> None:
    """拒绝没有绑定事件发布能力的跨图事件调用。

    Args:
        event: 需要发布、观察或处理的事件。
    """

    del event
    raise BricksRuntimeError("GraphWorker has no Event emitter")
