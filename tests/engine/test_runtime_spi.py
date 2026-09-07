"""Runtime 可替换能力端口的组合契约测试。"""

from __future__ import annotations

import pickle
from collections import defaultdict
from collections.abc import Callable
from threading import Event as ThreadEvent
from threading import Thread
from typing import Any, cast
from uuid import UUID

import pytest

from bricks import Event, Graph, InputPolicy, Node, Ports, Runtime
from bricks.adapters import memory
from bricks.engine.hooks import HookRegistry
from bricks.engine.slots import SlotPool
from bricks.runtime import EventRouter, GraphWorker
from bricks.spi import (
    Delivery,
    DeliveryOutcome,
    DeliveryResult,
    EventHandler,
    Work,
    WorkHandler,
    SlotProvider,
)


class EmptyNode(Node):
    """构造测试注册所需的最小 Graph。

    Attributes:
        input_ports: 节点声明的输入端口及其类型。
        output_ports: 节点声明的输出端口及其类型。
        input_policy: 仅依据端口和 token 数量生效的输入策略。
    """

    input_ports = Ports()
    output_ports = Ports()
    input_policy = InputPolicy.ON_START

    def execute(self, inputs, context) -> None:
        """执行当前测试场景的节点行为，供外层契约断言检查。

        Args:
            inputs: 入口数据或按端口名称组织的输入映射。
            context: 当前调用的执行或插件上下文。
        """

        del inputs, context


class RecordingBus:
    """证明 Runtime 不依赖默认内存 EventBus 的同步替身。

    Attributes:
        handlers: 测试后端登记的处理函数集合。
        events: 测试记录的事件或注入的事件总线。
        closed: 测试组件是否已经关闭。
    """

    def __init__(self) -> None:
        """初始化实例及其依赖，建立当前对象独立维护的状态。"""

        self.handlers: dict[str, list[EventHandler]] = defaultdict(list)
        self.events: list[Event] = []
        self.closed = False

    @property
    def idle(self) -> bool:
        """判断当前组件是否没有尚未完成的工作。

        Returns:
            没有未完成工作时为 True，否则为 False。
        """

        return True

    def subscribe(
        self,
        event_type: str,
        handler: EventHandler,
        *,
        subscription: str | None = None,
    ) -> None:
        """注册事件订阅，同名订阅组中的处理器竞争消费。

        Args:
            event_type: 用于订阅或路由匹配的事件类型。
            handler: 接收事件或投递的处理函数。
            subscription: 竞争消费组名称，None 创建独立订阅。
        """

        del subscription
        self.handlers[event_type].append(handler)

    def publish(self, event: Event) -> None:
        """发布事件并推进对应的投递或观察流程。

        Args:
            event: 需要发布、观察或处理的事件。
        """

        transported = pickle.loads(pickle.dumps(event))
        self.events.append(transported)
        handlers = tuple(self.handlers[transported.type]) + tuple(self.handlers["*"])
        for handler in handlers:
            handler(transported)

    def wait_idle(self, timeout: float | None = None) -> None:
        """等待已接受的工作完成，并传播已记录的失败。

        Args:
            timeout: 等待或执行时限，单位秒；None 表示不设置时限。
        """

        del timeout

    def close(self) -> None:
        """结束当前组件的生命周期并释放其拥有的资源。"""

        self.closed = True


class RecordingTasks:
    """同步消费 Work，模拟可由 Redis/MQ 替换的任务后端。

    Attributes:
        handlers: 测试后端登记的处理函数集合。
        submitted: 测试后端记录的已提交工作。
        closed: 测试组件是否已经关闭。
    """

    def __init__(self) -> None:
        """初始化实例及其依赖，建立当前对象独立维护的状态。"""

        self.handlers: dict[str, Callable[[Work], None]] = {}
        self.submitted: list[tuple[str, Work]] = []
        self.closed = False

    @property
    def idle(self) -> bool:
        """判断当前组件是否没有尚未完成的工作。

        Returns:
            没有未完成工作时为 True，否则为 False。
        """

        return True

    def bind(
        self,
        queue: str,
        handler: WorkHandler,
        *,
        concurrency: int,
        slots: SlotProvider | None = None,
    ) -> None:
        """绑定消费通道及其处理器和本地并发配置。

        Args:
            queue: 命名消费通道。
            handler: 接收事件或投递的处理函数。
            concurrency: 当前消费者允许并行执行的完整 Graph 数量。
            slots: 提供本地执行槽的资源池能力。
        """

        if slots is None:
            slots = SlotPool(concurrency)

        def handle(work: Work) -> None:
            """处理测试投递，并返回明确的交付结果。

            Args:
                work: 需要投递或执行的工作请求。
            """

            transported = pickle.loads(pickle.dumps(work))
            lease = slots.acquire()
            try:
                result = handler(Delivery(transported, slot_lease=lease))
                if result.outcome is not DeliveryOutcome.ACK:
                    raise result.error or RuntimeError(
                        f"work handler returned {result.outcome.value}"
                    )
            finally:
                lease.release()

        self.handlers[queue] = handle

    def submit(self, queue: str, work: Work) -> None:
        """向命名执行通道提交工作。

        Args:
            queue: 命名消费通道。
            work: 需要投递或执行的工作请求。
        """

        self.submitted.append((queue, work))
        self.handlers[queue](work)

    def wait_idle(self, timeout: float | None = None) -> None:
        """等待已接受的工作完成，并传播已记录的失败。

        Args:
            timeout: 等待或执行时限，单位秒；None 表示不设置时限。
        """

        del timeout

    def close(self) -> None:
        """结束当前组件的生命周期并释放其拥有的资源。"""

        self.closed = True


class RecordingExecutor:
    """证明 Graph 执行器也可以独立替换。

    Attributes:
        calls: 按顺序记录的测试调用信息。
        closed: 测试组件是否已经关闭。
    """

    def __init__(self) -> None:
        """初始化实例及其依赖，建立当前对象独立维护的状态。"""

        self.calls: list[tuple[str, object]] = []
        self.closed = False

    def execute(
        self,
        name: str,
        graph: Graph,
        inputs: object,
        emit: Callable[[Event], None],
        plan=None,
        *,
        slot=None,
        execution,
    ) -> None:
        """执行当前测试 Graph，并通过 Execution 的公开接口交付输出。

        Args:
            name: 注册或查找使用的名称。
            graph: 目标 Graph 定义或其注册名称，以类型声明为准。
            inputs: 入口数据或按端口名称组织的输入映射。
            emit: 发布跨图事件的回调。
            plan: 限定本次执行范围的计划，None 使用完整 Graph。
            slot: 当前逻辑执行链使用的本地执行槽。
            execution: 记录当前执行状态、控制限制及输出的句柄。
        """

        del emit, plan, slot
        with execution.step(graph.entrypoint):
            pass
        self.calls.append((name, inputs))

    def close(self) -> None:
        """结束当前组件的生命周期并释放其拥有的资源。"""

        self.closed = True


class HookableRecordingExecutor(RecordingExecutor):
    """不继承 Engine，但显式提供可选 Hook 能力。

    Attributes:
        hooks: 节点 Hook 的注册与快照能力。
    """

    def __init__(self) -> None:
        """初始化实例及其依赖，建立当前对象独立维护的状态。"""

        super().__init__()
        self.hooks = HookRegistry()

    def attach(self, hook, *, phase=None, graph=None, node=None):
        """注册扩展回调并返回可卸载的句柄。

        Args:
            hook: 节点 Hook 对象或单阶段回调。
            phase: 函数 Hook 对应的执行阶段。
            graph: 目标 Graph 定义或其注册名称，以类型声明为准。
            node: 节点实例或作用域中的节点 ID，以接口类型为准。

        Returns:
            当前测试回调收集或构造的结果。
        """

        return self.hooks.attach(
            hook,
            phase=phase,
            graph=graph,
            node=node,
        )

    def close(self) -> None:
        """结束当前组件的生命周期并释放其拥有的资源。"""

        self.hooks.close()
        super().close()


def test_runtime_composes_replaceable_capabilities() -> None:
    """替换传输、任务和执行器不改变 Runtime/Graph 使用 API。"""

    bus = RecordingBus()
    tasks = RecordingTasks()
    executor = RecordingExecutor()
    graph = Graph(entrypoint="empty").add("empty", EmptyNode())

    router = EventRouter(events=bus, publisher=tasks)
    worker = GraphWorker(consumer=tasks, executor=executor, emit=router.publish)
    with Runtime(router=router, worker=worker) as runtime:
        runtime.register("work.graph", graph)
        runtime.route(
            "work.created",
            graph="work.graph",
            queue="work-tasks",
        )
        runtime.consume("work-tasks", concurrency=8)
        runtime.emit("work.created", {"id": 1})
        runtime.wait_idle()

    assert bus.events == [Event("work.created", {"id": 1})]
    assert len(tasks.submitted) == 1
    queue, work = tasks.submitted[0]
    assert queue == "work-tasks"
    assert work.graph == "work.graph"
    assert work.inputs == {"id": 1}
    assert work.id
    assert tasks.submitted[0][1].trigger == Event("work.created", {"id": 1})
    assert executor.calls == [("work.graph", {"id": 1})]
    assert not bus.closed and not tasks.closed and not executor.closed


def test_custom_executor_can_explicitly_support_runtime_hooks() -> None:
    """Hook 是结构化可选能力，不要求自定义执行器继承 Engine。"""

    tasks = RecordingTasks()
    executor = HookableRecordingExecutor()
    worker = GraphWorker(consumer=tasks, executor=executor)
    worker.register("work.graph", Graph(entrypoint="empty").add(empty=EmptyNode()))

    handle = worker.attach(lambda call: call, graph="work.graph", node="empty")

    assert executor.hooks.snapshot("work.graph")
    handle.detach()
    assert executor.hooks.snapshot("work.graph") == ()
    worker.close()


def test_custom_executor_without_hook_capability_is_rejected() -> None:
    """普通 GraphExecutor 仍可执行，只在请求可选 Hook 能力时失败。"""

    tasks = RecordingTasks()
    worker = GraphWorker(consumer=tasks, executor=RecordingExecutor())
    worker.register("work.graph", Graph(entrypoint="empty").add(empty=EmptyNode()))

    with pytest.raises(TypeError, match="does not support hooks"):
        worker.attach(lambda call: call, graph="work.graph")

    worker.close()


def test_runtime_explicitly_exposes_composed_roles() -> None:
    """Runtime 保存并代理调用方显式传入的 Router 与 Worker。"""

    bus = RecordingBus()
    tasks = RecordingTasks()
    executor = RecordingExecutor()
    router = EventRouter(events=bus, publisher=tasks)
    worker = GraphWorker(consumer=tasks, executor=executor, emit=router.publish)

    runtime = Runtime(router=router, worker=worker)

    assert runtime.router is router
    assert runtime.worker is worker
    runtime.close()


def test_runtime_rejects_partial_role_composition() -> None:
    """组合根不接受缺少 Router 或 Worker 的残缺配置。"""

    tasks = memory.TaskBackend()
    router = EventRouter(publisher=tasks)
    with pytest.raises(TypeError, match="both router and worker"):
        Runtime(router=router)
    router.close()
    tasks.close()


def test_roles_can_close_injected_components() -> None:
    """角色可以显式接管注入组件的生命周期。"""

    bus = RecordingBus()
    publisher = RecordingTasks()
    consumer = RecordingTasks()
    executor = RecordingExecutor()
    router = EventRouter(
        events=bus,
        publisher=publisher,
        close_injected=True,
    )
    worker = GraphWorker(
        consumer=consumer,
        executor=executor,
        emit=router.publish,
        close_injected=True,
    )
    runtime = Runtime(router=router, worker=worker)

    runtime.close()

    assert bus.closed and publisher.closed and consumer.closed and executor.closed


def test_work_generates_id_and_accepts_explicit_id() -> None:
    """Work 默认生成 ID，也允许传输适配器恢复已有 ID。"""

    generated = Work("graph")
    restored = Work("graph", id="work-42")

    assert str(UUID(generated.id)) == generated.id
    assert restored.id == "work-42"


def test_named_event_subscriptions_compete_and_distinct_ones_broadcast() -> None:
    """同名订阅轮流消费，不同订阅各自收到一份 Event。"""

    bus = memory.EventBus()
    first: list[int] = []
    second: list[int] = []
    audit: list[int] = []
    bus.subscribe(
        "value", lambda event: first.append(event.payload), subscription="work"
    )
    bus.subscribe(
        "value", lambda event: second.append(event.payload), subscription="work"
    )
    bus.subscribe(
        "value", lambda event: audit.append(event.payload), subscription="audit"
    )

    for value in range(4):
        bus.publish(Event("value", value))

    assert first == [0, 2]
    assert second == [1, 3]
    assert audit == [0, 1, 2, 3]


def test_router_and_worker_can_use_separate_runtime_roles() -> None:
    """Router 无需注册 Graph，Worker 无需订阅 Event。"""

    bus = memory.EventBus()
    tasks = memory.TaskBackend()
    executor = RecordingExecutor()
    router = EventRouter(events=bus, publisher=tasks)
    router.route("work.created", graph="work.graph", queue="work")
    router.emit("work.created", {"id": 1})

    worker = GraphWorker(consumer=tasks, executor=executor, emit=router.publish)
    worker.register("work.graph", Graph(entrypoint="empty").add("empty", EmptyNode()))
    worker.consume("work", concurrency=2)

    tasks.wait_idle()
    router.close()
    worker.close()

    assert executor.calls == [("work.graph", {"id": 1})]
    assert not bus._closed
    assert not tasks._closed


def test_memory_event_bus_tracks_active_dispatches() -> None:
    """同步 handler 尚未返回时，EventBus 不能报告空闲。"""

    bus = memory.EventBus()
    started = ThreadEvent()
    release = ThreadEvent()

    def slow_handler(event: Event) -> None:
        """延迟处理测试投递，以验证并发与空闲等待。

        Args:
            event: 需要发布、观察或处理的事件。
        """

        del event
        started.set()
        release.wait()

    bus.subscribe("slow", slow_handler)
    publisher = Thread(target=bus.publish, args=(Event("slow"),))
    publisher.start()
    assert started.wait(1)
    try:
        assert not bus.idle
        with pytest.raises(TimeoutError):
            bus.wait_idle(0)
    finally:
        release.set()
        publisher.join(1)

    assert not publisher.is_alive()
    bus.wait_idle(0)
    assert bus.idle


def test_wildcard_event_reaches_wildcard_subscriber_once() -> None:
    """事件类型本身为通配符时不重复拼接同一订阅列表。"""

    bus = memory.EventBus()
    received: list[Event] = []
    bus.subscribe("*", received.append)

    event = Event("*", "payload")
    bus.publish(event)

    assert received == [event]


def test_event_bus_continues_after_a_handler_failure() -> None:
    """单个观察者失败时，其他订阅者仍应收到 Event。"""

    bus = memory.EventBus()
    received: list[Event] = []

    def fail(event: Event) -> None:
        """主动失败以验证调用方的异常传播路径。

        Args:
            event: 需要发布、观察或处理的事件。

        Raises:
            ValueError: 参数值或字段组合不合法。
        """

        del event
        raise ValueError("observer failed")

    bus.subscribe("value", fail)
    bus.subscribe("value", received.append)

    with pytest.raises(ValueError, match="observer failed"):
        bus.publish(Event("value", "payload"))
    assert received == [Event("value", "payload")]


def test_task_backend_drains_all_failures_after_idle() -> None:
    """一次 idle 等待后不应把并发失败残留到下一次关闭。"""

    backend = memory.TaskBackend()

    def fail(delivery: Delivery) -> DeliveryResult:
        """主动失败以验证调用方的异常传播路径。

        Args:
            delivery: 携带尝试次数和可选 Slot lease 的本次投递。

        Raises:
            ValueError: 参数值或字段组合不合法。
        """

        raise ValueError(delivery.work.inputs)

    backend.bind("failures", fail, concurrency=2)
    backend.submit("failures", Work("graph", "first"))
    backend.submit("failures", Work("graph", "second"))

    with pytest.raises(ValueError):
        backend.wait_idle()
    backend.wait_idle(0)
    backend.close()


def test_task_backend_requires_explicit_delivery_result() -> None:
    """验证任务后端要求显式 DeliveryResult。"""

    backend = memory.TaskBackend()
    backend.bind("invalid", cast(Any, lambda delivery: None), concurrency=1)
    backend.submit("invalid", Work("graph"))

    with pytest.raises(TypeError, match="must return DeliveryResult"):
        backend.wait_idle()
    backend.close()
