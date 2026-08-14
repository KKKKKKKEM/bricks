"""Runtime 可替换能力端口的组合契约测试。"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable
from dataclasses import replace
from threading import Event as ThreadEvent
from threading import Thread
from uuid import UUID

import pytest

from bricks import Event, Graph, InputPolicy, Node, Output, Ports, Runtime
from bricks.adapters import memory
from bricks.spi import (
    Delivery,
    DeliveryOutcome,
    DeliveryResult,
    EventHandler,
    Work,
    WorkHandler,
)
from bricks.engine.hooks import HookRegistry
from bricks.engine.slots import SlotPool
from bricks.runtime import EventRouter, GraphWorker


class EmptyNode(Node):
    """构造测试注册所需的最小 Graph。"""

    input_ports = Ports()
    output_ports = Ports()
    input_policy = InputPolicy.ON_START

    def execute(self, inputs, context) -> None:
        del inputs, context


class RecordingBus:
    """证明 Runtime 不依赖默认内存 EventBus 的同步替身。"""

    def __init__(self) -> None:
        self.handlers: dict[str, list[EventHandler]] = defaultdict(list)
        self.events: list[Event] = []
        self.closed = False

    @property
    def idle(self) -> bool:
        return True

    def subscribe(
        self,
        event_type: str,
        handler: EventHandler,
        *,
        subscription: str | None = None,
    ) -> None:
        del subscription
        self.handlers[event_type].append(handler)

    def publish(self, event: Event) -> None:
        self.events.append(event)
        handlers = tuple(self.handlers[event.type]) + tuple(self.handlers["*"])
        try:
            for handler in handlers:
                handler(event)
        finally:
            if event._slot_lease is not None:
                event._slot_lease.release()

    def wait_idle(self, timeout: float | None = None) -> None:
        del timeout

    def close(self) -> None:
        self.closed = True


class RecordingTasks:
    """同步消费 Work，模拟可由 Redis/MQ 替换的任务后端。"""

    def __init__(self) -> None:
        self.handlers: dict[str, Callable[[Work], None]] = {}
        self.submitted: list[tuple[str, Work]] = []
        self.closed = False

    @property
    def idle(self) -> bool:
        return True

    def bind(
        self,
        queue: str,
        handler: WorkHandler,
        *,
        concurrency: int,
        slots: SlotPool | None = None,
    ) -> None:
        if slots is None:
            slots = SlotPool(concurrency)

        def handle(work: Work) -> None:
            lease = work._slot_lease or slots._acquire()
            try:
                result = handler(Delivery(replace(work, _slot_lease=lease)))
                if result.outcome is not DeliveryOutcome.ACK:
                    raise result.error or RuntimeError(
                        f"work handler returned {result.outcome.value}"
                    )
            finally:
                lease.release()

        self.handlers[queue] = handle

    def submit(self, queue: str, work: Work) -> None:
        self.submitted.append((queue, work))
        self.handlers[queue](work)

    def wait_idle(self, timeout: float | None = None) -> None:
        del timeout

    def close(self) -> None:
        self.closed = True


class RecordingExecutor:
    """证明 Graph 执行器也可以独立替换。"""

    def __init__(self) -> None:
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
        execution=None,
    ) -> tuple[Output, ...]:
        del emit, plan, slot
        if execution is not None:
            with execution.step(graph.entrypoint):
                pass
        self.calls.append((name, inputs))
        return ()

    def close(self) -> None:
        self.closed = True


class HookableRecordingExecutor(RecordingExecutor):
    """不继承 Engine，但显式提供可选 Hook 能力。"""

    def __init__(self) -> None:
        super().__init__()
        self.hooks = HookRegistry()

    def attach(self, hook, *, phase=None, graph=None, node=None):
        return self.hooks.attach(
            hook,
            phase=phase,
            graph=graph,
            node=node,
        )

    def close(self) -> None:
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
    bus.subscribe("value", lambda event: first.append(event.payload), subscription="work")
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
        raise ValueError(delivery.work.inputs)

    backend.bind("failures", fail, concurrency=2)
    backend.submit("failures", Work("graph", "first"))
    backend.submit("failures", Work("graph", "second"))

    with pytest.raises(ValueError):
        backend.wait_idle()
    backend.wait_idle(0)
    backend.close()


def test_task_backend_requires_explicit_delivery_result() -> None:
    backend = memory.TaskBackend()
    backend.bind("invalid", lambda delivery: None, concurrency=1)  # type: ignore[arg-type]
    backend.submit("invalid", Work("graph"))

    with pytest.raises(TypeError, match="must return DeliveryResult"):
        backend.wait_idle()
    backend.close()
