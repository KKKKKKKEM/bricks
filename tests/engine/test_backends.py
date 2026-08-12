"""Runtime 可替换能力端口的组合契约测试。"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable
from threading import Event as ThreadEvent
from threading import Thread

import pytest

from bricks import Event, Graph, InputPolicy, Node, Output, Ports, Runtime
from bricks.engine.backends import (
    EventHandler,
    MemoryEventBus,
    MemoryTaskBackend,
    Work,
    WorkHandler,
)


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

    def subscribe(self, event_type: str, handler: EventHandler) -> None:
        self.handlers[event_type].append(handler)

    def publish(self, event: Event) -> None:
        self.events.append(event)
        handlers = tuple(self.handlers[event.type]) + tuple(self.handlers["*"])
        for handler in handlers:
            handler(event)

    def wait_idle(self, timeout: float | None = None) -> None:
        del timeout

    def close(self) -> None:
        self.closed = True


class RecordingTasks:
    """同步消费 Work，模拟可由 Redis/MQ 替换的任务后端。"""

    def __init__(self) -> None:
        self.handlers: dict[str, WorkHandler] = {}
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
    ) -> None:
        del concurrency
        self.handlers[queue] = handler

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
    ) -> tuple[Output, ...]:
        del graph, emit
        self.calls.append((name, inputs))
        return ()

    def close(self) -> None:
        self.closed = True


def test_runtime_composes_replaceable_capabilities() -> None:
    """替换传输、任务和执行器不改变 Runtime/Graph 使用 API。"""

    bus = RecordingBus()
    tasks = RecordingTasks()
    executor = RecordingExecutor()
    graph = Graph(entrypoint="empty").add("empty", EmptyNode())

    with Runtime(events=bus, tasks=tasks, executor=executor) as runtime:
        runtime.register("work.graph", graph)
        runtime.on(
            "work.created",
            graph="work.graph",
            queue="work-tasks",
            concurrency=8,
        )
        runtime.emit("work.created", {"id": 1})
        runtime.wait_idle()

    assert bus.events == [Event("work.created", {"id": 1})]
    assert tasks.submitted == [
        ("work-tasks", Work("work.graph", {"id": 1}))
    ]
    assert tasks.submitted[0][1].trigger == Event("work.created", {"id": 1})
    assert executor.calls == [("work.graph", {"id": 1})]
    assert bus.closed and tasks.closed and executor.closed


def test_memory_event_bus_tracks_active_dispatches() -> None:
    """同步 handler 尚未返回时，EventBus 不能报告空闲。"""

    bus = MemoryEventBus()
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

    bus = MemoryEventBus()
    received: list[Event] = []
    bus.subscribe("*", received.append)

    event = Event("*", "payload")
    bus.publish(event)

    assert received == [event]


def test_event_bus_continues_after_a_handler_failure() -> None:
    """单个观察者失败时，其他订阅者仍应收到 Event。"""

    bus = MemoryEventBus()
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

    backend = MemoryTaskBackend()

    def fail(work: Work) -> None:
        raise ValueError(work.inputs)

    backend.bind("failures", fail, concurrency=2)
    backend.submit("failures", Work("graph", "first"))
    backend.submit("failures", Work("graph", "second"))

    with pytest.raises(ValueError):
        backend.wait_idle()
    backend.wait_idle(0)
    backend.close()
