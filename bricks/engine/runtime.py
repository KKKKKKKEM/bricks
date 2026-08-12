"""用可替换能力端口组装 Graph 和 Event。"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from functools import partial
from threading import RLock
from typing import Any

from .backends import (
    EventBus,
    GraphExecutor,
    MemoryEventBus,
    MemoryTaskBackend,
    TaskBackend,
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

EventHandler = Callable[[Event], None]


class Runtime:
    """注册 Graph、连接 Event，并管理可替换组件的生命周期。"""

    def __init__(
        self,
        *,
        events: EventBus | None = None,
        tasks: TaskBackend | None = None,
        executor: GraphExecutor | None = None,
    ) -> None:
        """组装事件传输、任务后端和 Graph 执行器。"""

        self._events = MemoryEventBus() if events is None else events
        self._tasks = MemoryTaskBackend() if tasks is None else tasks
        self._executor = Engine() if executor is None else executor
        self._graphs: dict[str, Graph] = {}
        self._routes: set[tuple[str, str, str]] = set()
        self._queues: dict[str, int] = {}
        self._lock = RLock()
        self._closed = False

    def register(self, name: str, graph: Graph) -> Runtime:
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

    def on(
        self,
        event_type: str,
        handler: EventHandler | None = None,
        *,
        graph: str | None = None,
        queue: str | None = None,
        concurrency: int = 1,
    ) -> Runtime:
        """兼容入口：观察 Event 或把 Event 路由到 Graph。

        新代码可以使用语义更明确的 :meth:`observe` 和 :meth:`route`。
        """

        if handler is not None:
            if graph is not None or queue is not None or concurrency != 1:
                raise TypeError("event observer cannot also configure a graph route")
            return self.observe(event_type, handler)
        if graph is None or queue is None:
            raise TypeError("graph route requires graph and queue")
        return self.route(
            event_type,
            graph=graph,
            queue=queue,
            concurrency=concurrency,
        )

    def observe(self, event_type: str, handler: EventHandler) -> Runtime:
        """注册一个同步 Event 观察者。"""

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
        concurrency: int = 1,
    ) -> Runtime:
        """把 Event 路由到注册 Graph 的命名执行队列。"""

        self._ensure_open()
        event_type = require_non_empty_string(event_type, "subscription event type")
        graph = require_non_empty_string(graph, "route graph")
        queue = require_non_empty_string(queue, "route queue")
        if type(concurrency) is not int:
            raise TypeError("queue concurrency must be an integer")
        if concurrency < 1:
            raise ValueError("queue concurrency must be at least 1")
        with self._lock:
            if graph not in self._graphs:
                raise UnknownGraphError(f"unknown registered graph {graph!r}")
            route = (event_type, graph, queue)
            if route in self._routes:
                raise BricksRuntimeError(f"duplicate event route {route!r}")
            configured = self._queues.get(queue)
            if configured is not None and configured != concurrency:
                raise BricksRuntimeError(
                    f"queue {queue!r} already uses concurrency {configured}"
                )
            if configured is None:
                self._tasks.bind(
                    queue,
                    self._execute_work,
                    concurrency=concurrency,
                )
                self._queues[queue] = concurrency
            self._events.subscribe(
                event_type,
                partial(self._submit, graph, queue),
            )
            self._routes.add(route)
        return self

    def emit(self, event_or_type: Event | str, payload: Any = None) -> Event:
        """从 Runtime 外部发布一个 Event。"""

        if isinstance(event_or_type, Event):
            if payload is not None:
                raise TypeError("complete Event must not be combined with payload")
            event = event_or_type
        else:
            event = Event(event_or_type, payload)
        self._publish(event)
        return event

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
            return self._executor.execute(graph, registered, inputs, self._publish)
        return self._executor.execute(
            graph, registered, inputs, self._publish, plan=plan
        )

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
                    raise ValueError(
                        f"graph {graph!r} has no node {node!r}"
                    )
        if not isinstance(self._executor, Engine):
            raise TypeError("the configured GraphExecutor does not support hooks")
        return self._executor.hooks.attach(
            hook,
            phase=phase,
            graph=graph,
            node=node,
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

    def wait_idle(self, timeout: float | None = None) -> None:
        """等待 Event 与 Work 级联网络静止。"""

        _validate_timeout(timeout)
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            remaining = self._remaining(deadline)
            self._events.wait_idle(remaining)
            remaining = self._remaining(deadline)
            self._tasks.wait_idle(remaining)
            if self._events.idle and self._tasks.idle:
                return
            if deadline is not None and time.monotonic() >= deadline:
                raise TimeoutError("Runtime did not become idle before timeout")

    def close(self) -> None:
        """排空已接受工作，然后关闭注入的组件。"""

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
        for component in (self._tasks, self._events, self._executor):
            try:
                component.close()
            except Exception as exc:  # noqa: BLE001
                if failure is None:
                    failure = exc
        if failure is not None:
            raise failure

    def __enter__(self):
        self._ensure_open()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: object,
    ) -> None:
        del exc_type, traceback
        try:
            self.close()
        except Exception:
            if exc_value is None:
                raise

    def _publish(self, event: Event) -> None:
        self._ensure_open()
        try:
            self._events.publish(event)
        except EventDispatchError:
            raise
        except Exception as exc:
            raise EventDispatchError(event, exc) from exc

    def _submit(self, graph: str, queue: str, event: Event) -> None:
        self._tasks.submit(queue, Work(graph, event.payload, trigger=event))

    def _execute_work(self, work: Work) -> None:
        graph = self._get_graph(work.graph)
        try:
            self._executor.execute(work.graph, graph, work.inputs, self._publish)
        except ExecutionError as exc:
            if exc.event is None:
                exc.event = work.trigger
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
                raise RuntimeClosedError("Runtime is closed")

    @staticmethod
    def _remaining(deadline: float | None) -> float | None:
        if deadline is None:
            return None
        return max(0.0, deadline - time.monotonic())
