"""组合 EventRouter 与 GraphWorker 的 Runtime 门面。"""

from __future__ import annotations

import logging
import time
from collections.abc import AsyncIterator, Callable, Iterable, Iterator
from threading import RLock
from typing import Any

from ..engine.core import Output, _validate_timeout
from ..engine.errors import RuntimeClosedError
from ..engine.events import Event
from ..engine.execution import Execution, ExecutionLimits
from ..engine.graph import ExecutionPlan, Graph
from ..engine.hooks import HookHandle, HookPhase, NodeHook
from ..engine.observation import (
    CompositeObserverHandle,
    RuntimeObserver,
)
from ..engine.policies import InputSelector
from ..engine.slots import SlotPool
from ..plugins import (
    CAP_EVENT_BUS,
    CAP_EVENT_ROUTER,
    CAP_GRAPH_EXECUTOR,
    CAP_GRAPH_WORKER,
    CAP_TASK_BACKEND,
    Plugin,
    PluginHost,
)
from ..spi import EventHandler
from ._utils import _close_components, _remaining
from .plugin import LocalRuntimePlugin
from .router import EventRouter
from .worker import GraphWorker

_LOGGER = logging.getLogger(__name__)


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
                infrastructure = {
                    CAP_EVENT_BUS,
                    CAP_TASK_BACKEND,
                    CAP_GRAPH_EXECUTOR,
                }
                selected = (
                    LocalRuntimePlugin(
                        _provide_capabilities=frozenset(infrastructure - provides)
                    ),
                    *selected,
                )
            host = PluginHost(selected)
            try:
                host.start()
                router = host.require(CAP_EVENT_ROUTER)
                worker = host.require(CAP_GRAPH_WORKER)
                if not isinstance(router, EventRouter):
                    raise TypeError("router must be an EventRouter")
                if not isinstance(worker, GraphWorker):
                    raise TypeError("worker must be a GraphWorker")
            except BaseException:
                try:
                    host.close()
                except BaseException:
                    _LOGGER.exception("failed to roll back Runtime plugin host")
                raise
        else:
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

        return CompositeObserverHandle(
            (
                self.router.observe_runtime(observer),
                self.worker.observe_runtime(observer),
            )
        )

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
