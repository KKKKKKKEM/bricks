"""默认单进程 Runtime 的插件装配。"""

from __future__ import annotations

from typing import cast

from ..adapters import memory
from ..engine.executor import Engine
from ..engine.observation import ObservationHub
from ..engine.policies import PolicyRegistry
from ..plugins import (
    CAP_EVENT_BUS,
    CAP_EVENT_ROUTER,
    CAP_GRAPH_EXECUTOR,
    CAP_GRAPH_WORKER,
    CAP_INPUT_SELECTOR,
    CAP_NODE_HOOK,
    CAP_RUNTIME_OBSERVER,
    CAP_TASK_BACKEND,
    NodeHookContribution,
    PluginContext,
    PluginDescriptor,
)
from ..spi import EventBus, GraphExecutor, TaskBackend
from ._utils import _close_components, _unique
from .router import EventRouter
from .worker import GraphWorker


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
        tasks: TaskBackend | None = None,
        executor: GraphExecutor | None = None,
        close_injected: bool = False,
        _provide_capabilities: frozenset[str] | None = None,
    ) -> None:
        if type(close_injected) is not bool:
            raise TypeError("close_injected must be a boolean")
        if tasks is not None and not (
            callable(getattr(tasks, "submit", None))
            and callable(getattr(tasks, "bind", None))
        ):
            raise TypeError("tasks must implement TaskPublisher and TaskConsumer")
        infrastructure = {
            CAP_EVENT_BUS,
            CAP_TASK_BACKEND,
            CAP_GRAPH_EXECUTOR,
        }
        provided = infrastructure if _provide_capabilities is None else set(
            _provide_capabilities
        )
        if not provided <= infrastructure:
            raise ValueError("local runtime can only select infrastructure capabilities")
        if events is not None and CAP_EVENT_BUS not in provided:
            raise TypeError("events cannot be injected when event bus is externally provided")
        if tasks is not None and CAP_TASK_BACKEND not in provided:
            raise TypeError("tasks cannot be injected when task backend is externally provided")
        if executor is not None and CAP_GRAPH_EXECUTOR not in provided:
            raise TypeError("executor cannot be injected when graph executor is externally provided")
        self.descriptor = PluginDescriptor(
            "bricks.core/local-runtime",
            "1.0.0",
            requires_capabilities=tuple(sorted(infrastructure - provided)),
            provides=tuple(
                capability
                for capability in (
                    CAP_EVENT_BUS,
                    CAP_TASK_BACKEND,
                    CAP_GRAPH_EXECUTOR,
                    CAP_EVENT_ROUTER,
                    CAP_GRAPH_WORKER,
                )
                if capability in provided
                or capability in (CAP_EVENT_ROUTER, CAP_GRAPH_WORKER)
            ),
        )
        self._events = (
            memory.EventBus() if events is None else events
        ) if CAP_EVENT_BUS in provided else None
        self._tasks = (
            memory.TaskBackend() if tasks is None else tasks
        ) if CAP_TASK_BACKEND in provided else None
        self._router_observations = ObservationHub()
        self._worker_observations = ObservationHub()
        self._policies = PolicyRegistry()
        self._executor = (
            Engine(observations=self._worker_observations)
            if executor is None
            else executor
        ) if CAP_GRAPH_EXECUTOR in provided else None
        self._provided = frozenset(provided)
        self._owned = (
            CAP_EVENT_BUS in provided and events is None,
            CAP_TASK_BACKEND in provided and tasks is None,
            CAP_GRAPH_EXECUTOR in provided and executor is None,
        )
        self._close_injected = close_injected
        self.router: EventRouter | None = None
        self.worker: GraphWorker | None = None

    def setup(self, context: PluginContext) -> None:
        events = (
            self._events
            if CAP_EVENT_BUS in self._provided
            else context.require(CAP_EVENT_BUS)
        )
        tasks = (
            self._tasks
            if CAP_TASK_BACKEND in self._provided
            else context.require(CAP_TASK_BACKEND)
        )
        if not (
            callable(getattr(tasks, "submit", None))
            and callable(getattr(tasks, "bind", None))
        ):
            raise TypeError("task backend capability must publish and consume Work")
        tasks = cast(TaskBackend, tasks)
        executor = (
            self._executor
            if CAP_GRAPH_EXECUTOR in self._provided
            else context.require(CAP_GRAPH_EXECUTOR)
        )
        self.router = EventRouter(
            events=events,
            publisher=tasks,
            observations=self._router_observations,
        )
        self.worker = GraphWorker(
            consumer=tasks,
            executor=executor,
            emit=self.router.publish,
            emit_local=self.router.publish_local,
            observations=self._worker_observations,
            policies=self._policies,
        )
        if CAP_EVENT_BUS in self._provided:
            context.provide(CAP_EVENT_BUS, events)
        if CAP_TASK_BACKEND in self._provided:
            context.provide(CAP_TASK_BACKEND, tasks)
        if CAP_GRAPH_EXECUTOR in self._provided:
            context.provide(CAP_GRAPH_EXECUTOR, executor)
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
            self._router_observations.attach(contribution.value)
            self._worker_observations.attach(contribution.value)

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
            if component is not None and (owned or self._close_injected)
        )
        failure = _close_components(reversed(_unique(*components)), failure)
        if failure is not None:
            raise failure
