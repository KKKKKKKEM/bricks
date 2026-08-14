"""默认单进程 Runtime 的插件装配。"""

from __future__ import annotations

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
from ..spi import EventBus, GraphExecutor, TaskConsumer, TaskPublisher
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
        self._events = memory.EventBus() if events is None else events
        self._tasks = memory.TaskBackend() if tasks is None else tasks
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
