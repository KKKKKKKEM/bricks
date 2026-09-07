"""默认单进程 Runtime 的插件装配。"""

from __future__ import annotations

from typing import cast

from ..adapters import memory
from ..engine.execution import Execution
from ..engine.executor import Engine
from ..engine.observation import ObservationHub
from ..engine.policies import PolicyRegistry
from ..plugins import (
    CAP_EVENT_BUS,
    CAP_EVENT_ROUTER,
    CAP_EXECUTION_FACTORY,
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
from ..spi import EventBus, ExecutionFactory, GraphExecutor, TaskBackend
from ._utils import _close_components, _unique
from .router import EventRouter
from .worker import GraphWorker


class LocalRuntimePlugin:
    """使用标准能力协议组装单进程 Runtime 的内建插件。

    Attributes:
        descriptor: 插件身份、依赖和能力声明。
        _events: 事件传输实现。
        _tasks: 任务发布与消费后端。
        _router_observations: 路由角色的生命周期观察中心。
        _worker_observations: 执行角色的生命周期观察中心。
        _policies: 当前角色使用的输入策略注册能力。
        _executor: 执行 Graph 的实现。
        _execution_factory: 创建可替换 Execution 句柄的工厂。
        _provided: 当前默认插件负责提供的能力集合。
        _owned: 由当前插件接管并负责关闭的组件。
        _close_injected: 是否负责关闭调用方注入的底层组件。
        router: 负责事件发布和工作路由的角色。
        worker: 负责消费工作和执行 Graph 的角色。
    """

    descriptor = PluginDescriptor(
        "bricks.core/local-runtime",
        "1.0.0",
        provides=(
            CAP_EVENT_BUS,
            CAP_TASK_BACKEND,
            CAP_GRAPH_EXECUTOR,
            CAP_EXECUTION_FACTORY,
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
        execution_factory: ExecutionFactory | None = None,
        close_injected: bool = False,
        _provide_capabilities: frozenset[str] | None = None,
    ) -> None:
        """保存默认装配需要的底层组件与资源所有权设置。

        Args:
            events: 注入的事件传输实现。
            tasks: 注入的任务传输实现。
            executor: 实际执行任务或 Graph 的实现。
            execution_factory: 创建独立 Execution 句柄的工厂能力。
            close_injected: 是否由当前组件关闭注入的底层资源。
            _provide_capabilities: 默认插件需要自行提供的底层能力集合。

        Raises:
            TypeError: 参数类型或接口实现不符合当前契约。
            ValueError: 参数值或字段组合不合法。
        """

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
            CAP_EXECUTION_FACTORY,
        }
        provided = (
            infrastructure
            if _provide_capabilities is None
            else set(_provide_capabilities)
        )
        if not provided <= infrastructure:
            raise ValueError(
                "local runtime can only select infrastructure capabilities"
            )
        if events is not None and CAP_EVENT_BUS not in provided:
            raise TypeError(
                "events cannot be injected when event bus is externally provided"
            )
        if tasks is not None and CAP_TASK_BACKEND not in provided:
            raise TypeError(
                "tasks cannot be injected when task backend is externally provided"
            )
        if executor is not None and CAP_GRAPH_EXECUTOR not in provided:
            raise TypeError(
                "executor cannot be injected when graph executor is externally provided"
            )
        if execution_factory is not None and CAP_EXECUTION_FACTORY not in provided:
            raise TypeError("execution_factory is externally provided")
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
                    CAP_EXECUTION_FACTORY,
                    CAP_EVENT_ROUTER,
                    CAP_GRAPH_WORKER,
                )
                if capability in provided
                or capability in (CAP_EVENT_ROUTER, CAP_GRAPH_WORKER)
            ),
        )
        self._events = events
        self._tasks = tasks
        self._router_observations = ObservationHub()
        self._worker_observations = ObservationHub()
        self._policies = PolicyRegistry()
        self._executor = executor
        self._execution_factory = execution_factory
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
        """在插件装配阶段登记声明的能力或贡献。

        Args:
            context: 当前调用的执行或插件上下文。

        Raises:
            TypeError: 参数类型或接口实现不符合当前契约。
        """

        if CAP_EVENT_BUS in self._provided and self._events is None:
            self._events = memory.EventBus()
        if CAP_TASK_BACKEND in self._provided and self._tasks is None:
            self._tasks = memory.TaskBackend()
        if CAP_GRAPH_EXECUTOR in self._provided and self._executor is None:
            self._executor = Engine(observations=self._worker_observations)
        if CAP_EXECUTION_FACTORY in self._provided and self._execution_factory is None:
            self._execution_factory = Execution
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
        execution_factory = (
            self._execution_factory
            if CAP_EXECUTION_FACTORY in self._provided
            else context.require(CAP_EXECUTION_FACTORY)
        )
        if not callable(execution_factory):
            raise TypeError("execution factory capability must be callable")
        self.router = EventRouter(
            events=events,
            publisher=tasks,
            observations=self._router_observations,
        )
        self.worker = GraphWorker(
            consumer=tasks,
            executor=executor,
            execution_factory=cast(ExecutionFactory, execution_factory),
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
        if CAP_EXECUTION_FACTORY in self._provided:
            context.provide(CAP_EXECUTION_FACTORY, execution_factory)
        context.provide(CAP_EVENT_ROUTER, self.router)
        context.provide(CAP_GRAPH_WORKER, self.worker)

    def start(self, context: PluginContext) -> None:
        """完成本地默认插件启动，底层组件已在装配阶段创建。

        Args:
            context: 当前调用的执行或插件上下文。
        """

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
        """停止插件并释放其拥有的资源。

        Args:
            context: 当前调用的执行或插件上下文。
        """

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
