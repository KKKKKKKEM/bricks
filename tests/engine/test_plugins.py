"""统一插件宿主和 Runtime 插件装配的契约测试。"""

from __future__ import annotations

from threading import enumerate as enumerate_threads

import pytest

from bricks import Graph, Node, Output, Ports, Runtime
from bricks.adapters import memory
from bricks.engine.executor import Engine
from bricks.engine.hooks import NodeCall
from bricks.engine.observation import RuntimeEvent
from bricks.engine.policies import PolicyRef
from bricks.plugins import (
    CAP_EVENT_BUS,
    CAP_EVENT_ROUTER,
    CAP_GRAPH_EXECUTOR,
    CAP_GRAPH_WORKER,
    CAP_INPUT_SELECTOR,
    CAP_NODE_HOOK,
    CAP_RUNTIME_OBSERVER,
    CAP_TASK_BACKEND,
    ContributionPlugin,
    NodeHookContribution,
    PluginDescriptor,
    PluginHost,
)
from bricks.runtime import LocalRuntimePlugin


def test_multiple_plugins_can_contribute_each_extension_kind() -> None:
    """验证多个插件能分别贡献各类聚合扩展。"""

    observed: list[list[RuntimeEvent]] = [[], []]
    calls: list[str] = []

    def record_call(call: NodeCall) -> NodeCall:
        """记录当前装配或调用路径，验证组件经过约定入口。

        Args:
            call: Hook 当前处理的节点调用记录。

        Returns:
            当前测试回调收集或构造的结果。
        """

        calls.append(call.node_id)
        return call

    plugins = tuple(
        ContributionPlugin(
            f"example/{index}",
            selectors={f"example/{index}": AnySelector()},
            hooks={f"example/{index}": record_call},
            observers={
                f"example/{index}": lambda event, index=index: observed[index].append(
                    event
                )
            },
        )
        for index in range(2)
    )
    with Runtime(plugins=plugins) as runtime:
        runtime.register("work", Graph(entrypoint="node").add(node=Source()))
        assert runtime.run("work", "hello") == (Output("hello", "value"),)
        for capability in (CAP_INPUT_SELECTOR, CAP_NODE_HOOK, CAP_RUNTIME_OBSERVER):
            assert runtime.plugin_host is not None
            assert len(runtime.plugin_host.contributions(capability)) == 2
    assert calls == ["node", "node"]
    assert observed[0] == observed[1]
    assert observed[0]


def test_duplicate_contribution_names_across_plugins_are_rejected() -> None:
    """验证跨插件重复的贡献名称被拒绝。"""

    plugins = tuple(
        ContributionPlugin(
            f"example/{index}", observers={"example/shared": lambda event: None}
        )
        for index in range(2)
    )
    with pytest.raises(ValueError, match="already exists"):
        PluginHost(plugins).start()


def test_aggregate_declarations_cannot_be_fulfilled_by_singletons() -> None:
    """验证单例能力不能代替聚合贡献声明。"""

    class WrongKind(RecordingPlugin):
        def setup(self, context):
            """在插件装配阶段登记声明的能力或贡献。

            Args:
                context: 当前调用的执行或插件上下文。
            """

            context.provide("example/items", object())

    plugin = WrongKind("example/wrong", [])
    plugin.descriptor = PluginDescriptor(
        "example/wrong", "1", contributes=("example/items",)
    )
    with pytest.raises(ValueError, match="did not declare"):
        PluginHost((plugin,)).start()


def test_host_rejects_missing_contribution_and_mixed_capability_kinds() -> None:
    """验证宿主拒绝缺失贡献和混用能力种类。"""

    plugin = RecordingPlugin("example/aggregate", [])
    plugin.descriptor = PluginDescriptor(
        "example/aggregate", "1", contributes=("example/items",)
    )
    with pytest.raises(RuntimeError, match="did not provide declared"):
        PluginHost((plugin,)).start()
    singleton = RecordingPlugin("example/singleton", [], capability="example/items")
    with pytest.raises(ValueError, match="both singleton and aggregate"):
        PluginHost((plugin, singleton))


@pytest.mark.parametrize("stage", ["validation", "setup", "start"])
def test_failed_runtime_construction_does_not_leave_runner_threads(stage) -> None:
    """验证 Runtime 构造失败后不遗留运行器线程。

    Args:
        stage: 当前用例使用的 stage 夹具或参数化输入。
    """

    class Broken(RecordingPlugin):
        def setup(self, context):
            """在插件装配阶段登记声明的能力或贡献。

            Args:
                context: 当前调用的执行或插件上下文。

            Raises:
                RuntimeError: 当前生命周期状态或操作顺序不允许此操作。
            """

            if stage == "setup":
                raise RuntimeError("setup failed")
            super().setup(context)

        def start(self, context):
            """启动已经装配的组件或提交一次新的执行。

            Args:
                context: 当前调用的执行或插件上下文。

            Raises:
                RuntimeError: 当前生命周期状态或操作顺序不允许此操作。
            """

            if stage == "start":
                raise RuntimeError("start failed")

    plugin = Broken(
        "example/broken",
        [],
        requires=("example/missing",) if stage == "validation" else (),
    )
    before = {
        thread.ident
        for thread in enumerate_threads()
        if thread.name == "bricks-async-runner"
    }
    for _ in range(3):
        with pytest.raises(
            (ValueError, RuntimeError),
            match="missing required plugin|setup failed|start failed",
        ):
            Runtime(plugins=(plugin,))
    after = {
        thread.ident
        for thread in enumerate_threads()
        if thread.name == "bricks-async-runner"
    }
    assert after == before


def test_builtin_setup_rolls_back_partially_created_components(monkeypatch) -> None:
    """验证默认插件回滚部分创建的底层组件。

    Args:
        monkeypatch: 当前用例使用的 monkeypatch 夹具或参数化输入。
    """

    bus = memory.EventBus()
    closed = []
    original_close = bus.close

    def close():
        """结束当前组件的生命周期并释放其拥有的资源。"""

        closed.append(True)
        original_close()

    def broken_tasks():
        """模拟任务组件创建失败，验证部分装配资源回滚。

        Raises:
            RuntimeError: 当前生命周期状态或操作顺序不允许此操作。
        """

        raise RuntimeError("task construction failed")

    monkeypatch.setattr(bus, "close", close)
    monkeypatch.setattr("bricks.runtime.plugin.memory.EventBus", lambda: bus)
    monkeypatch.setattr("bricks.runtime.plugin.memory.TaskBackend", broken_tasks)
    with pytest.raises(RuntimeError, match="task construction failed"):
        Runtime()
    assert closed == [True]


@pytest.mark.parametrize(
    ("hook", "phase", "error"),
    [(object(), None, TypeError), (lambda call: call, "invalid", ValueError)],
)
def test_hook_contribution_validates_hook_and_phase_before_registration(
    hook, phase, error
) -> None:
    """验证 Hook 贡献注册前检查对象和阶段。

    Args:
        hook: 节点 Hook 对象或单阶段回调。
        phase: 函数 Hook 对应的执行阶段。
        error: 需要传播、记录或用于恢复的异常。
    """

    with pytest.raises(error):
        NodeHookContribution(hook, phase=phase, graph="not-yet-registered")


class RecordingPlugin:
    """当前契约测试使用的 RecordingPlugin 替代实现。

    Attributes:
        descriptor: 插件身份、依赖和能力声明。
        events: 测试记录的事件或注入的事件总线。
        capability: 当前贡献所属的能力类别。
    """

    def __init__(
        self,
        plugin_id: str,
        events: list[str],
        *,
        requires: tuple[str, ...] = (),
        requires_capabilities: tuple[str, ...] = (),
        capability: str | None = None,
    ) -> None:
        """初始化实例及其依赖，建立当前对象独立维护的状态。

        Args:
            plugin_id: 带命名空间的插件标识。
            events: 注入的事件传输实现。
            requires: 当前插件依赖的插件标识集合。
            requires_capabilities: 当前插件要求宿主已提供的能力集合。
            capability: 需要提供、取得或移除的能力名称。
        """

        provides = () if capability is None else (capability,)
        self.descriptor = PluginDescriptor(
            plugin_id,
            "1.0.0",
            requires=requires,
            requires_capabilities=requires_capabilities,
            provides=provides,
        )
        self.events = events
        self.capability = capability

    def setup(self, context) -> None:
        """在插件装配阶段登记声明的能力或贡献。

        Args:
            context: 当前调用的执行或插件上下文。
        """

        self.events.append(f"setup:{self.descriptor.id}")
        if self.capability is not None:
            context.provide(self.capability, self.descriptor.id)

    def start(self, context) -> None:
        """启动已经装配的组件或提交一次新的执行。

        Args:
            context: 当前调用的执行或插件上下文。
        """

        del context
        self.events.append(f"start:{self.descriptor.id}")

    def stop(self, context) -> None:
        """停止插件并释放其拥有的资源。

        Args:
            context: 当前调用的执行或插件上下文。
        """

        del context
        self.events.append(f"stop:{self.descriptor.id}")


def test_host_orders_dependencies_and_stops_in_reverse() -> None:
    """验证插件按依赖启动并按逆序关闭。"""

    events: list[str] = []
    base = RecordingPlugin("example/base", events, capability="example/base-value")
    feature = RecordingPlugin("example/feature", events, requires=("example/base",))

    host = PluginHost((feature, base)).start()

    assert host.require("example/base-value") == "example/base"
    assert events == [
        "setup:example/base",
        "setup:example/feature",
        "start:example/base",
        "start:example/feature",
    ]
    host.close()
    assert events[-2:] == ["stop:example/feature", "stop:example/base"]


def test_host_orders_capability_dependencies_and_rejects_missing_provider() -> None:
    """验证按能力依赖装配且拒绝缺失的提供者。"""

    events: list[str] = []
    provider = RecordingPlugin("example/provider", events, capability="example/value")
    consumer = RecordingPlugin(
        "example/consumer",
        events,
        requires_capabilities=("example/value",),
    )

    host = PluginHost((consumer, provider)).start()
    assert events[:2] == ["setup:example/provider", "setup:example/consumer"]
    host.close()

    with pytest.raises(ValueError, match="missing required capability"):
        PluginHost((consumer,))


def test_host_rejects_missing_cycle_and_capability_conflict() -> None:
    """验证宿主拒绝缺失依赖、循环依赖和能力冲突。"""

    events: list[str] = []
    with pytest.raises(ValueError, match="missing required plugin"):
        PluginHost(
            (RecordingPlugin("example/feature", events, requires=("example/base",)),)
        )

    left = RecordingPlugin("example/left", events, requires=("example/right",))
    right = RecordingPlugin("example/right", events, requires=("example/left",))
    with pytest.raises(ValueError, match="cyclic plugin dependency"):
        PluginHost((left, right))

    first = RecordingPlugin("example/first", events, capability="example/value")
    second = RecordingPlugin("example/second", events, capability="example/value")
    with pytest.raises(ValueError, match="already provided"):
        PluginHost((first, second)).start()


def test_host_rejects_incompatible_plugin_api() -> None:
    """验证宿主拒绝不兼容的插件 SPI 版本。"""

    class FuturePlugin:
        """当前契约测试使用的 FuturePlugin 替代实现。

        Attributes:
            descriptor: 插件身份、依赖和能力声明。
        """

        descriptor = PluginDescriptor("example/future", "1.0.0", api_version="2")

        def setup(self, context):
            """在插件装配阶段登记声明的能力或贡献。

            Args:
                context: 当前调用的执行或插件上下文。
            """

            del context

        def start(self, context):
            """启动已经装配的组件或提交一次新的执行。

            Args:
                context: 当前调用的执行或插件上下文。
            """

            del context

        def stop(self, context):
            """停止插件并释放其拥有的资源。

            Args:
                context: 当前调用的执行或插件上下文。
            """

            del context

    with pytest.raises(ValueError, match="requires API 2"):
        PluginHost((FuturePlugin(),))


def test_setup_failure_rolls_back_configured_plugins() -> None:
    """验证 setup 失败时回滚已接管的插件。"""

    events: list[str] = []
    base = RecordingPlugin("example/base", events)

    class Broken(RecordingPlugin):
        def setup(self, context) -> None:
            """在插件装配阶段登记声明的能力或贡献。

            Args:
                context: 当前调用的执行或插件上下文。

            Raises:
                RuntimeError: 当前生命周期状态或操作顺序不允许此操作。
            """

            super().setup(context)
            raise RuntimeError("setup failed")

    with pytest.raises(RuntimeError, match="setup failed"):
        PluginHost((base, Broken("example/broken", events))).start()

    assert events[-2:] == ["stop:example/broken", "stop:example/base"]


def test_start_failure_rolls_back_plugins_in_reverse_order() -> None:
    """验证 start 失败时按逆序回滚插件。"""

    events: list[str] = []

    class Broken(RecordingPlugin):
        def start(self, context) -> None:
            """启动已经装配的组件或提交一次新的执行。

            Args:
                context: 当前调用的执行或插件上下文。

            Raises:
                RuntimeError: 当前生命周期状态或操作顺序不允许此操作。
            """

            super().start(context)
            raise RuntimeError("start failed")

    with pytest.raises(RuntimeError, match="start failed"):
        PluginHost(
            (
                RecordingPlugin("example/base", events),
                Broken("example/broken", events),
            )
        ).start()

    assert events[-2:] == ["stop:example/broken", "stop:example/base"]


def test_host_rejects_declared_but_missing_capability() -> None:
    """验证宿主拒绝未兑现的能力声明。"""

    class Missing(RecordingPlugin):
        def setup(self, context) -> None:
            """在插件装配阶段登记声明的能力或贡献。

            Args:
                context: 当前调用的执行或插件上下文。
            """

            del context

    plugin = Missing("example/missing", [], capability="example/value")

    with pytest.raises(RuntimeError, match="did not provide declared capabilities"):
        PluginHost((plugin,)).start()


def test_stop_failure_does_not_skip_remaining_plugins() -> None:
    """验证单个插件关闭失败后仍关闭其余插件。"""

    events: list[str] = []

    class BrokenStop(RecordingPlugin):
        def stop(self, context) -> None:
            """停止插件并释放其拥有的资源。

            Args:
                context: 当前调用的执行或插件上下文。

            Raises:
                RuntimeError: 当前生命周期状态或操作顺序不允许此操作。
            """

            super().stop(context)
            raise RuntimeError(f"stop failed: {self.descriptor.id}")

    host = PluginHost(
        (
            BrokenStop("example/first", events),
            BrokenStop("example/second", events),
        )
    ).start()

    with pytest.raises(RuntimeError, match="example/second"):
        host.close()

    assert events[-2:] == ["stop:example/second", "stop:example/first"]


def test_contributions_reject_duplicate_names_and_freeze_after_start() -> None:
    """验证贡献名称唯一且宿主启动后冻结注册。"""

    class Contributions:
        """当前契约测试使用的 Contributions 替代实现。

        Attributes:
            descriptor: 插件身份、依赖和能力声明。
        """

        descriptor = PluginDescriptor(
            "example/contributions",
            "1.0.0",
            contributes=("example/items",),
        )

        def setup(self, context) -> None:
            """在插件装配阶段登记声明的能力或贡献。

            Args:
                context: 当前调用的执行或插件上下文。
            """

            context.contribute("example/items", "example/item", object())
            with pytest.raises(ValueError, match="already exists"):
                context.contribute("example/items", "example/item", object())

        def start(self, context) -> None:
            """启动已经装配的组件或提交一次新的执行。

            Args:
                context: 当前调用的执行或插件上下文。
            """

            with pytest.raises(RuntimeError, match="frozen"):
                context.contribute("example/items", "example/late", object())

        def stop(self, context) -> None:
            """停止插件并释放其拥有的资源。

            Args:
                context: 当前调用的执行或插件上下文。
            """

            del context

    PluginHost((Contributions(),)).start().close()


def test_runtime_role_validation_failure_closes_started_plugin_host() -> None:
    """验证角色校验失败时关闭已经启动的插件宿主。"""

    class InvalidRoles:
        """当前契约测试使用的 InvalidRoles 替代实现。

        Attributes:
            descriptor: 插件身份、依赖和能力声明。
            stopped: 测试插件是否已经停止。
        """

        descriptor = PluginDescriptor(
            "example/invalid-roles",
            "1.0.0",
            provides=(CAP_EVENT_ROUTER, CAP_GRAPH_WORKER),
        )

        def __init__(self) -> None:
            """初始化实例及其依赖，建立当前对象独立维护的状态。"""

            self.stopped = False

        def setup(self, context) -> None:
            """在插件装配阶段登记声明的能力或贡献。

            Args:
                context: 当前调用的执行或插件上下文。
            """

            context.provide(CAP_EVENT_ROUTER, object())
            context.provide(CAP_GRAPH_WORKER, object())

        def start(self, context) -> None:
            """启动已经装配的组件或提交一次新的执行。

            Args:
                context: 当前调用的执行或插件上下文。
            """

            del context

        def stop(self, context) -> None:
            """停止插件并释放其拥有的资源。

            Args:
                context: 当前调用的执行或插件上下文。
            """

            del context
            self.stopped = True

    plugin = InvalidRoles()
    with pytest.raises(TypeError, match="router must implement RouterRole"):
        Runtime(plugins=(plugin,))

    assert plugin.stopped


class AnySelector:
    def select(self, ports, available, config):
        """仅根据端口名称和可用 token 数量选择本次输入组合。

        Args:
            ports: 保持声明顺序的输入端口名称。
            available: 各端口当前可消费的 token 数量。
            config: 当前具名策略的参数映射。

        Returns:
            符合当前可用条件的选择结果，没有可执行输入时不触发。
        """

        del config
        return next(((port,) for port in ports if available[port]), None)


class Source(Node):
    """当前契约测试使用的 Source 替代实现。

    Attributes:
        input_ports: 节点声明的输入端口及其类型。
        output_ports: 节点声明的输出端口及其类型。
    """

    input_ports = Ports(value=str)
    output_ports = Ports(value=str)

    def execute(self, inputs, context):
        """执行当前测试场景的节点行为，供外层契约断言检查。

        Args:
            inputs: 入口数据或按端口名称组织的输入映射。
            context: 当前调用的执行或插件上下文。

        Returns:
            测试节点或替代执行器产生的返回值。
        """

        del context
        return Output(inputs["value"], "value")


class Target(Node):
    """当前契约测试使用的 Target 替代实现。

    Attributes:
        input_ports: 节点声明的输入端口及其类型。
        output_ports: 节点声明的输出端口及其类型。
        input_policy: 仅依据端口和 token 数量生效的输入策略。
    """

    input_ports = Ports(value=str)
    output_ports = Ports(value=str)
    input_policy = PolicyRef("example.plugin/any")

    def execute(self, inputs, context):
        """执行当前测试场景的节点行为，供外层契约断言检查。

        Args:
            inputs: 入口数据或按端口名称组织的输入映射。
            context: 当前调用的执行或插件上下文。

        Returns:
            测试节点或替代执行器产生的返回值。
        """

        del context
        return Output(inputs["value"], "value")


def test_runtime_installs_extension_contributions_before_graph_freeze() -> None:
    """验证 Runtime 在 Graph 冻结前安装扩展贡献。"""

    plugin = ContributionPlugin(
        "example/plugin",
        selectors={"example.plugin/any": AnySelector()},
        hooks={
            "example.plugin/uppercase": NodeHookContribution(
                lambda call, outputs: tuple(
                    Output(item.value.upper(), item.port) for item in outputs
                ),
                phase="exit",
                graph="work.graph",
                node="target",
            )
        },
    )
    graph = (
        Graph(entrypoint="source")
        .add(source=Source(), target=Target())
        .connect("source", "target", source_port="value", target_port="value")
    )

    with Runtime(plugins=(plugin,)) as runtime:
        runtime.register("work.graph", graph)
        assert runtime.run("work.graph", "hello") == (Output("HELLO", "value"),)
        assert runtime.plugin_host is not None
        assert {item.id for item in runtime.plugin_host.descriptors} == {
            "bricks.core/local-runtime",
            "example/plugin",
        }


def test_default_runtime_is_composed_by_the_builtin_plugin() -> None:
    """验证默认 Runtime 经由默认插件装配。"""

    with Runtime() as runtime:
        assert runtime.plugin_host is not None
        assert runtime.plugin_host.descriptors == (LocalRuntimePlugin.descriptor,)


def test_custom_infrastructure_uses_the_same_local_plugin_path() -> None:
    """验证自定义底层能力使用相同的默认插件装配路径。"""

    events = memory.EventBus()
    tasks = memory.TaskBackend()
    executor = Engine()
    local = LocalRuntimePlugin(events=events, tasks=tasks, executor=executor)

    with Runtime(plugins=(local,)) as runtime:
        assert runtime.plugin_host is not None
        assert runtime.plugin_host.require("bricks.runtime/event-bus") is events
        assert runtime.plugin_host.require("bricks.runtime/task-backend") is tasks
        assert runtime.plugin_host.require("bricks.runtime/graph-executor") is executor

    # 注入资源仍由调用方持有，与旧的角色构造器所有权约定一致。
    assert not events._closed
    assert not tasks._closed
    events.close()
    tasks.close()
    executor.close()


def test_runtime_fills_defaults_around_custom_event_bus_capability() -> None:
    """验证替换事件总线后其余能力仍由默认插件补齐。"""

    events: list[str] = []
    bus = memory.EventBus()

    class EventBusPlugin(RecordingPlugin):
        def __init__(self) -> None:
            """初始化实例及其依赖，建立当前对象独立维护的状态。"""

            super().__init__(
                "example/events",
                events,
                capability=CAP_EVENT_BUS,
            )

        def setup(self, context) -> None:
            """在插件装配阶段登记声明的能力或贡献。

            Args:
                context: 当前调用的执行或插件上下文。
            """

            self.events.append(f"setup:{self.descriptor.id}")
            context.provide(CAP_EVENT_BUS, bus)

        def stop(self, context) -> None:
            """停止插件并释放其拥有的资源。

            Args:
                context: 当前调用的执行或插件上下文。
            """

            super().stop(context)
            bus.close()

    with Runtime(plugins=(EventBusPlugin(),)) as runtime:
        assert runtime.plugin_host is not None
        assert runtime.plugin_host.require(CAP_EVENT_BUS) is bus


@pytest.mark.parametrize(
    ("capability", "component"),
    [
        (CAP_TASK_BACKEND, memory.TaskBackend()),
        (CAP_GRAPH_EXECUTOR, Engine()),
    ],
)
def test_runtime_fills_defaults_around_each_infrastructure_capability(
    capability: str,
    component: memory.EventBus | memory.TaskBackend | Engine,
) -> None:
    """验证独立替换各底层能力时仍能补齐默认装配。

    Args:
        capability: 需要提供、取得或移除的能力名称。
        component: 当前用例使用的 component 夹具或参数化输入。
    """

    class InfrastructurePlugin:
        """当前契约测试使用的 InfrastructurePlugin 替代实现。

        Attributes:
            descriptor: 插件身份、依赖和能力声明。
        """

        descriptor = PluginDescriptor(
            f"example/{capability.rsplit('/', 1)[-1]}",
            "1.0.0",
            provides=(capability,),
        )

        def setup(self, context) -> None:
            """在插件装配阶段登记声明的能力或贡献。

            Args:
                context: 当前调用的执行或插件上下文。
            """

            context.provide(capability, component)

        def start(self, context) -> None:
            """启动已经装配的组件或提交一次新的执行。

            Args:
                context: 当前调用的执行或插件上下文。
            """

            del context

        def stop(self, context) -> None:
            """停止插件并释放其拥有的资源。

            Args:
                context: 当前调用的执行或插件上下文。
            """

            del context
            component.close()

    with Runtime(plugins=(InfrastructurePlugin(),)) as runtime:
        assert runtime.plugin_host is not None
        assert runtime.plugin_host.require(capability) is component
