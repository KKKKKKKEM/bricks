"""统一插件宿主和 Runtime 插件装配的契约测试。"""

from __future__ import annotations

import pytest

from bricks import Graph, Node, Output, Ports, Runtime
from bricks.adapters import memory
from bricks.engine.executor import Engine
from bricks.engine.policies import PolicyRef
from bricks.plugins import (
    CAP_EVENT_BUS,
    CAP_EVENT_ROUTER,
    CAP_GRAPH_EXECUTOR,
    CAP_GRAPH_WORKER,
    CAP_TASK_BACKEND,
    ContributionPlugin,
    NodeHookContribution,
    PluginDescriptor,
    PluginHost,
)
from bricks.runtime import LocalRuntimePlugin


class RecordingPlugin:
    def __init__(
        self,
        plugin_id: str,
        events: list[str],
        *,
        requires: tuple[str, ...] = (),
        requires_capabilities: tuple[str, ...] = (),
        capability: str | None = None,
    ) -> None:
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
        self.events.append(f"setup:{self.descriptor.id}")
        if self.capability is not None:
            context.provide(self.capability, self.descriptor.id)

    def start(self, context) -> None:
        del context
        self.events.append(f"start:{self.descriptor.id}")

    def stop(self, context) -> None:
        del context
        self.events.append(f"stop:{self.descriptor.id}")


def test_host_orders_dependencies_and_stops_in_reverse() -> None:
    events: list[str] = []
    base = RecordingPlugin("example/base", events, capability="example/base-value")
    feature = RecordingPlugin(
        "example/feature", events, requires=("example/base",)
    )

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
    events: list[str] = []
    with pytest.raises(ValueError, match="missing required plugin"):
        PluginHost(
            (RecordingPlugin("example/feature", events, requires=("example/base",)),)
        )

    left = RecordingPlugin(
        "example/left", events, requires=("example/right",)
    )
    right = RecordingPlugin(
        "example/right", events, requires=("example/left",)
    )
    with pytest.raises(ValueError, match="cyclic plugin dependency"):
        PluginHost((left, right))

    first = RecordingPlugin("example/first", events, capability="example/value")
    second = RecordingPlugin("example/second", events, capability="example/value")
    with pytest.raises(ValueError, match="already provided"):
        PluginHost((first, second)).start()


def test_host_rejects_incompatible_plugin_api() -> None:
    class FuturePlugin:
        descriptor = PluginDescriptor(
            "example/future", "1.0.0", api_version="2"
        )

        def setup(self, context):
            del context

        def start(self, context):
            del context

        def stop(self, context):
            del context

    with pytest.raises(ValueError, match="requires API 2"):
        PluginHost((FuturePlugin(),))


def test_setup_failure_rolls_back_configured_plugins() -> None:
    events: list[str] = []
    base = RecordingPlugin("example/base", events)

    class Broken(RecordingPlugin):
        def setup(self, context) -> None:
            super().setup(context)
            raise RuntimeError("setup failed")

    with pytest.raises(RuntimeError, match="setup failed"):
        PluginHost((base, Broken("example/broken", events))).start()

    assert events[-2:] == ["stop:example/broken", "stop:example/base"]


def test_start_failure_rolls_back_plugins_in_reverse_order() -> None:
    events: list[str] = []

    class Broken(RecordingPlugin):
        def start(self, context) -> None:
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
    class Missing(RecordingPlugin):
        def setup(self, context) -> None:
            del context

    plugin = Missing("example/missing", [], capability="example/value")

    with pytest.raises(RuntimeError, match="did not provide declared capabilities"):
        PluginHost((plugin,)).start()


def test_stop_failure_does_not_skip_remaining_plugins() -> None:
    events: list[str] = []

    class BrokenStop(RecordingPlugin):
        def stop(self, context) -> None:
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
    class Contributions:
        descriptor = PluginDescriptor(
            "example/contributions",
            "1.0.0",
            provides=("example/items",),
        )

        def setup(self, context) -> None:
            context.contribute("example/items", "example/item", object())
            with pytest.raises(ValueError, match="already exists"):
                context.contribute("example/items", "example/item", object())

        def start(self, context) -> None:
            with pytest.raises(RuntimeError, match="frozen"):
                context.contribute("example/items", "example/late", object())

        def stop(self, context) -> None:
            del context

    PluginHost((Contributions(),)).start().close()


def test_runtime_role_validation_failure_closes_started_plugin_host() -> None:
    class InvalidRoles:
        descriptor = PluginDescriptor(
            "example/invalid-roles",
            "1.0.0",
            provides=(CAP_EVENT_ROUTER, CAP_GRAPH_WORKER),
        )

        def __init__(self) -> None:
            self.stopped = False

        def setup(self, context) -> None:
            context.provide(CAP_EVENT_ROUTER, object())
            context.provide(CAP_GRAPH_WORKER, object())

        def start(self, context) -> None:
            del context

        def stop(self, context) -> None:
            del context
            self.stopped = True

    plugin = InvalidRoles()
    with pytest.raises(TypeError, match="router must be an EventRouter"):
        Runtime(plugins=(plugin,))

    assert plugin.stopped


class AnySelector:
    def select(self, ports, available, config):
        del config
        return next(((port,) for port in ports if available[port]), None)


class Source(Node):
    input_ports = Ports(value=str)
    output_ports = Ports(value=str)

    def execute(self, inputs, context):
        del context
        return Output(inputs["value"], "value")


class Target(Node):
    input_ports = Ports(value=str)
    output_ports = Ports(value=str)
    input_policy = PolicyRef("example.plugin/any")

    def execute(self, inputs, context):
        del context
        return Output(inputs["value"], "value")


def test_runtime_installs_extension_contributions_before_graph_freeze() -> None:
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
    with Runtime() as runtime:
        assert runtime.plugin_host is not None
        assert runtime.plugin_host.descriptors == (LocalRuntimePlugin.descriptor,)


def test_custom_infrastructure_uses_the_same_local_plugin_path() -> None:
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
    events: list[str] = []
    bus = memory.EventBus()

    class EventBusPlugin(RecordingPlugin):
        def __init__(self) -> None:
            super().__init__(
                "example/events",
                events,
                capability=CAP_EVENT_BUS,
            )

        def setup(self, context) -> None:
            self.events.append(f"setup:{self.descriptor.id}")
            context.provide(CAP_EVENT_BUS, bus)

        def stop(self, context) -> None:
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
    component: object,
) -> None:
    class InfrastructurePlugin:
        descriptor = PluginDescriptor(
            f"example/{capability.rsplit('/', 1)[-1]}",
            "1.0.0",
            provides=(capability,),
        )

        def setup(self, context) -> None:
            context.provide(capability, component)

        def start(self, context) -> None:
            del context

        def stop(self, context) -> None:
            del context
            component.close()

    with Runtime(plugins=(InfrastructurePlugin(),)) as runtime:
        assert runtime.plugin_host is not None
        assert runtime.plugin_host.require(capability) is component
