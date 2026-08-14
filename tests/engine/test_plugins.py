"""统一插件宿主和 Runtime 插件装配的契约测试。"""

from __future__ import annotations

import pytest

from bricks import Graph, InputPolicy, Node, Output, Ports, Runtime
from bricks.adapters import memory
from bricks.engine.executor import Engine
from bricks.engine.policies import PolicyRef
from bricks.plugins import (
    CAP_INPUT_SELECTOR,
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
        capability: str | None = None,
    ) -> None:
        provides = () if capability is None else (capability,)
        self.descriptor = PluginDescriptor(
            plugin_id, "1.0.0", requires=requires, provides=provides
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
