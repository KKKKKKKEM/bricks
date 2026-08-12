"""Event 驱动 Runtime 的执行契约测试。"""

from __future__ import annotations

import asyncio
from threading import Lock

import pytest

from bricks import (
    AsyncNode,
    Graph,
    InputPolicy,
    Node,
    Output,
    Ports,
    Runtime,
)
from bricks.engine import (
    Context,
    IncompleteInputsError,
    InvalidOutputError,
    PortValueTypeError,
)


class Split(Node):
    """产生 join 所需的两个图内端口。"""

    input_ports = Ports(value=int)
    output_ports = Ports(left=int, right=int)

    def execute(self, inputs, context: Context):
        """把整数复制到两个端口。

        参数：
            inputs: 当前整数输入。
            context: 当前执行上下文。

        返回：
            两个命名 Output。
        """

        del context
        value = inputs["value"]
        return (Output(value, "left"), Output(value + 1, "right"))


class Join(Node):
    """等待两个输入端口后求和。"""

    input_ports = Ports(left=int, right=int)
    output_ports = Ports(total=int)
    input_policy = InputPolicy.ALL

    def execute(self, inputs, context: Context) -> Output:
        """求和并返回终端 Output。

        参数：
            inputs: 同时包含 left 和 right。
            context: 当前执行上下文。

        返回：
            求和结果。
        """

        del context
        return Output(inputs["left"] + inputs["right"], "total")


def join_graph() -> Graph:
    """构建用于测试图内 Edge 和 InputPolicy 的 DAG。

    返回：
        已冻结的两节点 Graph。
    """

    return (
        Graph(entrypoint="split")
        .add("split", Split())
        .add("join", Join())
        .connect("split", "join", source_port="left", target_port="left")
        .connect("split", "join", source_port="right", target_port="right")
        .freeze()
    )


def test_runtime_executes_graph_internal_dataflow() -> None:
    """Runtime.run 返回未连接 Edge 的终端 Output。"""

    with Runtime() as runtime:
        runtime.register("join.graph", join_graph())
        outputs = runtime.run("join.graph", 2)

    assert outputs == (Output(5, "total"),)


def test_runtime_executes_different_plans_from_one_graph() -> None:
    """同一冻结 Graph 可按每次调用选择不同分支，未选 Node 不执行。"""

    calls: list[str] = []

    class SourceNode(Node):
        input_ports = Ports(value=int)
        output_ports = Ports(value=int)

        def execute(self, inputs, context: Context) -> Output:
            del context
            calls.append("source")
            return Output(inputs["value"], "value")

    class Branch(Node):
        input_ports = Ports(value=int)
        output_ports = Ports(result=str)

        def __init__(self, name: str) -> None:
            self.name = name

        def execute(self, inputs, context: Context) -> Output:
            del context
            calls.append(self.name)
            return Output(f"{self.name}:{inputs['value']}", "result")

    graph = (
        Graph(entrypoint="source")
        .add("source", SourceNode())
        .add("fast", Branch("fast"))
        .add("full", Branch("full"))
        .connect("source", "fast", source_port="value", target_port="value")
        .connect("source", "full", source_port="value", target_port="value")
    )
    fast = graph.plan(include={"source", "fast"})
    full = graph.plan(include={"source", "full"})

    with Runtime() as runtime:
        runtime.register("work.graph", graph)
        assert runtime.run("work.graph", 3, plan=fast) == (
            Output("fast:3", "result"),
        )
        assert runtime.run("work.graph", 4, plan=full) == (
            Output("full:4", "result"),
        )

    assert calls == ["source", "fast", "source", "full"]


def test_runtime_rejects_plan_from_another_graph() -> None:
    """Plan 只能用于创建它的同一个 Graph 实例。"""

    class Start(Node):
        input_ports = Ports()
        output_ports = Ports()
        input_policy = InputPolicy.ON_START

        def execute(self, inputs, context: Context) -> None:
            del inputs, context

    first = Graph(entrypoint="source").add("source", Start()).freeze()
    second = Graph(entrypoint="source").add("source", Start()).freeze()
    plan = first.plan(include={"source"})

    with Runtime() as runtime:
        runtime.register("second", second)
        with pytest.raises(ValueError, match="different Graph"):
            runtime.run("second", plan=plan)


def test_single_port_receives_complete_mapping_payload() -> None:
    """单端口的 Mapping payload 不按多端口输入映射解释。"""

    class MappingConsumer(Node):
        input_ports = Ports(task=dict)
        output_ports = Ports(result=dict)

        def execute(self, inputs, context: Context) -> Output:
            del context
            return Output(inputs["task"], "result")

    graph = Graph(entrypoint="consume").add("consume", MappingConsumer())
    payload = {"task": 1}
    with Runtime() as runtime:
        runtime.register("mapping.graph", graph)
        outputs = runtime.run("mapping.graph", payload)

    assert outputs == (Output(payload, "result"),)


@pytest.mark.parametrize("node", ["sync", "async"])
def test_runtime_classifies_invalid_node_results(node: str) -> None:
    """同步和异步 Node 的非法返回值都使用公共输出协议错误。"""

    class InvalidSync(Node):
        input_ports = Ports()
        output_ports = Ports()
        input_policy = InputPolicy.ON_START

        def execute(self, inputs, context: Context):
            del inputs, context
            return 1

    class InvalidAsync(AsyncNode):
        input_ports = Ports()
        output_ports = Ports()
        input_policy = InputPolicy.ON_START

        async def execute(self, inputs, context: Context):
            del inputs, context
            return ["not-an-output"]

    invalid = InvalidSync() if node == "sync" else InvalidAsync()
    graph = Graph(entrypoint="invalid").add("invalid", invalid)
    with Runtime() as runtime:
        runtime.register("invalid.graph", graph)
        with pytest.raises(InvalidOutputError) as raised:
            runtime.run("invalid.graph")

    assert raised.value.graph == "invalid.graph"
    assert raised.value.node == "invalid"


def test_idle_runtime_supports_zero_timeout_polling() -> None:
    """零超时是即时空闲检查，空闲 Runtime 应立即成功。"""

    with Runtime() as runtime:
        runtime.wait_idle(0)


def test_context_emit_routes_to_another_graph() -> None:
    """context.emit 通过 route 启动另一张 Graph。"""

    received: list[str] = []

    class Producer(Node):
        input_ports = Ports(value=str)
        output_ports = Ports()

        def execute(self, inputs, context: Context) -> None:
            context.emit("value.created", inputs["value"])

    class Consumer(Node):
        input_ports = Ports(value=str)
        output_ports = Ports()

        def execute(self, inputs, context: Context) -> None:
            del context
            received.append(inputs["value"])

    producer = Graph(entrypoint="produce").add("produce", Producer())
    consumer = Graph(entrypoint="consume").add("consume", Consumer())

    with Runtime() as runtime:
        runtime.register("producer.graph", producer)
        runtime.register("consumer.graph", consumer)
        runtime.on(
            "value.created",
            graph="consumer.graph",
            queue="values",
            concurrency=2,
        )
        runtime.run("producer.graph", "hello")
        runtime.wait_idle()

    assert received == ["hello"]


def test_runtime_keeps_observe_separate_from_combined_on() -> None:
    """observe 只观察 Event，on 组合 route 与 consume。"""

    observed = []
    received: list[str] = []

    class Consumer(Node):
        input_ports = Ports(value=str)
        output_ports = Ports()

        def execute(self, inputs, context: Context) -> None:
            del context
            received.append(inputs["value"])

    graph = Graph(entrypoint="consume").add("consume", Consumer())
    with Runtime() as runtime:
        runtime.register("consumer.graph", graph)
        runtime.observe("value.observed", observed.append)
        runtime.on(
            "value.routed",
            graph="consumer.graph",
            queue="values",
        )
        runtime.emit("value.observed", "seen")
        runtime.emit("value.routed", "handled")
        runtime.wait_idle()

    assert [event.payload for event in observed] == ["seen"]
    assert received == ["handled"]


def test_runtime_on_forwards_subscription() -> None:
    """组合入口保留 route 的显式 subscription 身份。"""

    class Consumer(Node):
        input_ports = Ports(value=str)
        output_ports = Ports()

        def execute(self, inputs, context: Context) -> None:
            del inputs, context

    graph = Graph(entrypoint="consume").add(consume=Consumer())
    with Runtime() as runtime:
        runtime.register("consumer.graph", graph)
        runtime.on(
            "value.routed",
            graph="consumer.graph",
            queue="values",
            subscription="consumer-values",
        )


def test_runtime_on_does_not_leave_route_when_consumer_setup_fails() -> None:
    """消费配置失败时，同一 route 仍可在修正参数后正常注册。"""

    class Consumer(Node):
        input_ports = Ports(value=str)
        output_ports = Ports()

        def execute(self, inputs, context: Context) -> None:
            del inputs, context

    with Runtime() as runtime:
        runtime.register(
            "consumer.graph",
            Graph(entrypoint="consume").add(consume=Consumer()),
        )
        with pytest.raises(ValueError, match="at least 1"):
            runtime.on(
                "value.routed",
                graph="consumer.graph",
                queue="values",
                concurrency=0,
            )

        runtime.on(
            "value.routed",
            graph="consumer.graph",
            queue="values",
        )


def test_event_is_committed_even_if_source_node_later_fails() -> None:
    """emit 成功后的事件不因当前 Node 后续失败而撤回。"""

    class Broken(Node):
        input_ports = Ports(value=str)
        output_ports = Ports()

        def execute(self, inputs, context: Context) -> None:
            context.emit("value.committed", inputs["value"])
            raise ValueError("boom")

    graph = Graph(entrypoint="broken").add("broken", Broken())
    seen = []
    with Runtime() as runtime:
        runtime.register("broken.graph", graph)
        runtime.observe("value.committed", seen.append)
        with pytest.raises(Exception, match="boom"):
            runtime.run("broken.graph", "kept")

    assert [event.payload for event in seen] == ["kept"]


def test_runtime_dispatches_each_event_without_domain_deduplication() -> None:
    """Runtime 不判断两个领域 payload 是否代表相同工作。"""

    received: list[str] = []

    class Consumer(Node):
        input_ports = Ports(value=str)
        output_ports = Ports()

        def execute(self, inputs, context: Context) -> None:
            del context
            received.append(inputs["value"])

    graph = Graph(entrypoint="consume").add("consume", Consumer())
    with Runtime() as runtime:
        runtime.register("consumer.graph", graph)
        runtime.on("value", graph="consumer.graph", queue="values")
        runtime.emit("value", "Same")
        runtime.emit("value", "same")
        runtime.wait_idle()

    assert sorted(received) == ["Same", "same"]


def test_queue_concurrency_limits_graph_executions() -> None:
    """命名执行通道的 concurrency 限制同时运行的 Graph 数量。"""

    active = 0
    maximum = 0
    lock = Lock()

    class Slow(AsyncNode):
        input_ports = Ports(value=int)
        output_ports = Ports()

        async def execute(self, inputs, context: Context) -> None:
            nonlocal active, maximum
            del inputs, context
            with lock:
                active += 1
                maximum = max(maximum, active)
            try:
                await asyncio.sleep(0.02)
            finally:
                with lock:
                    active -= 1

    graph = Graph(entrypoint="slow").add("slow", Slow())
    with Runtime() as runtime:
        runtime.register("slow.graph", graph)
        runtime.on(
            "slow.requested",
            graph="slow.graph",
            queue="slow-tasks",
            concurrency=3,
        )
        for value in range(9):
            runtime.emit("slow.requested", value)
        runtime.wait_idle()

    assert maximum == 3


def test_event_can_drive_a_chain_of_graphs() -> None:
    """Event 输出可以继续驱动下一张 Graph。"""

    received: list[str] = []

    class Relay(Node):
        input_ports = Ports(value=str)
        output_ports = Ports()

        def execute(self, inputs, context: Context) -> None:
            context.emit("relayed", inputs["value"])

    class Sink(Node):
        input_ports = Ports(value=str)
        output_ports = Ports()

        def execute(self, inputs, context: Context) -> None:
            del context
            received.append(inputs["value"])

    graph = Graph(entrypoint="relay").add("relay", Relay())
    sink = Graph(entrypoint="sink").add("sink", Sink())
    with Runtime() as runtime:
        runtime.register("relay.graph", graph)
        runtime.register("sink.graph", sink)
        runtime.on("root", graph="relay.graph", queue="relay-tasks")
        runtime.on("relayed", graph="sink.graph", queue="sink-tasks")
        runtime.emit("root", "value")
        runtime.wait_idle()

    assert received == ["value"]


def test_runtime_validates_entry_value_type() -> None:
    """入口 payload 必须满足 Ports 声明。"""

    with Runtime() as runtime:
        runtime.register("join.graph", join_graph())
        with pytest.raises(PortValueTypeError):
            runtime.run("join.graph", "not-an-int")


def test_runtime_reports_incomplete_join() -> None:
    """图停止时残留的半组输入必须显式失败。"""

    class LeftOnly(Split):
        def execute(self, inputs, context: Context):
            del context
            return Output(inputs["value"], "left")

    graph = (
        Graph(entrypoint="split")
        .add("split", LeftOnly())
        .add("join", Join())
        .connect("split", "join", source_port="left", target_port="left")
    )
    # Join.right 仍由声明策略覆盖；没有 Edge 不影响静态合法性。
    graph.freeze()
    with Runtime() as runtime:
        runtime.register("incomplete.graph", graph)
        with pytest.raises(IncompleteInputsError):
            runtime.run("incomplete.graph", 1)
