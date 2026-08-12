"""精简 Graph 定义的契约测试。"""

from __future__ import annotations

from collections.abc import Mapping

import pytest

from bricks import AsyncNode, ExecutionPlan, Graph, InputPolicy, Node, Output, Ports
from bricks.engine import Context, GraphFrozenError, GraphValidationError


class Source(Node):
    """产生一个字符串的零输入节点。"""

    input_ports = Ports()
    output_ports = Ports(value=str)
    input_policy = InputPolicy.ON_START

    def execute(self, inputs: Mapping[str, object], context: Context) -> Output:
        """产生固定字符串。

        参数：
            inputs: 空输入。
            context: 当前执行上下文。

        返回：
            固定字符串 Output。
        """

        del inputs, context
        return Output("value", port="value")


class Sink(Node):
    """接收一个 object 的终端节点。"""

    input_ports = Ports(value=object)
    output_ports = Ports()

    def execute(self, inputs, context: Context) -> None:
        """消费输入。

        参数：
            inputs: 当前字符串输入。
            context: 当前执行上下文。
        """

        del inputs, context


def test_graph_freezes_typed_dag() -> None:
    """派生类型 output 可以连接 object input。"""

    graph = (
        Graph(entrypoint="source")
        .add("source", Source())
        .add("sink", Sink())
        .connect("source", "sink", source_port="value", target_port="value")
        .freeze()
    )

    assert graph.frozen
    assert graph.entrypoint == "source"
    assert len(graph.edges) == 1


def test_graph_rejects_cycle() -> None:
    """图内环必须改用跨图 Event 表达。"""

    class Relay(Node):
        input_ports = Ports(value=str)
        output_ports = Ports(value=str)

        def execute(self, inputs, context: Context) -> Output:
            del context
            return Output(inputs["value"], port="value")

    graph = Graph(entrypoint="a").add("a", Relay()).add("b", Relay())
    graph.connect("a", "b", source_port="value", target_port="value")
    graph.connect("b", "a", source_port="value", target_port="value")

    with pytest.raises(GraphValidationError, match="acyclic"):
        graph.freeze()


def test_graph_rejects_unreachable_node() -> None:
    """冻结时拒绝入口无法到达的定义。"""

    graph = Graph(entrypoint="source")
    graph.add("source", Source()).add("sink", Sink())

    with pytest.raises(GraphValidationError, match="unreachable"):
        graph.freeze()


def test_graph_rejects_incompatible_ports() -> None:
    """宽类型 output 不能连接窄类型 input。"""

    class Wide(Source):
        output_ports = Ports(value=object)

    class Narrow(Sink):
        input_ports = Ports(value=str)

    graph = Graph(entrypoint="source")
    graph.add("source", Wide()).add("sink", Narrow())
    graph.connect("source", "sink", source_port="value", target_port="value")

    with pytest.raises(GraphValidationError, match="incompatible"):
        graph.freeze()


def test_graph_rejects_sync_async_mismatch() -> None:
    """Node 类目必须与 execute 风格一致。"""

    class BrokenAsync(AsyncNode):
        async def not_execute(self) -> None:
            """仅用于避免空类。"""

    # 抽象类本身无法实例化；用动态覆盖模拟错误声明。
    class Wrong(Node):
        async def execute(self, inputs, context):
            del inputs, context

    graph = Graph(entrypoint="wrong").add("wrong", Wrong())
    with pytest.raises(GraphValidationError, match="sync execute"):
        graph.freeze()


def test_frozen_graph_is_immutable() -> None:
    """冻结后禁止修改节点和连接。"""

    graph = Graph(entrypoint="source").add("source", Source()).freeze()

    with pytest.raises(GraphFrozenError):
        graph.add("another", Source())


def test_graph_builds_strict_execution_plan() -> None:
    """Plan 仅保留两端都被选中的原始 Edge，并冻结所属 Graph。"""

    graph = (
        Graph(entrypoint="source")
        .add("source", Source())
        .add("left", Sink())
        .add("right", Sink())
        .connect("source", "left", source_port="value", target_port="value")
        .connect("source", "right", source_port="value", target_port="value")
    )

    plan = graph.plan(include={"source", "left"})

    assert isinstance(plan, ExecutionPlan)
    assert graph.frozen
    assert plan.entrypoint == "source"
    assert plan.nodes == frozenset({"source", "left"})
    assert plan.edges == (graph.edges[0],)


def test_execution_plan_rejects_disconnected_selection() -> None:
    """严格 Plan 不会跨过未选中的中间 Node 自动补边。"""

    class Relay(Source):
        input_ports = Ports(value=str)
        input_policy = InputPolicy.ALL

        def execute(self, inputs, context: Context) -> Output:
            del context
            return Output(inputs["value"], "value")

    graph = (
        Graph(entrypoint="source")
        .add("source", Source())
        .add("relay", Relay())
        .add("sink", Sink())
        .connect("source", "relay", source_port="value", target_port="value")
        .connect("relay", "sink", source_port="value", target_port="value")
    )

    with pytest.raises(GraphValidationError, match="unreachable"):
        graph.plan(include={"source", "sink"})


def test_execution_plan_requires_all_join_inputs() -> None:
    """裁掉 ALL Node 的任一输入分支时在创建 Plan 阶段失败。"""

    class Split(Node):
        input_ports = Ports(value=int)
        output_ports = Ports(left=int, right=int)

        def execute(self, inputs, context: Context):
            del context
            return (
                Output(inputs["value"], "left"),
                Output(inputs["value"], "right"),
            )

    class Relay(Node):
        input_ports = Ports(value=int)
        output_ports = Ports(value=int)

        def execute(self, inputs, context: Context) -> Output:
            del context
            return Output(inputs["value"], "value")

    class Join(Node):
        input_ports = Ports(left=int, right=int)
        output_ports = Ports(total=int)
        input_policy = InputPolicy.ALL

        def execute(self, inputs, context: Context) -> Output:
            del context
            return Output(inputs["left"] + inputs["right"], "total")

    graph = (
        Graph(entrypoint="split")
        .add("split", Split())
        .add("left", Relay())
        .add("right", Relay())
        .add("join", Join())
        .connect("split", "left", source_port="left", target_port="value")
        .connect("split", "right", source_port="right", target_port="value")
        .connect("left", "join", source_port="value", target_port="left")
        .connect("right", "join", source_port="value", target_port="right")
    )
    with pytest.raises(GraphValidationError, match="ALL node.*right"):
        graph.plan(include={"split", "left", "join"})


def test_input_policy_any_uses_declaration_order() -> None:
    """多个就绪端口时，ANY 按声明顺序选择。"""

    policy = InputPolicy.ANY
    queues = {
        "left": [object()],
        "right": [object()],
        "cancel": [object()],
    }

    assert policy._select(("left", "right", "cancel"), queues) == ("left",)
