"""精简 Graph 定义的契约测试。"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, cast

import pytest

from bricks import (
    AsyncNode,
    Context,
    ExecutionPlan,
    Graph,
    InputPolicy,
    Node,
    Output,
    Ports,
    Runtime,
)
from bricks.engine.errors import GraphFrozenError, GraphValidationError


class Source(Node):
    """产生一个字符串的零输入节点。

    Attributes:
        input_ports: 节点声明的输入端口及其类型。
        output_ports: 节点声明的输出端口及其类型。
        input_policy: 仅依据端口和 token 数量生效的输入策略。
    """

    input_ports = Ports()
    output_ports = Ports(value=str)
    input_policy = InputPolicy.ON_START

    def execute(self, inputs: Mapping[str, object], context: Context) -> Output:
        """产生固定字符串。

        Args:
            inputs: 空输入。
            context: 当前执行上下文。

        Returns:
            固定字符串 Output。
        """

        del inputs, context
        return Output("value", port="value")


class Sink(Node):
    """接收一个 object 的终端节点。

    Attributes:
        input_ports: 节点声明的输入端口及其类型。
        output_ports: 节点声明的输出端口及其类型。
    """

    input_ports = Ports(value=object)
    output_ports = Ports()

    def execute(self, inputs, context: Context) -> None:
        """消费输入。

        Args:
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


def test_graph_rejects_all_node_without_every_required_incoming_port() -> None:
    """ALL Node 的必需端口没有任何入边时应在冻结阶段失败。"""

    class Join(Node):
        """当前契约测试使用的 Join 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
        """

        input_ports = Ports(left=str, right=str)
        output_ports = Ports()

        def execute(self, inputs, context: Context) -> None:
            """执行当前测试场景的节点行为，供外层契约断言检查。

            Args:
                inputs: 入口数据或按端口名称组织的输入映射。
                context: 当前调用的执行或插件上下文。
            """

            del inputs, context

    graph = (
        Graph(entrypoint="source")
        .add(source=Source(), join=Join())
        .connect("source", "join", source_port="value", target_port="left")
    )

    with pytest.raises(GraphValidationError, match="join.*right"):
        graph.freeze()


def test_graph_adds_keyword_node_bindings() -> None:
    """关键字名称直接作为 Graph 内的 Node ID。"""

    source = Source()
    sink = Sink()
    graph = Graph(entrypoint="source").add(source=source, sink=sink)

    assert graph.nodes == {"source": source, "sink": sink}


def test_graph_keyword_add_is_atomic() -> None:
    """批量绑定包含非法 Node 时不留下部分结果。"""

    graph = Graph(entrypoint="source")

    with pytest.raises(TypeError, match="sink.*Node"):
        graph.add(source=Source(), sink=cast(Any, object()))

    assert graph.nodes == {}


def test_graph_add_rejects_mixed_forms() -> None:
    """单 Node 位置参数与关键字批量形式不能混用。"""

    with pytest.raises(TypeError, match="either"):
        cast(Any, Graph().add)("source", Source(), sink=Sink())


def test_graph_accepts_cycle() -> None:
    """普通 typed Edge 可以组成环。"""

    class Relay(Node):
        """当前契约测试使用的 Relay 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
        """

        input_ports = Ports(value=str)
        output_ports = Ports(value=str)

        def execute(self, inputs, context: Context) -> Output:
            """执行当前测试场景的节点行为，供外层契约断言检查。

            Args:
                inputs: 入口数据或按端口名称组织的输入映射。
                context: 当前调用的执行或插件上下文。

            Returns:
                测试节点或替代执行器产生的返回值。
            """

            del context
            return Output(inputs["value"], port="value")

    graph = Graph(entrypoint="a").add("a", Relay()).add("b", Relay())
    graph.connect("a", "b", source_port="value", target_port="value")
    graph.connect("b", "a", source_port="value", target_port="value")

    assert graph.freeze().frozen
    assert graph.edges[1].target == "a"


def test_graph_rejects_unreachable_node() -> None:
    """冻结时拒绝入口无法到达的定义。"""

    graph = Graph(entrypoint="source")
    graph.add("source", Source()).add("sink", Sink())

    with pytest.raises(GraphValidationError, match="unreachable"):
        graph.freeze()


def test_graph_rejects_incompatible_ports() -> None:
    """宽类型 output 不能连接窄类型 input。"""

    class Wide(Source):
        """当前契约测试使用的 Wide 替代实现。

        Attributes:
            output_ports: 节点声明的输出端口及其类型。
        """

        output_ports = Ports(value=object)

    class Narrow(Sink):
        """当前契约测试使用的 Narrow 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
        """

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
            """执行当前测试场景的节点行为，供外层契约断言检查。

            Args:
                inputs: 入口数据或按端口名称组织的输入映射。
                context: 当前调用的执行或插件上下文。
            """

            del inputs, context

    graph = Graph(entrypoint="wrong").add("wrong", Wrong())
    with pytest.raises(GraphValidationError, match="sync execute"):
        graph.freeze()


@pytest.mark.parametrize("timeout", [0, -1, float("inf"), float("nan"), True, "1"])
def test_graph_rejects_invalid_node_timeout(timeout: object) -> None:
    """Node timeout 必须是 None 或有限正数。

    Args:
        timeout: 等待或执行时限，单位秒；None 表示不设置时限。
    """

    source = Source()
    source.timeout = timeout  # type: ignore[assignment]
    graph = Graph(entrypoint="source").add(source=source)

    with pytest.raises(GraphValidationError, match="source.*timeout"):
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
        """当前契约测试使用的 Relay 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            input_policy: 仅依据端口和 token 数量生效的输入策略。
        """

        input_ports = Ports(value=str)
        input_policy = InputPolicy.ALL

        def execute(self, inputs, context: Context) -> Output:
            """执行当前测试场景的节点行为，供外层契约断言检查。

            Args:
                inputs: 入口数据或按端口名称组织的输入映射。
                context: 当前调用的执行或插件上下文。

            Returns:
                测试节点或替代执行器产生的返回值。
            """

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
        """将同一输入分发到两条计算分支的示例节点。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
        """

        input_ports = Ports(value=int)
        output_ports = Ports(left=int, right=int)

        def execute(self, inputs, context: Context):
            """执行当前测试场景的节点行为，供外层契约断言检查。

            Args:
                inputs: 入口数据或按端口名称组织的输入映射。
                context: 当前调用的执行或插件上下文。

            Returns:
                测试节点或替代执行器产生的返回值。
            """

            del context
            return (
                Output(inputs["value"], "left"),
                Output(inputs["value"], "right"),
            )

    class Relay(Node):
        """当前契约测试使用的 Relay 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
        """

        input_ports = Ports(value=int)
        output_ports = Ports(value=int)

        def execute(self, inputs, context: Context) -> Output:
            """执行当前测试场景的节点行为，供外层契约断言检查。

            Args:
                inputs: 入口数据或按端口名称组织的输入映射。
                context: 当前调用的执行或插件上下文。

            Returns:
                测试节点或替代执行器产生的返回值。
            """

            del context
            return Output(inputs["value"], "value")

    class Join(Node):
        """当前契约测试使用的 Join 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
            input_policy: 仅依据端口和 token 数量生效的输入策略。
        """

        input_ports = Ports(left=int, right=int)
        output_ports = Ports(total=int)
        input_policy = InputPolicy.ALL

        def execute(self, inputs, context: Context) -> Output:
            """执行当前测试场景的节点行为，供外层契约断言检查。

            Args:
                inputs: 入口数据或按端口名称组织的输入映射。
                context: 当前调用的执行或插件上下文。

            Returns:
                测试节点或替代执行器产生的返回值。
            """

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

    class Select(Node):
        """当前契约测试使用的 Select 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            input_policy: 仅依据端口和 token 数量生效的输入策略。
        """

        input_ports = Ports(left=int, right=int, cancel=int)
        input_policy = InputPolicy.ANY

        def execute(self, inputs, context):
            """执行当前测试场景的节点行为，供外层契约断言检查。

            Args:
                inputs: 入口数据或按端口名称组织的输入映射。
                context: 当前调用的执行或插件上下文。

            Returns:
                测试节点或替代执行器产生的返回值。
            """

            return Output(next(iter(inputs)))

    with Runtime() as runtime:
        runtime.register("select", Graph(entrypoint="node").add(node=Select()))
        outputs = runtime.run("select", {"cancel": 3, "right": 2, "left": 1})
    assert outputs == (Output("left"), Output("right"), Output("cancel"))
