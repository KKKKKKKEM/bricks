from types import MappingProxyType

import pytest

from bricks.engine import (
    Edge,
    Endpoint,
    Flow,
    Graph,
    GraphDefinitionError,
    GraphFrozenError,
    GraphValidationError,
    InputPolicy,
    Node,
    NodeInputs,
    NodeResult,
    Ports,
    UnknownFlowError,
    UnknownNodeError,
)


class IdentityNode(Node):
    async def execute(self, inputs: NodeInputs, context):
        """原样返回输入。

        参数：
            inputs: 测试传入的节点输入。
            context: 本次节点调用的执行上下文。

        返回：
            包含原输入的 NodeResult。
        """

        return NodeResult.one(inputs.single())


class BaseValue:
    """Graph 类型兼容测试使用的基础类型。"""


class ChildValue(BaseValue):
    """Graph 类型兼容测试使用的派生类型。"""


class OtherValue:
    """Graph 类型兼容测试使用的不相关类型。"""


class ProducerNode(IdentityNode):
    """产生 ChildValue 的测试 Node。"""

    output_ports = Ports(result=ChildValue)


class ConsumerNode(IdentityNode):
    """接收 BaseValue 并产生 BaseValue 的测试 Node。"""

    input_ports = Ports(value=BaseValue)
    output_ports = Ports(result=BaseValue)


class OtherConsumerNode(IdentityNode):
    """只接收 OtherValue 的测试 Node。"""

    input_ports = Ports(value=OtherValue)
    output_ports = Ports(result=OtherValue)


def build_shared_graph() -> Graph:
    """构建包含共享路径和两个 Flow 的测试 Graph。

    返回：
        尚未冻结的测试 Graph。
    """

    node = IdentityNode()
    graph = Graph("etl")
    graph.add_node("extract", node)
    graph.add_node("transform", node)
    graph.add_node("validate", node)
    graph.add_node("load", node)
    graph.connect("extract", "transform")
    graph.connect("transform", "validate")
    graph.connect("validate", "load")
    graph.add_flow(
        Flow(
            name="full",
            entrypoint="extract",
            endpoints=frozenset({Endpoint("load")}),
        )
    )
    graph.add_flow(
        Flow(
            name="transform-only",
            entrypoint="transform",
            endpoints=frozenset({Endpoint("validate")}),
        )
    )
    return graph


def test_graph_supports_shared_routes_with_flow_specific_endpoints() -> None:
    """验证多个 Flow 可以共享路径并在不同位置终止。"""

    graph = build_shared_graph().freeze()

    assert graph.is_endpoint("transform-only", "validate")
    assert not graph.is_endpoint("full", "validate")
    assert graph.is_endpoint("full", "load")
    assert graph.outgoing("validate") == (
        Edge("validate", "load"),
    )


def test_graph_accepts_compatible_typed_port_connections() -> None:
    """验证 Graph 允许派生输出连接基础类型输入。"""

    graph = Graph("typed")
    graph.add_node("producer", ProducerNode())
    graph.add_node("consumer", ConsumerNode())
    graph.connect(
        "producer",
        "consumer",
        source_port="result",
        target_port="value",
    )
    graph.add_flow(
        Flow(
            "default",
            "producer",
            frozenset({Endpoint("consumer", "result")}),
        )
    )

    graph.freeze()

    assert graph.outgoing("producer", "result") == (
        Edge("producer", "consumer", "result", "value"),
    )


def test_freeze_rejects_incompatible_typed_port_connections() -> None:
    """验证 Graph 拒绝类型不兼容的 Edge。"""

    graph = Graph("typed")
    graph.add_node("producer", ProducerNode())
    graph.add_node("consumer", OtherConsumerNode())
    graph.connect(
        "producer",
        "consumer",
        source_port="result",
        target_port="value",
    )
    graph.add_flow(
        Flow(
            "default",
            "producer",
            frozenset({Endpoint("consumer", "result")}),
        )
    )

    with pytest.raises(GraphValidationError) as raised:
        graph.freeze()

    assert any("produces ChildValue" in issue for issue in raised.value.issues)


def test_freeze_rejects_undeclared_edge_and_endpoint_ports() -> None:
    """验证 Graph 拒绝 Edge 或 Endpoint 引用未声明端口。"""

    graph = Graph("ports")
    graph.add_node("producer", ProducerNode())
    graph.add_node("consumer", ConsumerNode())
    graph.connect(
        "producer",
        "consumer",
        source_port="missing-output",
        target_port="missing-input",
    )
    graph.add_flow(
        Flow(
            "default",
            "producer",
            frozenset({Endpoint("consumer", "missing-result")}),
        )
    )

    with pytest.raises(GraphValidationError) as raised:
        graph.freeze()

    assert any("source port producer.missing-output" in issue for issue in raised.value.issues)
    assert any("target port consumer.missing-input" in issue for issue in raised.value.issues)
    assert any("endpoint port consumer.missing-result" in issue for issue in raised.value.issues)


def test_freeze_rejects_input_policy_that_does_not_cover_ports() -> None:
    """验证 Graph 拒绝没有覆盖全部声明端口的输入策略。"""

    class InvalidPolicyNode(IdentityNode):
        """声明了未被策略消费端口的测试 Node。"""

        input_ports = Ports(left=int, right=int)
        input_policy = InputPolicy.require("left")

    graph = Graph("policy")
    graph.add_node("node", InvalidPolicyNode())
    graph.add_flow(
        Flow("default", "node", frozenset({Endpoint("node")}))
    )

    with pytest.raises(GraphValidationError) as raised:
        graph.freeze()

    assert any("does not cover ports" in issue for issue in raised.value.issues)


def test_zero_input_node_requires_on_start_policy() -> None:
    """验证零输入 Source Node 只能使用 on_start 策略。"""

    class InvalidSourceNode(IdentityNode):
        """错误使用默认 all 策略的零输入 Node。"""

        input_ports = Ports()

    graph = Graph("source")
    graph.add_node("source", InvalidSourceNode())
    graph.add_flow(
        Flow("default", "source", frozenset({Endpoint("source")}))
    )

    with pytest.raises(GraphValidationError) as raised:
        graph.freeze()

    assert any("requires at least one port" in issue for issue in raised.value.issues)


def test_zero_input_node_accepts_on_start_policy() -> None:
    """验证零输入 Source Node 可以使用 on_start 策略。"""

    class SourceNode(IdentityNode):
        """使用合法 on_start 策略的零输入 Node。"""

        input_ports = Ports()
        input_policy = InputPolicy.on_start()

    graph = Graph("source")
    graph.add_node("source", SourceNode())
    graph.add_flow(
        Flow("default", "source", frozenset({Endpoint("source")}))
    )

    graph.freeze()

    assert graph.frozen


def test_same_node_behavior_can_be_bound_to_multiple_positions() -> None:
    """验证同一 Node 行为可以绑定到多个 Graph 位置。"""

    node = IdentityNode()
    graph = Graph("reuse")
    graph.add_node("first", node)
    graph.add_node("second", node)
    graph.connect("first", "second")
    graph.add_flow(
        Flow("default", "first", frozenset({Endpoint("second")}))
    )
    graph.freeze()

    assert graph.node("first") is node
    assert graph.node("second") is node


def test_freeze_is_idempotent_and_prevents_mutation() -> None:
    """验证 freeze() 幂等且冻结后禁止修改。"""

    graph = build_shared_graph()

    assert graph.freeze() is graph
    assert graph.freeze() is graph
    assert graph.frozen

    with pytest.raises(GraphFrozenError):
        graph.add_node("another", IdentityNode())
    with pytest.raises(GraphFrozenError):
        graph.connect("load", "extract")
    with pytest.raises(GraphFrozenError):
        graph.add_flow(
            Flow("another", "load", frozenset({Endpoint("load")}))
        )


def test_graph_exposes_read_only_definition_views() -> None:
    """验证 Graph 只暴露只读定义视图。"""

    graph = build_shared_graph().freeze()

    assert isinstance(graph.nodes, MappingProxyType)
    assert isinstance(graph.flows, MappingProxyType)
    with pytest.raises(TypeError):
        graph.nodes["new"] = IdentityNode()  # type: ignore[index]


def test_freeze_reports_unknown_edge_nodes() -> None:
    """验证 freeze() 报告 Edge 引用的未知 Node。"""

    graph = Graph("invalid")
    graph.add_node("start", IdentityNode())
    graph.connect("start", "missing")
    graph.add_flow(
        Flow("default", "start", frozenset({Endpoint("start")}))
    )

    with pytest.raises(GraphValidationError) as raised:
        graph.freeze()

    assert "edge target 'missing' is not a registered node" in raised.value.issues
    assert not graph.frozen


def test_freeze_reports_unreachable_flow_endpoint() -> None:
    """验证 freeze() 报告 Flow 无法到达的 Endpoint。"""

    graph = Graph("invalid")
    graph.add_node("start", IdentityNode())
    graph.add_node("isolated", IdentityNode())
    graph.add_flow(
        Flow("default", "start", frozenset({Endpoint("isolated")}))
    )

    with pytest.raises(GraphValidationError) as raised:
        graph.freeze()

    assert any("cannot reach any endpoint" in issue for issue in raised.value.issues)


def test_duplicate_nodes_edges_and_flows_are_rejected() -> None:
    """验证重复 Node、Edge 和 Flow 会被拒绝。"""

    graph = Graph("duplicates")
    graph.add_node("node", IdentityNode())

    with pytest.raises(GraphDefinitionError, match="duplicate node"):
        graph.add_node("node", IdentityNode())

    graph.connect("node", "node")
    with pytest.raises(GraphDefinitionError, match="duplicate edge"):
        graph.connect("node", "node")

    flow = Flow("default", "node", frozenset({Endpoint("node")}))
    graph.add_flow(flow)
    with pytest.raises(GraphDefinitionError, match="duplicate flow"):
        graph.add_flow(flow)


def test_unknown_node_and_flow_lookups_are_explicit() -> None:
    """验证未知 Node 和 Flow 查询抛出明确异常。"""

    graph = build_shared_graph().freeze()

    with pytest.raises(UnknownNodeError):
        graph.node("missing")
    with pytest.raises(UnknownNodeError):
        graph.outgoing("missing")
    with pytest.raises(UnknownFlowError):
        graph.flow("missing")


def test_flow_normalizes_endpoint_collection_to_frozenset() -> None:
    """验证 Flow 将 Endpoint 集合规范化为 frozenset。"""

    flow = Flow(
        "default",
        "node",
        {Endpoint("node")},  # type: ignore[arg-type]
    )

    assert flow.endpoints == frozenset({Endpoint("node")})


def test_flow_requires_at_least_one_endpoint() -> None:
    """验证 Flow 至少需要一个 Endpoint。"""

    with pytest.raises(ValueError, match="must not be empty"):
        Flow("default", "node", frozenset())
