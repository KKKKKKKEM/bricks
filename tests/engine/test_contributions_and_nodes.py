"""受控 contribution 和官方可复用 Node 的契约测试。"""

from __future__ import annotations

import pytest

from bricks import Graph, Node, Output, Ports, Runtime
from bricks.adapters import memory
from bricks.engine.errors import GraphValidationError, IncompleteInputsError
from bricks.engine.observation import RuntimeEventKind
from bricks.engine.policies import PolicyRef
from bricks.nodes import KeyedJoin, KeyedPair, KeyedValue
from bricks.spi import (
    DeliveryResult,
    Work,
)


class Source(Node):
    """当前契约测试使用的 Source 替代实现。

    Attributes:
        input_ports: 节点声明的输入端口及其类型。
        output_ports: 节点声明的输出端口及其类型。
    """

    input_ports = Ports(value=int)
    output_ports = Ports(value=int)

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


def test_runtime_observer_sees_read_only_lifecycle_and_cannot_break_work(
    caplog,
) -> None:
    """验证生命周期观察只读且观察失败不影响工作结果。

    Args:
        caplog: 当前用例使用的 caplog 夹具或参数化输入。
    """

    events = []

    def observe(event) -> None:
        """注册只读事件观察者，不建立目标 Graph 路由。

        Args:
            event: 需要发布、观察或处理的事件。

        Raises:
            RuntimeError: 当前生命周期状态或操作顺序不允许此操作。
        """

        events.append(event)
        raise RuntimeError("telemetry failed")

    graph = Graph(entrypoint="source").add(source=Source())
    with Runtime() as runtime:
        handle = runtime.observe_runtime(observe)
        runtime.register("observed.graph", graph)
        assert runtime.run("observed.graph", 3) == (Output(3, "value"),)
        handle.detach()

    assert [event.kind for event in events] == [
        RuntimeEventKind.EXECUTION_STARTED,
        RuntimeEventKind.NODE_STARTED,
        RuntimeEventKind.NODE_FINISHED,
        RuntimeEventKind.EXECUTION_FINISHED,
    ]
    assert events[-1].attributes["steps"] == 1
    assert "runtime observer failed" in caplog.text
    with pytest.raises(TypeError):
        events[-1].attributes["steps"] = 2


class AnyTwo:
    def select(self, ports, available, config):
        """仅根据端口名称和可用 token 数量选择本次输入组合。

        Args:
            ports: 保持声明顺序的输入端口名称。
            available: 各端口当前可消费的 token 数量。
            config: 当前具名策略的参数映射。

        Returns:
            符合当前可用条件的选择结果，没有可执行输入时不触发。
        """

        count = config["count"]
        selected = tuple(port for port in ports if available[port])
        return selected[:count] if len(selected) >= count else None


class ContributedNode(Node):
    """当前契约测试使用的 ContributedNode 替代实现。

    Attributes:
        input_ports: 节点声明的输入端口及其类型。
        output_ports: 节点声明的输出端口及其类型。
        input_policy: 仅依据端口和 token 数量生效的输入策略。
    """

    input_ports = Ports(a=int, b=int, c=int)
    output_ports = Ports(total=int)
    input_policy = PolicyRef("example.test/any-two", {"count": 2})

    def execute(self, inputs, context):
        """执行当前测试场景的节点行为，供外层契约断言检查。

        Args:
            inputs: 入口数据或按端口名称组织的输入映射。
            context: 当前调用的执行或插件上下文。

        Returns:
            测试节点或替代执行器产生的返回值。
        """

        del context
        return Output(sum(inputs.values()), "total")


class FanOut(Node):
    """当前契约测试使用的 FanOut 替代实现。

    Attributes:
        input_ports: 节点声明的输入端口及其类型。
        output_ports: 节点声明的输出端口及其类型。
    """

    input_ports = Ports(value=int)
    output_ports = Ports(a=int, b=int)

    def execute(self, inputs, context):
        """执行当前测试场景的节点行为，供外层契约断言检查。

        Args:
            inputs: 入口数据或按端口名称组织的输入映射。
            context: 当前调用的执行或插件上下文。

        Returns:
            测试节点或替代执行器产生的返回值。
        """

        del context
        return Output(inputs["value"], "a"), Output(inputs["value"] + 1, "b")


def test_contributed_policy_is_bound_when_graph_freezes() -> None:
    """验证 Graph 冻结时固定贡献策略的实现快照。"""

    graph = (
        Graph(entrypoint="source")
        .add(source=FanOut(), join=ContributedNode())
        .connect("source", "join", source_port="a", target_port="a")
        .connect("source", "join", source_port="b", target_port="b")
    )
    with Runtime() as runtime:
        runtime.register_policy("example.test/any-two", AnyTwo())
        runtime.register("policy.graph", graph)
        assert runtime.run("policy.graph", 2) == (Output(5, "total"),)


def test_missing_contributed_policy_fails_at_registration() -> None:
    """验证缺少贡献策略时注册 Graph 明确失败。"""

    graph = Graph(entrypoint="join").add(join=ContributedNode())
    with (
        Runtime() as runtime,
        pytest.raises(GraphValidationError, match="not registered"),
    ):
        runtime.register("missing.graph", graph)


def test_memory_delivery_retries_and_increments_attempt() -> None:
    """验证内存后端根据交付决定重试并递增尝试次数。"""

    backend = memory.TaskBackend()
    attempts = []

    def handle(delivery):
        """处理测试投递，并返回明确的交付结果。

        Args:
            delivery: 携带尝试次数和可选 Slot lease 的本次投递。

        Returns:
            本次测试投递对应的 DeliveryResult。
        """

        attempts.append(delivery.attempt)
        if delivery.attempt < 3:
            return DeliveryResult.retry()
        return DeliveryResult.ack()

    backend.bind("retry", handle, concurrency=1)
    backend.submit("retry", Work("graph"))
    backend.wait_idle()
    backend.close()

    assert attempts == [1, 2, 3]


def test_memory_delivery_reports_rejection() -> None:
    """验证内存后端向等待方传播投递拒绝。"""

    backend = memory.TaskBackend()
    backend.bind(
        "reject",
        lambda delivery: DeliveryResult.reject(ValueError(delivery.work.id)),
        concurrency=1,
    )
    backend.submit("reject", Work("graph", id="work-1"))
    with pytest.raises(ValueError, match="work-1"):
        backend.wait_idle()
    backend.close()


def test_memory_delivery_stops_after_configured_attempts() -> None:
    """验证内存重投递在配置次数耗尽后停止。"""

    backend = memory.TaskBackend(max_delivery_attempts=2)
    attempts = []

    def retry(delivery):
        """创建请求后端重新投递的交付决定。

        Args:
            delivery: 携带尝试次数和可选 Slot lease 的本次投递。

        Returns:
            当前测试回调收集或构造的结果。
        """

        attempts.append(delivery.attempt)
        return DeliveryResult.retry()

    backend.bind("retry", retry, concurrency=1)
    backend.submit("retry", Work("graph", id="bounded"))
    with pytest.raises(RuntimeError, match="max_delivery_attempts=2"):
        backend.wait_idle()
    backend.close()
    assert attempts == [1, 2]


class PairSource(Node):
    """当前契约测试使用的 PairSource 替代实现。

    Attributes:
        input_ports: 节点声明的输入端口及其类型。
        output_ports: 节点声明的输出端口及其类型。
    """

    input_ports = Ports(items=tuple)
    output_ports = Ports(left=KeyedValue, right=KeyedValue)

    def execute(self, inputs, context):
        """执行当前测试场景的节点行为，供外层契约断言检查。

        Args:
            inputs: 入口数据或按端口名称组织的输入映射。
            context: 当前调用的执行或插件上下文。

        Returns:
            测试节点或替代执行器产生的返回值。
        """

        del context
        return tuple(Output(item, side) for side, item in inputs["items"])


def keyed_graph() -> Graph:
    """构造用于验证按键关联输入的 Graph。

    Returns:
        连接输入分发与按键关联节点的 Graph。
    """

    return (
        Graph(entrypoint="source")
        .add(source=PairSource(), join=KeyedJoin())
        .connect("source", "join", source_port="left", target_port="left")
        .connect("source", "join", source_port="right", target_port="right")
    )


def test_keyed_join_matches_interleaved_values_by_key() -> None:
    """验证交错到达的数据按领域键正确配对。"""

    items = (
        ("left", KeyedValue("a", "A-left")),
        ("left", KeyedValue("b", "B-left")),
        ("right", KeyedValue("b", "B-right")),
        ("right", KeyedValue("a", "A-right")),
    )
    with Runtime() as runtime:
        runtime.register("join.graph", keyed_graph())
        outputs = runtime.run("join.graph", items)

    assert outputs == (
        Output(KeyedPair("b", "B-left", "B-right"), "joined"),
        Output(KeyedPair("a", "A-left", "A-right"), "joined"),
    )


def test_keyed_join_rejects_unmatched_values_at_quiescence() -> None:
    """验证静止时未配对的关联数据导致执行失败。"""

    items = (("left", KeyedValue("a", "only-left")),)
    with Runtime() as runtime:
        runtime.register("join.graph", keyed_graph())
        with pytest.raises(IncompleteInputsError, match="unmatched"):
            runtime.run("join.graph", items)
