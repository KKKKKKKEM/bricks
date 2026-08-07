import asyncio

import pytest

from bricks.engine import InvalidOutputError, NodeResult, Output


def collect(result: NodeResult) -> list[Output]:
    """同步收集 NodeResult 的全部输出。

    参数：
        result: 需要消费的节点结果。

    返回：
        按产生顺序排列的 Output 列表。
    """

    async def consume() -> list[Output]:
        """在当前事件循环中异步消费节点结果。"""

        return [output async for output in result]

    return asyncio.run(consume())


def test_empty_result_produces_no_outputs() -> None:
    """验证 empty() 不产生 Output。"""

    assert collect(NodeResult.empty()) == []


def test_one_result_wraps_value_and_port() -> None:
    """验证 one() 正确包装 value 和 port。"""

    assert collect(NodeResult.one("value", port="success")) == [
        Output("value", port="success")
    ]


def test_many_result_preserves_output_order() -> None:
    """验证 many() 保留输入顺序。"""

    result = NodeResult.many(
        Output(index, port="item")
        for index in range(3)
    )

    assert collect(result) == [
        Output(0, port="item"),
        Output(1, port="item"),
        Output(2, port="item"),
    ]


def test_stream_yields_outputs_progressively() -> None:
    """验证 stream() 在消费时渐进产生 Output。"""

    produced: list[int] = []

    async def generate():
        """逐个生成测试 Output。"""

        for index in range(3):
            produced.append(index)
            yield Output(index)

    result = NodeResult.stream(generate())
    assert produced == []
    assert collect(result) == [Output(0), Output(1), Output(2)]
    assert produced == [0, 1, 2]


def test_stream_rejects_non_output_values_when_consumed() -> None:
    """验证 stream() 在消费阶段拒绝非法值。"""

    async def generate():
        """生成一个用于触发校验异常的非法值。"""

        yield "not-an-output"

    with pytest.raises(InvalidOutputError, match="got str"):
        collect(NodeResult.stream(generate()))  # type: ignore[arg-type]


def test_many_rejects_non_output_values_immediately() -> None:
    """验证 many() 在构建阶段立即拒绝非法值。"""

    with pytest.raises(InvalidOutputError, match="got str"):
        NodeResult.many(["not-an-output"])  # type: ignore[list-item]


@pytest.mark.parametrize("port", ["", " ", "\t"])
def test_output_rejects_empty_ports(port: str) -> None:
    """验证 Output 拒绝空端口。

    参数：
        port: 本轮参数化测试使用的空白端口。
    """

    with pytest.raises(ValueError, match="must not be empty"):
        Output(port=port)
