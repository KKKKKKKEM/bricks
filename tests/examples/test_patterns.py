"""仓库中的通用编排示例应可直接运行。"""

import asyncio

from examples.async_node import run as run_async_node
from examples.cycle import run as run_cycle
from examples.event_routing import run as run_event_routing
from examples.fan_in import run as run_fan_in
from examples.linear import run as run_linear
from examples.output_stream import arun as run_async_output_stream
from examples.output_stream import run as run_output_stream


def test_linear_example() -> None:
    """验证线性数据流示例的文本转换结果。"""

    assert run_linear("  bricks ") == "BRICKS"


def test_fan_in_example() -> None:
    """验证分支汇聚示例的计算结果。"""

    assert run_fan_in(2) == 5


def test_event_routing_example() -> None:
    """验证跨图事件路由示例的交付结果。"""

    assert run_event_routing("message") == ["message"]


def test_async_node_example() -> None:
    """验证异步节点示例的执行结果。"""

    assert run_async_node("bricks") == "BRICKS"


def test_cycle_example() -> None:
    """验证循环示例在不再产生数据时自然结束。"""

    assert run_cycle(0) == 3


def test_output_stream_example() -> None:
    """验证同步和异步输出流示例结果一致。"""

    assert run_output_stream(3) == ([0, 1, 2], [0, 1, 2])
    assert asyncio.run(run_async_output_stream(3)) == (
        [0, 1, 2],
        [0, 1, 2],
    )
