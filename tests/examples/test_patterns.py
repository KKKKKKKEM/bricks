"""仓库中的通用编排示例应可直接运行。"""

from examples.async_node import run as run_async_node
from examples.event_routing import run as run_event_routing
from examples.fan_in import run as run_fan_in
from examples.linear import run as run_linear


def test_linear_example() -> None:
    assert run_linear("  bricks ") == "BRICKS"


def test_fan_in_example() -> None:
    assert run_fan_in(2) == 5


def test_event_routing_example() -> None:
    assert run_event_routing("message") == ["message"]


def test_async_node_example() -> None:
    assert run_async_node("bricks") == "BRICKS"
