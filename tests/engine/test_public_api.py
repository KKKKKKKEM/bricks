"""精简公共 API 和 Event 值对象测试。"""

from __future__ import annotations

import pytest

import bricks
from bricks import Event, Ports


def test_top_level_api_contains_only_core_vocabulary() -> None:
    """顶层只增加可控执行句柄，不暴露内部调度与 checkpoint DTO。"""

    assert bricks.__all__ == [
        "AsyncNode",
        "Context",
        "Edge",
        "Event",
        "Execution",
        "ExecutionLimits",
        "ExecutionPlan",
        "ExecutionStatus",
        "Graph",
        "InputPolicy",
        "Node",
        "Output",
        "Ports",
        "Runtime",
        "Slot",
        "SlotPool",
    ]


def test_ports_are_immutable_and_ordered() -> None:
    """Ports 保存声明顺序并拒绝修改。"""

    ports = Ports(left=str, right=int)

    assert tuple(ports) == ("left", "right")
    with pytest.raises(TypeError):
        ports["left"] = object  # type: ignore[index]


def test_event_is_minimal_domain_message() -> None:
    """Event 只保留路由类型和领域 payload。"""

    event = Event(
        "crawl.page.requested",
        {"url": "https://example.com"},
    )

    assert event.type == "crawl.page.requested"
    assert event.payload["url"] == "https://example.com"
