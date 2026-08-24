"""精简公共 API 和 Event 值对象测试。"""

from __future__ import annotations

from dataclasses import fields

import pytest

import bricks
from bricks import Event, Ports, adapters, engine, nodes, plugins, runtime, spi
from bricks.spi import Work


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


def test_advanced_packages_expose_explicit_architecture_boundaries() -> None:
    """内核、编排、SPI、插件和默认适配器不再聚合到 engine。"""

    assert engine.__all__ == [name for name in bricks.__all__ if name != "Runtime"]
    assert runtime.__all__ == [
        "EventRouter",
        "GraphWorker",
        "LocalRuntimePlugin",
        "Runtime",
    ]
    assert adapters.__all__ == ["memory"]
    assert "EventBus" in spi.__all__
    assert "PluginHost" in plugins.__all__
    assert "ContributionPlugin" in plugins.__all__
    assert nodes.__all__ == ["KeyedJoin", "KeyedPair", "KeyedValue"]
    assert not hasattr(engine, "Runtime")
    assert not hasattr(engine, "PluginHost")
    assert adapters.memory.EventBus.__name__ == "EventBus"
    assert adapters.memory.TaskBackend.__name__ == "TaskBackend"
    assert not hasattr(adapters.memory, "MemoryEventBus")
    assert not hasattr(adapters.memory, "MemoryTaskBackend")
    assert not hasattr(plugins, "ExtensionPlugin")


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
    assert tuple(field.name for field in fields(Event)) == ("type", "payload")


def test_work_contains_only_transportable_execution_data() -> None:
    Work("crawl.graph", {"url": "https://example.com"})

    assert tuple(field.name for field in fields(Work)) == (
        "graph",
        "inputs",
        "trigger",
        "id",
        "limits",
    )
