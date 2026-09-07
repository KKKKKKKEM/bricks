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
    input_ports = Ports(value=int)
    output_ports = Ports(value=int)

    def execute(self, inputs, context):
        del context
        return Output(inputs["value"], "value")


def test_runtime_observer_sees_read_only_lifecycle_and_cannot_break_work(
    caplog,
) -> None:
    events = []

    def observe(event) -> None:
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
        count = config["count"]
        selected = tuple(port for port in ports if available[port])
        return selected[:count] if len(selected) >= count else None


class ContributedNode(Node):
    input_ports = Ports(a=int, b=int, c=int)
    output_ports = Ports(total=int)
    input_policy = PolicyRef("example.test/any-two", {"count": 2})

    def execute(self, inputs, context):
        del context
        return Output(sum(inputs.values()), "total")


class FanOut(Node):
    input_ports = Ports(value=int)
    output_ports = Ports(a=int, b=int)

    def execute(self, inputs, context):
        del context
        return Output(inputs["value"], "a"), Output(inputs["value"] + 1, "b")


def test_contributed_policy_is_bound_when_graph_freezes() -> None:
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
    graph = Graph(entrypoint="join").add(join=ContributedNode())
    with (
        Runtime() as runtime,
        pytest.raises(GraphValidationError, match="not registered"),
    ):
        runtime.register("missing.graph", graph)


def test_memory_delivery_retries_and_increments_attempt() -> None:
    backend = memory.TaskBackend()
    attempts = []

    def handle(delivery):
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
    backend = memory.TaskBackend(max_delivery_attempts=2)
    attempts = []

    def retry(delivery):
        attempts.append(delivery.attempt)
        return DeliveryResult.retry()

    backend.bind("retry", retry, concurrency=1)
    backend.submit("retry", Work("graph", id="bounded"))
    with pytest.raises(RuntimeError, match="max_delivery_attempts=2"):
        backend.wait_idle()
    backend.close()
    assert attempts == [1, 2]


class PairSource(Node):
    input_ports = Ports(items=tuple)
    output_ports = Ports(left=KeyedValue, right=KeyedValue)

    def execute(self, inputs, context):
        del context
        return tuple(Output(item, side) for side, item in inputs["items"])


def keyed_graph() -> Graph:
    return (
        Graph(entrypoint="source")
        .add(source=PairSource(), join=KeyedJoin())
        .connect("source", "join", source_port="left", target_port="left")
        .connect("source", "join", source_port="right", target_port="right")
    )


def test_keyed_join_matches_interleaved_values_by_key() -> None:
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
    items = (("left", KeyedValue("a", "only-left")),)
    with Runtime() as runtime:
        runtime.register("join.graph", keyed_graph())
        with pytest.raises(IncompleteInputsError, match="unmatched"):
            runtime.run("join.graph", items)
