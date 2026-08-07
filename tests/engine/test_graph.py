import json
import asyncio
from dataclasses import dataclass, field
from typing import ClassVar

import pytest

from bricks.engine import Event, Graph, GraphBuilder, Machine, Status
from bricks.engine.errors import (
    AsyncGuardRequired,
    GraphSerializationError,
    GraphValidationError,
    NoTransition,
)
from bricks.engine.graph import (
    AllOf,
    AnyOf,
    BaseNode,
    Not,
    Predicate,
    cycle_nodes,
    dead_end_nodes,
    non_terminating_nodes,
    reachable_nodes,
    terminal_nodes,
    transition_conflicts,
    unreachable_nodes,
)


def test_sync_dispatch_rejects_an_async_guard_with_a_clear_error():
    async def allow(context, event):
        return True

    builder = GraphBuilder("async-guard-sync", initial="start")
    builder.action("start")
    builder.terminal("done")
    builder.transition("start", "finish", "done", guard=allow)
    machine = Machine(builder.build())
    machine.start()

    with pytest.raises(AsyncGuardRequired, match="dispatch_async"):
        machine.dispatch("finish")

    assert machine.status is Status.RUNNING
    assert machine.node_id == "start"


def test_async_dispatch_awaits_async_guards_and_combinators():
    async def allow(context, event):
        await asyncio.sleep(0)
        return context.get("allowed", False)

    async def enabled(context, event):
        await asyncio.sleep(0)
        return True

    builder = GraphBuilder("async-guard", initial="start")
    builder.action("start", lambda context, event: {"allowed": True})
    builder.terminal("done")
    builder.transition(
        "start",
        "finish",
        "done",
        guard=lambda context, event: AllOf(allow, enabled)(context, event),
    )
    machine = Machine(builder.build())

    async def run():
        await machine.start_async()
        await machine.dispatch_async("finish")

    asyncio.run(run())

    assert machine.status is Status.COMPLETED


def test_guard_receives_the_complete_event_for_sync_routing():
    observed = []

    def allow(context, event):
        observed.append((event.payload, event.source, event.event_id))
        return event.payload["approved"] is True

    builder = GraphBuilder("complete-event-guard", initial="start")
    builder.action("start")
    builder.terminal("done")
    builder.transition("start", "review", "done", guard=allow)
    machine = Machine(builder.build())
    machine.start()

    received = Event(
        "review",
        payload={"approved": True},
        source="reviewer",
        event_id="review-1",
    )
    machine.dispatch(received)

    assert observed == [({"approved": True}, "reviewer", "review-1")]


def test_async_guard_receives_the_complete_event():
    observed = []

    async def allow(context, event):
        await asyncio.sleep(0)
        observed.append((event.payload, event.source, event.event_id))
        return event.payload["approved"] is True

    builder = GraphBuilder("async-complete-event-guard", initial="start")
    builder.action("start")
    builder.terminal("done")
    builder.transition("start", "review", "done", guard=allow)
    machine = Machine(builder.build())

    async def run():
        await machine.start_async()
        await machine.dispatch_async(
            Event(
                "review",
                payload={"approved": True},
                source="reviewer",
                event_id="review-async-1",
            )
        )

    asyncio.run(run())

    assert observed == [({"approved": True}, "reviewer", "review-async-1")]


def test_async_guard_combinators_keep_short_circuit_semantics():
    calls = []

    async def false_guard(context, event):
        calls.append("false")
        await asyncio.sleep(0)
        return False

    async def true_guard(context, event):
        calls.append("true")
        await asyncio.sleep(0)
        return True

    async def run():
        assert await AllOf(false_guard, true_guard)(None, None) is False
        assert await AnyOf(false_guard, true_guard)(None, None) is True
        assert await Not(Predicate(false_guard))(None, None) is True

    asyncio.run(run())

    assert calls == ["false", "false", "true", "false"]


def test_builder_produces_a_shared_immutable_graph_definition():
    builder = GraphBuilder("approval", initial="draft")
    builder.action("draft")
    builder.terminal("approved")
    builder.transition("draft", "approve", "approved")
    graph = builder.build()

    assert graph.id == "approval"
    assert graph.node("draft").kind == "action"
    assert graph.node("approved").terminal is True
    assert len(tuple(graph.transitions_from("draft", "approve"))) == 1

    with pytest.raises(TypeError):
        graph.nodes["new"] = graph.node("draft")
    with pytest.raises(TypeError):
        graph.node("draft").metadata["owner"] = "alice"
    with pytest.raises(TypeError):
        graph.transitions[0].metadata["owner"] = "alice"


def test_event_rejects_empty_identity_fields():
    with pytest.raises(ValueError, match="event name"):
        Event("")
    with pytest.raises(ValueError, match="event_id"):
        Event("ready", event_id="")


def test_custom_node_extends_base_node_without_machine_type_branches():
    @dataclass(frozen=True)
    class ApprovalNode(BaseNode):
        kind: ClassVar[str] = "approval"
        role: str = "reviewer"

        def enter(self, context, event, executor):
            context.set("approval_role", self.role)
            return super().enter(context, event, executor)

    builder = GraphBuilder("custom-node", initial="approval")
    builder.add_node(ApprovalNode("approval", role="owner"))
    builder.terminal("done")
    builder.transition("approval", "approved", "done")

    machine = Machine(builder.build())
    machine.start()
    machine.dispatch("approved")

    assert machine.context.get("approval_role") == "owner"
    assert machine.status is Status.COMPLETED


def test_async_machine_preserves_custom_sync_enter_and_exit_overrides():
    @dataclass(frozen=True)
    class ApprovalNode(BaseNode):
        kind: ClassVar[str] = "approval"

        def enter(self, context, event, executor):
            context.set("entered", event.name)
            return super().enter(context, event, executor)

        def exit(self, context, event, executor):
            context.set("exited", event.name)
            return super().exit(context, event, executor)

    builder = GraphBuilder("async-custom-node", initial="approval")
    builder.add_node(ApprovalNode("approval"))
    builder.terminal("done")
    builder.transition("approval", "approved", "done")
    machine = Machine(builder.build())

    async def run():
        await machine.start_async()
        await machine.dispatch_async("approved")

    asyncio.run(run())

    assert machine.context.data == {
        "entered": "__start__",
        "exited": "approved",
    }
    assert machine.status is Status.COMPLETED


def test_builder_includes_a_graph_as_a_namespaced_reusable_fragment():
    fragment_builder = GraphBuilder("review", initial="draft")
    fragment_builder.action("draft")
    fragment_builder.action("approved")
    fragment_builder.transition("draft", "approve", "approved")
    fragment = fragment_builder.build()

    builder = GraphBuilder("order", initial="start")
    builder.action("start")
    entry = builder.include(fragment, prefix="review_flow")
    builder.transition("start", "review", entry)
    graph = builder.build()

    assert entry == "review_flow.draft"
    assert graph.node("review_flow.approved").kind == "action"
    assert graph.transitions[-1].source == "start"
    assert graph.transitions[-1].target == "review_flow.draft"

    machine = Machine(graph)
    machine.start()
    machine.dispatch("review")
    machine.dispatch("approve")
    assert machine.node_id == "review_flow.approved"


def test_builder_include_reruns_custom_node_id_invariants():
    @dataclass(frozen=True)
    class DerivedNode(BaseNode):
        qualified_name: str = field(init=False)

        def __post_init__(self):
            super().__post_init__()
            object.__setattr__(self, "qualified_name", f"node:{self.id}")

    fragment_builder = GraphBuilder("fragment", initial="entry")
    fragment_builder.add_node(DerivedNode("entry"))
    fragment = fragment_builder.build()
    builder = GraphBuilder("host", initial="ns.entry")
    builder.include(fragment, prefix="ns")

    included = builder.build().node("ns.entry")
    assert included.qualified_name == "node:ns.entry"


def test_graph_describe_exposes_structure_without_exposing_executable_objects():
    builder = GraphBuilder("described", initial="start")
    builder.action("start", lambda context, event: None, metadata={"role": "root"})
    builder.terminal("done")
    builder.transition(
        "start",
        "finish",
        "done",
        guard=lambda context, event: True,
        action=lambda context, event: None,
    )
    description = builder.build().describe()

    assert description["id"] == "described"
    assert description["schema"] == "bricks.graph.description"
    assert description["schema_version"] == 1
    assert description["graph_version"] == "1"
    assert description["initial"] == "start"
    assert description["nodes"][0]["has_action"] is True
    assert description["transitions"][0]["has_guard"] is True
    assert "action" not in description["transitions"][0]
    assert json.loads(json.dumps(description))["schema"] == "bricks.graph.description"
    mermaid = builder.build().to_mermaid()
    assert "graph TD" in mermaid
    assert '"finish"' in mermaid
    dot = builder.build().to_dot()
    assert dot.startswith('digraph "described"')
    assert '"start" -> "done"' in dot
    assert "[guard]" in dot


def test_graph_description_keeps_builtin_node_configuration_without_callables():
    child = GraphBuilder("described-child", initial="start")
    child.action("start")
    child_graph = child.build()

    builder = GraphBuilder("described-config", initial="wait")
    builder.wait("wait", delay=3, resume_event="wake")
    builder.subgraph(
        "child",
        child_graph,
        entry_event="enter",
        return_event="returned",
        data={"source": "test"},
    )
    description = builder.build().describe()
    nodes = {node["id"]: node for node in description["nodes"]}

    assert nodes["wait"]["config"] == {"delay": 3, "resume_event": "wake"}
    assert nodes["child"]["config"] == {
        "graph_id": "described-child",
        "entry_event": "enter",
        "return_event": "returned",
        "data": {"source": "test"},
    }
    assert "action" not in nodes["wait"]


def test_subgraph_node_data_is_detached_and_read_only():
    child = GraphBuilder("immutable-child", initial="start")
    child.action("start")
    child_graph = child.build()
    data = {"nested": {"items": [1]}}

    builder = GraphBuilder("immutable-subgraph", initial="start")
    builder.action("start")
    builder.subgraph("child", child_graph, data=data)
    graph = builder.build()

    data["nested"]["items"].append(2)
    stored = graph.node("child").data
    assert stored == {"nested": {"items": [1]}}
    with pytest.raises(TypeError):
        stored["new"] = "value"


def test_graph_serialization_rejects_duplicate_node_ids():
    definition = {
        "version": 1,
        "id": "duplicate-nodes",
        "initial": "start",
        "nodes": [
            {"id": "start", "kind": "node"},
            {"id": "start", "kind": "terminal", "terminal": True},
        ],
        "transitions": [],
    }

    with pytest.raises(GraphSerializationError, match="duplicate node id"):
        Graph.from_dict(definition)

def test_graph_static_analysis_reports_unreachable_nodes():
    builder = GraphBuilder("analysis", initial="start")
    builder.action("start")
    builder.action("reachable")
    builder.action("orphan")
    builder.transition("start", "go", "reachable")
    graph = builder.build()

    assert reachable_nodes(graph) == {"start", "reachable"}
    assert unreachable_nodes(graph) == {"orphan"}


def test_graph_static_analysis_reports_terminals_and_reachable_dead_ends():
    builder = GraphBuilder("dead-ends", initial="start")
    builder.action("start")
    builder.action("dead-end")
    builder.terminal("done")
    builder.action("orphan")
    builder.transition("start", "go", "dead-end")
    builder.transition("start", "finish", "done")
    graph = builder.build()

    assert terminal_nodes(graph) == {"done"}
    assert dead_end_nodes(graph) == {"dead-end"}


def test_graph_static_analysis_reports_cycles_terminal_coverage_and_conflicts():
    builder = GraphBuilder("advanced-analysis", initial="start")
    builder.action("start")
    builder.action("loop-a")
    builder.action("loop-b")
    builder.action("stuck")
    builder.terminal("done")
    builder.transition("start", "begin", "loop-a")
    builder.transition("start", "choose", "loop-a")
    builder.transition("start", "choose", "done")
    builder.transition("start", "stuck", "stuck")
    builder.transition("loop-a", "next", "loop-b")
    builder.transition("loop-b", "back", "loop-a")
    builder.transition("loop-b", "finish", "done")
    graph = builder.build()

    assert cycle_nodes(graph) == {"loop-a", "loop-b"}
    assert non_terminating_nodes(graph) == {"stuck"}
    assert len(transition_conflicts(graph)) == 1
    assert set(transition_conflicts(graph)[0]) == {
        "start:choose:loop-a:1",
        "start:choose:done:2",
    }

    self_loop = GraphBuilder("self-loop", initial="loop")
    self_loop.action("loop")
    self_loop.transition("loop", "again", None)
    assert cycle_nodes(self_loop.build()) == {"loop"}


def test_graph_definition_round_trips_with_explicit_callable_resolvers():
    def enter(context, event):
        context.set("entered", True)

    def allow(context, event):
        return context.get("allowed", False)

    def transition_action(context, event):
        context.set("transitioned", True)

    builder = GraphBuilder("serializable", initial="ready")
    builder.action("ready", enter)
    builder.terminal("done")
    builder.transition(
        "ready",
        "finish",
        "done",
        guard=allow,
        action=transition_action,
        priority=2,
        metadata={"owner": "test"},
    )
    graph = builder.build()
    actions = {
        "enter": enter,
        "transition": transition_action,
    }
    encoded = graph.to_dict(
        action_serializer=lambda action: next(
            name for name, value in actions.items() if value is action
        ),
        guard_serializer=lambda guard: "allow" if guard is allow else "unknown",
    )

    restored = graph.from_dict(
        encoded,
        action_resolver=actions.__getitem__,
        guard_resolver=lambda reference: {"allow": allow}[reference],
    )

    assert restored.to_dict(
        action_serializer=lambda action: next(
            name for name, value in actions.items() if value is action
        ),
        guard_serializer=lambda guard: "allow",
    ) == encoded


def test_graph_definition_version_is_preserved_and_validated():
    builder = GraphBuilder("versioned", initial="ready", version="2026.1")
    builder.action("ready")
    graph = builder.build()
    encoded = graph.to_dict()
    restored = Graph.from_dict(encoded)

    assert restored.version == "2026.1"
    assert restored.describe()["version"] == "2026.1"


def test_graph_serialization_requires_explicit_resolvers_and_valid_version():
    builder = GraphBuilder("serializable-errors", initial="ready")
    builder.action("ready", lambda context, event: None)
    graph = builder.build()

    with pytest.raises(GraphSerializationError, match="action_serializer"):
        graph.to_dict()

    encoded = {
        "version": 999,
        "id": "serializable-errors",
        "initial": "ready",
        "nodes": [],
        "transitions": [],
    }
    with pytest.raises(GraphSerializationError, match="unsupported graph schema"):
        Graph.from_dict(encoded)


def test_custom_node_serialization_uses_a_small_explicit_codec():
    @dataclass(frozen=True)
    class ApprovalNode(BaseNode):
        kind: ClassVar[str] = "approval"
        role: str = "reviewer"

    builder = GraphBuilder("custom-serializable", initial="approval")
    builder.add_node(ApprovalNode("approval", role="owner"))
    builder.terminal("done")
    builder.transition("approval", "approve", "done")
    graph = builder.build()

    encoded = graph.to_dict(
        node_serializer=lambda node: {"role": node.role},
    )
    restored = Graph.from_dict(
        encoded,
        node_resolver=lambda value: ApprovalNode(
            value["id"], role=value["config"]["role"]
        ),
    )

    assert restored.node("approval").role == "owner"


def test_guarded_event_transition_and_hook_order():
    calls = []
    builder = GraphBuilder("guarded", initial="ready")
    builder.action("ready", lambda ctx, event: ctx.set("seen", event.payload))
    builder.terminal("accepted")
    builder.terminal("rejected")
    builder.transition(
        "ready",
        "review",
        "accepted",
        guard=lambda context, event: context.get("allowed", False),
        action=lambda context, event: context.set("seen", event.payload),
    )
    builder.transition("ready", "review", "rejected")

    machine = Machine(builder.build())
    machine.context.set("allowed", True)
    machine.hooks.on("transition.before", lambda hook: calls.append("before"))
    machine.hooks.on("node.exit", lambda hook: calls.append("exit"))
    machine.hooks.on("node.enter", lambda hook: calls.append("enter"))
    machine.hooks.on("transition.after", lambda hook: calls.append("after"))

    machine.start()
    calls.clear()
    machine.dispatch(Event("review", payload={"id": 1}))

    assert machine.node_id == "accepted"
    assert machine.status is Status.COMPLETED
    assert machine.context.get("seen") == {"id": 1}
    assert calls == ["before", "exit", "enter", "after"]


def test_invalid_graph_and_unhandled_events_fail_explicitly():
    with pytest.raises(GraphValidationError):
        GraphBuilder("invalid", initial="missing").build()

    builder = GraphBuilder("unhandled", initial="ready")
    builder.action("ready")
    machine = Machine(builder.build())
    machine.start()
    with pytest.raises(NoTransition):
        machine.dispatch("unknown")


def test_custom_node_can_add_a_required_dataclass_field():
    @dataclass(frozen=True)
    class RequiredNode(BaseNode):
        value: str

    node = RequiredNode("required", "configured")

    assert node.id == "required"
    assert node.value == "configured"


def test_graph_constructor_enforces_the_same_invariants_as_builder():
    with pytest.raises(GraphValidationError, match="initial node"):
        Graph(id="invalid", initial="missing", nodes={}, transitions=())


def test_graph_metadata_is_recursively_immutable_and_input_isolated():
    metadata = {"nested": {"owners": ["alice"]}}
    graph = GraphBuilder("immutable", initial="node").action(
        "node", metadata=metadata
    ).build()
    metadata["nested"]["owners"].append("bob")

    owners = graph.node("node").metadata["nested"]["owners"]
    assert owners == ["alice"]
    with pytest.raises(AttributeError):
        owners.append("mallory")
