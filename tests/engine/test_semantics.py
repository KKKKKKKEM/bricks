import asyncio

import pytest

from bricks.engine import GraphBuilder, Machine, Outcome, Status
from bricks.engine.errors import (
    CancellationError,
    GraphValidationError,
    MachineNotRunnable,
    NoTransition,
    PersistenceError,
)
from bricks.engine.policies import CancellationToken
from bricks.engine.runtime.outcomes import ForkBranch
from bricks.engine.runtime.fork import ForkController
from bricks.engine.semantics import (
    CompensationPlan,
    CompensationStep,
    JoinPolicy,
    Parallel,
    SagaRuntime,
    Workflow,
)


def test_fork_creates_independent_children_and_join_returns_to_parent_graph():
    def fork_action(ctx, event):
        return Outcome.fork(
            ForkBranch("branch", payload="a", data={"branch": "a"}),
            {"event": "branch", "payload": "b", "data": {"branch": "b"}},
            join_event="joined",
        )

    def branch_action(ctx, event):
        ctx.set("result", (ctx.get("branch"), event.payload))
        return Outcome.next("finish")

    builder = GraphBuilder("fork", initial="start")
    builder.action("start")
    builder.action("forking", fork_action)
    builder.action("branch", branch_action)
    builder.action("parent_waiting")
    builder.terminal("child_done")
    builder.terminal("done")
    builder.transition("start", "fork", "forking")
    builder.transition("start", "branch", "branch")
    builder.transition("branch", "finish", "child_done")
    builder.transition("forking", "joined", "done")

    machine = Machine(builder.build())
    machine.start()
    result = machine.dispatch("fork")
    assert result.target == "forking"
    assert machine.status is Status.WAITING
    assert machine.fork_group is not None
    assert len(machine.fork_group.children) == 2
    assert all(child.status is Status.COMPLETED for child in machine.fork_group.children)
    assert [child.context.get("result") for child in machine.fork_group.children] == [
        ("a", "a"),
        ("b", "b"),
    ]

    joined = machine.join()
    assert joined is not None
    assert joined.event.name == "joined"
    assert machine.status is Status.COMPLETED
    assert machine.fork_group is None


def test_fork_runtime_factory_is_inherited_by_child_runs():
    created = []

    class RecordingForkRuntime(ForkController):
        def __init__(self, parent):
            created.append(parent.context.run_id)
            super().__init__(parent)

    builder = GraphBuilder("fork-runtime-port", initial="ready")
    builder.action("ready")
    builder.action(
        "forking", lambda context, event: Outcome.fork({"event": "finish"})
    )
    builder.terminal("done")
    builder.transition("ready", "fork", "forking")
    builder.transition("ready", "finish", "done")
    machine = Machine(
        builder.build(),
        fork_runtime_factory=RecordingForkRuntime,
    )

    machine.start()
    machine.dispatch("fork")

    assert machine.fork_group is not None
    child = machine.fork_group.children[0]
    assert created == [machine.context.run_id, child.context.run_id]


def test_dynamic_fanout_maps_context_items_and_reduces_at_join():
    def fan_out(context, event):
        return Outcome.fork(
            *(
                {"event": "process", "data": {"item": item}}
                for item in context.get("items", [])
            ),
            join_event="joined",
        )

    def process(context, event):
        context.set("value", context.get("item") * 2)
        return Outcome.next("finish")

    def reduce_results(context, event):
        context.set(
            "results",
            [child["data"]["value"] for child in event.payload["children"]],
        )

    builder = GraphBuilder("dynamic-fanout", initial="start")
    builder.action("start", lambda context, event: context.set("items", [1, 2, 3]))
    builder.action("fan_out", fan_out)
    builder.action("process", process)
    builder.terminal("branch_done")
    builder.action("joined", reduce_results)
    builder.terminal("done")
    builder.transition("start", "fork", "fan_out")
    builder.transition("start", "process", "process")
    builder.transition("process", "finish", "branch_done")
    builder.transition("fan_out", "joined", "joined")
    builder.transition("joined", "finish", "done")

    machine = Machine(builder.build())
    machine.start()
    machine.dispatch("fork")
    machine.join()
    machine.dispatch("finish")

    assert machine.status is Status.COMPLETED
    assert machine.context.get("results") == [2, 4, 6]


def test_fork_snapshot_restores_children_for_join():
    builder = GraphBuilder("fork-snapshot", initial="start")
    builder.action("start")
    builder.action(
        "forking",
        lambda ctx, event: Outcome.fork({"event": "finish"}, join_event="joined"),
    )
    builder.terminal("child_done")
    builder.terminal("done")
    builder.transition("start", "fork", "forking")
    builder.transition("start", "finish", "child_done")
    builder.transition("forking", "joined", "done")
    graph = builder.build()

    machine = Machine(graph)
    machine.start()
    machine.dispatch("fork")
    restored = Machine.from_snapshot(graph, machine.snapshot())

    assert restored.fork_group is not None
    assert restored.fork_group.children[0].status is Status.COMPLETED
    restored.join()
    assert restored.status is Status.COMPLETED


def test_fork_snapshot_preserves_failure_policy():
    builder = GraphBuilder("fork-failure-snapshot", initial="start")
    builder.action("start")
    builder.action(
        "forking",
        lambda ctx, event: Outcome.fork(
            {"event": "fail"},
            {"event": "finish"},
            join_event="joined",
            failure_policy="continue",
        ),
    )
    builder.action("failing", lambda ctx, event: Outcome.fail("branch error"))
    builder.terminal("child_done")
    builder.terminal("done")
    builder.transition("start", "fork", "forking")
    builder.transition("start", "fail", "failing")
    builder.transition("start", "finish", "child_done")
    builder.transition("forking", "joined", "done")
    graph = builder.build()

    machine = Machine(graph)
    machine.start()
    machine.dispatch("fork")
    restored = Machine.from_snapshot(graph, machine.snapshot())

    assert restored.fork_group.failure_policy == "continue"
    restored.join()
    assert restored.status is Status.COMPLETED


def test_join_without_a_return_event_restores_a_runnable_parent():
    builder = GraphBuilder("fork-without-join-event", initial="start")
    builder.action("start")
    builder.action(
        "forking",
        lambda ctx, event: Outcome.fork({"event": "finish"}),
    )
    builder.terminal("child_done")
    builder.terminal("done")
    builder.transition("start", "fork", "forking")
    builder.transition("start", "finish", "child_done")
    builder.transition("forking", "finish", "done")
    machine = Machine(builder.build())

    machine.start()
    machine.dispatch("fork")
    machine.join()

    assert machine.status is Status.RUNNING
    assert machine.context.waiting is None
    machine.dispatch("finish")
    assert machine.status is Status.COMPLETED


def test_route_can_wake_a_waiting_fork_child_before_joining_parent():
    builder = GraphBuilder("routed-fork", initial="start")
    builder.action("start")
    builder.action(
        "forking",
        lambda ctx, event: Outcome.fork(
            {"event": "wait"},
            {"event": "finish"},
            join_event="joined",
        ),
    )
    builder.wait("waiting", resume_event="wake")
    builder.terminal("child_done")
    builder.terminal("done")
    builder.transition("start", "fork", "forking")
    builder.transition("start", "wait", "waiting")
    builder.transition("start", "finish", "child_done")
    builder.transition("waiting", "wake", "child_done")
    builder.transition("forking", "joined", "done")
    machine = Machine(builder.build())

    machine.start()
    machine.dispatch("fork")

    waiting_child = next(
        child
        for child in machine.fork_group.children
        if child.status is Status.WAITING
    )
    completed_child = next(
        child
        for child in machine.fork_group.children
        if child.status is Status.COMPLETED
    )
    assert waiting_child.context.metadata["parent_run_id"] == machine.context.run_id
    assert completed_child.context.metadata["parent_run_id"] == machine.context.run_id

    routed = machine.route(waiting_child.context.run_id, "wake")

    assert routed.target == "child_done"
    assert waiting_child.status is Status.COMPLETED
    machine.join()
    assert machine.status is Status.COMPLETED


def test_nested_fork_can_route_grandchild_and_join_at_each_level():
    child_builder = GraphBuilder("nested-child", initial="start")
    child_builder.action("start")
    child_builder.action(
        "forking",
        lambda ctx, event: Outcome.fork({"event": "wait"}, join_event="joined"),
    )
    child_builder.wait("waiting", resume_event="wake")
    child_builder.terminal("grandchild_done")
    child_builder.terminal("child_done")
    child_builder.transition("start", "fork", "forking")
    child_builder.transition("start", "wait", "waiting")
    child_builder.transition("waiting", "wake", "grandchild_done")
    child_builder.transition("forking", "joined", "child_done")
    child_graph = child_builder.build()

    parent_builder = GraphBuilder("nested-parent", initial="start")
    parent_builder.action("start")
    parent_builder.subgraph(
        "child_flow",
        child_graph,
        entry_event="fork",
        return_event="returned",
    )
    parent_builder.terminal("done")
    parent_builder.transition("start", "enter", "child_flow")
    parent_builder.transition("child_flow", "returned", "done")
    parent_graph = parent_builder.build()
    machine = Machine(parent_graph)

    machine.start()
    machine.dispatch("enter")

    child = machine.fork_group.children[0]
    assert child.context.metadata["parent_run_id"] == machine.context.run_id
    assert child.fork_group is not None
    grandchild = child.fork_group.children[0]
    assert grandchild.context.metadata["parent_run_id"] == child.context.run_id
    assert grandchild.status is Status.WAITING

    machine.route(grandchild.context.run_id, "wake")
    machine.join(child.context.run_id)
    machine.join()

    assert grandchild.status is Status.COMPLETED
    assert child.status is Status.COMPLETED
    assert machine.status is Status.COMPLETED


def test_async_route_can_wake_a_fork_child_before_async_join():
    builder = GraphBuilder("async-routed-fork", initial="start")
    builder.action("start")
    builder.action(
        "forking",
        lambda ctx, event: Outcome.fork({"event": "wait"}, join_event="joined"),
    )
    builder.wait("waiting", resume_event="wake")
    builder.terminal("child_done")
    builder.terminal("done")
    builder.transition("start", "fork", "forking")
    builder.transition("start", "wait", "waiting")
    builder.transition("waiting", "wake", "child_done")
    builder.transition("forking", "joined", "done")
    machine = Machine(builder.build())

    async def run():
        await machine.start_async()
        await machine.dispatch_async("fork")
        child = machine.fork_group.children[0]
        await machine.route_async(child.context.run_id, "wake")
        await machine.join_async()

    asyncio.run(run())
    assert machine.status is Status.COMPLETED


def test_any_join_stops_remaining_children_after_one_succeeds():
    builder = GraphBuilder("fork-any", initial="start")
    builder.action("start")
    builder.action(
        "forking",
        lambda ctx, event: Outcome.fork(
            {"event": "quick"},
            {"event": "slow"},
            join_event="joined",
            policy="any",
        ),
    )
    builder.terminal("quick_done")
    builder.wait("slow_wait", resume_event="resume")
    builder.terminal("done")
    builder.transition("start", "fork", "forking")
    builder.transition("start", "quick", "quick_done")
    builder.transition("start", "slow", "slow_wait")
    builder.transition("forking", "joined", "done")

    machine = Machine(builder.build())
    machine.start()
    machine.dispatch("fork")
    assert machine.fork_group is not None
    assert machine.fork_group.finished is True
    slow_child = machine.fork_group.children[1]
    assert slow_child.status is Status.WAITING
    machine.join()

    assert machine.status is Status.COMPLETED
    assert machine.fork_group is None
    assert slow_child.status is Status.STOPPED


def test_async_fork_and_join_are_available():
    async def fork_action(ctx, event):
        return Outcome.fork({"event": "finish"}, join_event="joined")

    builder = GraphBuilder("async-fork", initial="start")
    builder.action("start")
    builder.action("forking", fork_action)
    builder.terminal("child_done")
    builder.terminal("done")
    builder.transition("start", "fork", "forking")
    builder.transition("start", "finish", "child_done")
    builder.transition("forking", "joined", "done")
    machine = Machine(builder.build())

    async def run():
        await machine.start_async()
        await machine.dispatch_async("fork")
        await machine.join_async()

    asyncio.run(run())


def test_subgraph_node_runs_an_independent_graph_and_returns_to_parent():
    child_builder = GraphBuilder("child", initial="start")
    child_builder.action("start", lambda ctx, event: ctx.set("child_value", 42))
    child_builder.terminal("done")
    child_builder.transition("start", "finish", "done")
    child_graph = child_builder.build()

    parent_builder = GraphBuilder("parent", initial="start")
    parent_builder.action("start")
    parent_builder.subgraph(
        "child_flow",
        child_graph,
        entry_event="finish",
        return_event="returned",
    )
    parent_builder.terminal("done")
    parent_builder.transition("start", "enter", "child_flow")
    parent_builder.transition("child_flow", "returned", "done")
    parent_graph = parent_builder.build()

    machine = Machine(parent_graph)
    machine.start()
    machine.dispatch("enter")

    assert machine.status is Status.WAITING
    assert machine.fork_group is not None
    child = machine.fork_group.children[0]
    assert child.graph.id == "child"
    assert child.status is Status.COMPLETED

    machine.join()

    assert machine.status is Status.COMPLETED
    assert machine.context.get("child_value") == 42


def test_subgraph_snapshot_restore_uses_a_graph_resolver():
    child_builder = GraphBuilder("child-snapshot", initial="start")
    child_builder.action("start")
    child_builder.terminal("done")
    child_builder.transition("start", "finish", "done")
    child_graph = child_builder.build()

    parent_builder = GraphBuilder("parent-snapshot", initial="start")
    parent_builder.action("start")
    parent_builder.subgraph(
        "child_flow", child_graph, entry_event="finish", return_event="returned"
    )
    parent_builder.terminal("done")
    parent_builder.transition("start", "enter", "child_flow")
    parent_builder.transition("child_flow", "returned", "done")
    parent_graph = parent_builder.build()

    machine = Machine(parent_graph)
    machine.start()
    machine.dispatch("enter")
    snapshot = machine.snapshot()

    with pytest.raises(PersistenceError, match="graph resolver"):
        Machine.from_snapshot(parent_graph, snapshot)

    restored = Machine.from_snapshot(
        parent_graph,
        snapshot,
        graph_resolver=lambda graph_id: {"child-snapshot": child_graph}[graph_id],
    )
    assert restored.fork_group.children[0].graph.id == "child-snapshot"
    restored.join()
    assert restored.status is Status.COMPLETED


def test_async_subgraph_uses_async_child_lifecycle():
    async def mark_reviewed(context, event):
        context.set("reviewed", True)

    child_builder = GraphBuilder("async-child", initial="start")
    child_builder.action("start", mark_reviewed)
    child_builder.terminal("done")
    child_builder.transition("start", "finish", "done")
    child_graph = child_builder.build()

    parent_builder = GraphBuilder("async-parent", initial="start")
    parent_builder.action("start")
    parent_builder.subgraph(
        "child_flow", child_graph, entry_event="finish", return_event="returned"
    )
    parent_builder.terminal("done")
    parent_builder.transition("start", "enter", "child_flow")
    parent_builder.transition("child_flow", "returned", "done")
    machine = Machine(parent_builder.build())

    async def run():
        await machine.start_async()
        await machine.dispatch_async("enter")
        await machine.join_async()

    asyncio.run(run())
    assert machine.status is Status.COMPLETED
    assert machine.context.get("reviewed") is True


def test_async_fork_starts_child_actions_concurrently():
    active = 0
    maximum_active = 0

    async def work(context, event):
        nonlocal active, maximum_active
        active += 1
        maximum_active = max(maximum_active, active)
        await asyncio.sleep(0)
        active -= 1
        return Outcome.next("finish")

    builder = GraphBuilder("concurrent-fork", initial="start")
    builder.action("start")
    builder.action("forking", lambda ctx, event: Outcome.fork(
        {"event": "finish"}, {"event": "finish"}, join_event="joined"
    ))
    builder.action("branch", work)
    builder.terminal("child_done")
    builder.terminal("done")
    builder.transition("start", "fork", "forking")
    builder.transition("start", "finish", "branch")
    builder.transition("branch", "finish", "child_done")
    builder.transition("forking", "joined", "done")
    machine = Machine(builder.build())

    async def run():
        await machine.start_async()
        await machine.dispatch_async("fork")
        await machine.join_async()

    asyncio.run(run())
    assert maximum_active == 2
    assert machine.status is Status.COMPLETED


def test_async_fork_cancels_sibling_tasks_when_a_branch_fails():
    started = asyncio.Event()
    cleaned_up = asyncio.Event()

    async def long_running(context, event):
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            cleaned_up.set()

    async def fail_after_sibling_started(context, event):
        await started.wait()
        raise RuntimeError("branch failed")

    builder = GraphBuilder("cancel-failed-fork", initial="start")
    builder.action("start")
    builder.action(
        "forking",
        lambda ctx, event: Outcome.fork(
            {"event": "run"}, {"event": "fail"}, join_event="joined"
        ),
    )
    builder.action("running", long_running)
    builder.action("failing", fail_after_sibling_started)
    builder.terminal("done")
    builder.transition("start", "fork", "forking")
    builder.transition("start", "run", "running")
    builder.transition("start", "fail", "failing")
    builder.transition("running", "finish", "done")
    builder.transition("forking", "joined", "done")
    machine = Machine(builder.build())

    async def run():
        await machine.start_async()
        with pytest.raises(RuntimeError, match="branch failed"):
            await machine.dispatch_async("fork")

    asyncio.run(run())
    assert cleaned_up.is_set()
    assert machine.status is Status.FAILED


def test_async_fork_respects_a_maximum_concurrency_limit():
    active = 0
    maximum_active = 0

    async def work(context, event):
        nonlocal active, maximum_active
        active += 1
        maximum_active = max(maximum_active, active)
        await asyncio.sleep(0)
        active -= 1
        return Outcome.next("finish")

    builder = GraphBuilder("limited-fork", initial="start")
    builder.action("start")
    builder.action(
        "forking",
        lambda ctx, event: Outcome.fork(
            {"event": "finish"},
            {"event": "finish"},
            {"event": "finish"},
            {"event": "finish"},
            join_event="joined",
            max_concurrency=2,
        ),
    )
    builder.action("branch", work)
    builder.terminal("child_done")
    builder.terminal("done")
    builder.transition("start", "fork", "forking")
    builder.transition("start", "finish", "branch")
    builder.transition("branch", "finish", "child_done")
    builder.transition("forking", "joined", "done")
    machine = Machine(builder.build())

    async def run():
        await machine.start_async()
        await machine.dispatch_async("fork")
        await machine.join_async()

    asyncio.run(run())
    assert maximum_active == 2
    assert machine.status is Status.COMPLETED


def test_any_join_waits_for_a_success_after_an_early_failure():
    builder = GraphBuilder("any-success", initial="start")
    builder.action("start")
    builder.action(
        "forking",
        lambda ctx, event: Outcome.fork(
            {"event": "fail"},
            {"event": "wait"},
            join_event="joined",
            policy="any",
        ),
    )
    builder.action("failing", lambda ctx, event: Outcome.fail("branch failed"))
    builder.wait("waiting", resume_event="wake")
    builder.terminal("child_done")
    builder.terminal("done")
    builder.transition("start", "fork", "forking")
    builder.transition("start", "fail", "failing")
    builder.transition("start", "wait", "waiting")
    builder.transition("waiting", "wake", "child_done")
    builder.transition("forking", "joined", "done")
    machine = Machine(builder.build())
    machine.start()
    machine.dispatch("fork")

    assert machine.fork_group is not None
    assert machine.fork_group.children[0].status is Status.FAILED
    assert machine.fork_group.children[1].status is Status.WAITING
    assert machine.fork_group.finished is False
    with pytest.raises(MachineNotRunnable, match="尚未完成"):
        machine.join()

    machine.fork_group.children[1].resume("wake")
    machine.join()
    assert machine.status is Status.COMPLETED


def test_fork_continue_failure_policy_joins_with_partial_results():
    def record_children(context, event):
        context.set(
            "child_statuses",
            [child["status"] for child in event.payload["children"]],
        )

    builder = GraphBuilder("fork-continue-failure", initial="start")
    builder.action("start")
    builder.action(
        "forking",
        lambda ctx, event: Outcome.fork(
            {"event": "fail"},
            {"event": "finish"},
            join_event="joined",
            failure_policy="continue",
        ),
    )
    builder.action("failing", lambda ctx, event: Outcome.fail("branch error"))
    builder.terminal("child_done")
    builder.action("done", record_children)
    builder.transition("start", "fork", "forking")
    builder.transition("start", "fail", "failing")
    builder.transition("start", "finish", "child_done")
    builder.transition("forking", "joined", "done")
    builder.terminal("completed")
    builder.transition("done", "finish", "completed")
    machine = Machine(builder.build())

    machine.start()
    machine.dispatch("fork")
    joined = machine.join()

    assert joined is not None
    assert machine.status is Status.RUNNING
    assert machine.context.get("child_statuses") == [
        Status.FAILED.value,
        Status.COMPLETED.value,
    ]
    machine.dispatch("finish")
    assert machine.status is Status.COMPLETED


def test_fork_fail_fast_stops_waiting_siblings_and_fails_parent():
    builder = GraphBuilder("fork-fail-fast", initial="start")
    builder.action("start")
    builder.action(
        "forking",
        lambda ctx, event: Outcome.fork(
            {"event": "fail"},
            {"event": "wait"},
            join_event="joined",
            failure_policy="fail_fast",
        ),
    )
    builder.action("failing", lambda ctx, event: Outcome.fail("branch error"))
    builder.wait("waiting", resume_event="wake")
    builder.terminal("done")
    builder.transition("start", "fork", "forking")
    builder.transition("start", "fail", "failing")
    builder.transition("start", "wait", "waiting")
    builder.transition("forking", "joined", "done")
    machine = Machine(builder.build())

    machine.start()
    machine.dispatch("fork")
    waiting_child = machine.fork_group.children[1]
    assert machine.fork_group.finished is True

    machine.join()

    assert machine.status is Status.FAILED
    assert waiting_child.status is Status.STOPPED
    assert waiting_child.context.metadata["stop_reason"] == "fork_fail_fast"


def test_fail_fast_snapshot_restores_never_started_siblings():
    builder = GraphBuilder("fork-fail-fast-restore", initial="start")
    builder.action("start")
    builder.action(
        "forking",
        lambda ctx, event: Outcome.fork(
            {"event": "fail"},
            {"event": "finish"},
            failure_policy="fail_fast",
        ),
    )
    builder.action("failing", lambda ctx, event: Outcome.fail("branch error"))
    builder.terminal("done")
    builder.transition("start", "fork", "forking")
    builder.transition("start", "fail", "failing")
    builder.transition("start", "finish", "done")
    graph = builder.build()
    machine = Machine(graph)
    machine.start()
    machine.dispatch("fork")

    restored = Machine.from_snapshot(graph, machine.snapshot())

    assert restored.fork_group is not None
    assert [child.status for child in restored.fork_group.children] == [
        Status.FAILED,
        Status.STOPPED,
    ]
    assert restored.fork_group.children[1].node_id == graph.initial


def test_async_fork_continue_failure_policy_keeps_join_event_available():
    builder = GraphBuilder("async-fork-continue", initial="start")
    builder.action("start")
    builder.action(
        "forking",
        lambda ctx, event: Outcome.fork(
            {"event": "fail"},
            {"event": "finish"},
            join_event="joined",
            failure_policy="continue",
        ),
    )
    builder.action("failing", lambda ctx, event: Outcome.fail("branch error"))
    builder.terminal("child_done")
    builder.terminal("done")
    builder.transition("start", "fork", "forking")
    builder.transition("start", "fail", "failing")
    builder.transition("start", "finish", "child_done")
    builder.transition("forking", "joined", "done")
    machine = Machine(builder.build())

    async def run():
        await machine.start_async()
        await machine.dispatch_async("fork")
        joined = await machine.join_async()
        assert joined is not None

    asyncio.run(run())
    assert machine.status is Status.COMPLETED


def test_failed_join_dispatch_restores_completed_fork_for_retry():
    builder = GraphBuilder("join-retry", initial="start")
    builder.action("start")
    builder.action(
        "forking",
        lambda context, event: Outcome.fork(
            {"event": "finish"},
            join_event="joined",
        ),
    )
    builder.terminal("child_done")
    builder.terminal("done")
    builder.transition("start", "fork", "forking")
    builder.transition("start", "finish", "child_done")
    builder.transition(
        "forking",
        "joined",
        "done",
        guard=lambda context, event: context.get("allow_join", False),
    )
    machine = Machine(builder.build())
    machine.start()
    machine.dispatch("fork")
    group = machine.fork_group
    assert group is not None

    with pytest.raises(NoTransition):
        machine.join()

    assert machine.fork_group is group
    assert machine.status is Status.WAITING
    assert machine.context.waiting["fork_id"] == group.id

    machine.context.set("allow_join", True)
    machine.join()

    assert machine.fork_group is None
    assert machine.status is Status.COMPLETED


def test_join_transition_failure_restores_group_before_error_hooks():
    attempts = []

    def join_action(context, event):
        attempts.append(event.name)
        if len(attempts) == 1:
            raise RuntimeError("join action failed")

    builder = GraphBuilder("join-action-retry", initial="start")
    builder.action("start")
    builder.action(
        "forking",
        lambda context, event: Outcome.fork(
            {"event": "finish"},
            join_event="joined",
        ),
    )
    builder.terminal("child_done")
    builder.terminal("done")
    builder.transition("start", "fork", "forking")
    builder.transition("start", "finish", "child_done")
    builder.transition("forking", "joined", "done", action=join_action)
    machine = Machine(builder.build())
    observed = []
    machine.hooks.on(
        "transition.error",
        lambda hook: observed.append(
            (hook.context.status, hook.machine.fork_group is not None)
        ),
    )
    machine.start()
    machine.dispatch("fork")

    with pytest.raises(RuntimeError, match="join action failed"):
        machine.join()

    assert observed == [(Status.WAITING, True)]
    assert machine.status is Status.WAITING
    assert machine.fork_group is not None

    machine.join()

    assert attempts == ["joined", "joined"]
    assert machine.status is Status.COMPLETED


def test_async_fail_fast_cancels_a_running_sibling_before_join():
    started = asyncio.Event()
    cleaned = asyncio.Event()

    async def running(context, event):
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            cleaned.set()

    async def fail_after_started(context, event):
        await started.wait()
        return Outcome.fail("branch error")

    builder = GraphBuilder("async-fail-fast", initial="start")
    builder.action("start")
    builder.action(
        "forking",
        lambda ctx, event: Outcome.fork(
            {"event": "fail"},
            {"event": "run"},
            join_event="joined",
            failure_policy="fail_fast",
        ),
    )
    builder.action("failing", fail_after_started)
    builder.action("running", running)
    builder.terminal("done")
    builder.transition("start", "fork", "forking")
    builder.transition("start", "fail", "failing")
    builder.transition("start", "run", "running")
    builder.transition("forking", "joined", "done")
    machine = Machine(builder.build())

    async def run_machine():
        await machine.start_async()
        await machine.dispatch_async("fork")

    asyncio.run(run_machine())

    assert started.is_set()
    assert cleaned.is_set()
    assert machine.fork_group.children[1].status is Status.STOPPED
    machine.join()
    assert machine.status is Status.FAILED


def test_async_fork_cancellation_propagates_to_running_branches():
    token = CancellationToken()
    started = asyncio.Event()
    cleaned_up = asyncio.Event()

    async def long_running(context, event):
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            cleaned_up.set()

    builder = GraphBuilder("cancel-running-fork", initial="start")
    builder.action("start")
    builder.action(
        "forking",
        lambda ctx, event: Outcome.fork({"event": "run"}, join_event="joined"),
    )
    builder.action("running", long_running)
    builder.terminal("done")
    builder.transition("start", "fork", "forking")
    builder.transition("start", "run", "running")
    builder.transition("forking", "joined", "done")
    machine = Machine(builder.build(), cancellation=token)

    async def run():
        await machine.start_async()
        dispatch = asyncio.create_task(machine.dispatch_async("fork"))
        await started.wait()
        token.cancel()
        with pytest.raises(CancellationError):
            await dispatch

    asyncio.run(run())
    assert cleaned_up.is_set()
    assert machine.status is Status.FAILED


def test_workflow_and_parallel_semantics_are_composable_facades():
    builder = GraphBuilder("workflow", initial="ready")
    builder.action("ready")
    builder.terminal("done")
    builder.transition("ready", "go", "done")
    workflow = Workflow(builder.build())

    machine = workflow.run([("go", {"source": "test"})], data={"count": 1})
    assert machine.status is Status.COMPLETED
    assert workflow.topological_order() == ("ready", "done")

    plan = Parallel.plan(
        {"event": "branch"},
        policy=JoinPolicy.ANY,
        failure_policy="continue",
    )
    outcome = plan.outcome()
    assert outcome.policy == "any"
    assert outcome.failure_policy == "continue"
    assert outcome.branches[0].event == "branch"


def test_workflow_rejects_cycles_only_when_using_dag_semantics():
    builder = GraphBuilder("cyclic", initial="one")
    builder.action("one")
    builder.action("two")
    builder.transition("one", "next", "two")
    builder.transition("two", "back", "one")

    with pytest.raises(GraphValidationError, match="cycle"):
        Workflow(builder.build()).topological_order()


def test_workflow_treats_an_implicit_target_as_a_self_loop():
    builder = GraphBuilder("implicit-self-loop", initial="loop")
    builder.action("loop")
    builder.transition("loop", "again", None)

    with pytest.raises(GraphValidationError, match="cycle"):
        Workflow(builder.build()).topological_order()


def test_saga_runtime_records_transition_compensation_in_reverse_order():
    calls = []

    def compensate_first(ctx, event):
        calls.append("first")

    def compensate_second(ctx, event):
        calls.append("second")

    builder = GraphBuilder("saga", initial="ready")
    builder.action("ready")
    builder.action("middle")
    builder.terminal("done")
    builder.transition(
        "ready",
        "start",
        "middle",
        metadata={"compensate": compensate_first},
    )
    builder.transition(
        "middle",
        "finish",
        "done",
        metadata={"compensate": compensate_second},
    )
    machine = Machine(builder.build())
    plan = CompensationPlan(
        [CompensationStep("declared", lambda ctx, event: calls.append("declared"))]
    )
    saga = SagaRuntime(machine, plan)
    machine.start()
    machine.dispatch("start")
    machine.dispatch("finish")

    results = saga.compensate()
    assert [result.name for result in results] == [
        "middle:finish:done:1",
        "ready:start:middle:0",
    ]
    assert calls == ["second", "first"]
    assert saga.close() is True
