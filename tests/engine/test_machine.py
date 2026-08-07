import asyncio

import pytest

from bricks.engine import Event, GraphBuilder, Machine, Outcome, Status
from bricks.engine.errors import (
    InternalStepLimitExceeded,
    NoTransition,
)
from bricks.engine.events import EventBus, RuntimeEvent
from bricks.engine.policies import InMemoryIdempotencyStore, RetryPolicy
from bricks.engine.runtime import (
    OutcomeDirective,
    default_outcome_interpreter,
)
from bricks.engine.runtime.outcomes import Retry


def test_control_outcome_stops_before_target_side_effects():
    entered = []
    builder = GraphBuilder("outcome-conflict", initial="ready")
    builder.action("ready")
    builder.action("target", lambda context, event: entered.append(event.name))
    builder.transition(
        "ready", "go", "target", action=lambda context, event: Outcome.fail("failed")
    )
    machine = Machine(builder.build())
    machine.start()

    result = machine.dispatch("go")

    assert machine.status is Status.FAILED
    assert machine.node_id == "ready"
    assert result.target == "ready"
    assert entered == []


def test_transition_effect_is_visible_to_target_enter_immediately():
    observed = []
    builder = GraphBuilder("stage-effect", initial="ready")
    builder.action("ready")
    builder.terminal(
        "target", lambda context, event: observed.append(context.get("value"))
    )
    builder.transition(
        "ready", "go", "target", action=lambda context, event: {"value": 42}
    )
    machine = Machine(builder.build())
    machine.start()

    machine.dispatch("go")

    assert observed == [42]
    assert machine.status is Status.COMPLETED


def test_custom_outcome_handler_extends_machine_without_core_type_branches():
    class Notify(Outcome):
        pass

    handled = []

    def handle_legacy(current, outcome):
        assert isinstance(current, Machine)
        handled.append(current.context.run_id)
    builder = GraphBuilder("custom-outcome", initial="ready")
    builder.action("ready", lambda context, event: Notify())
    machine = Machine(
        builder.build(),
        outcome_handlers={
            Notify: handle_legacy,
        },
    )

    machine.start()

    assert handled == [machine.context.run_id]
    assert machine.status is Status.RUNNING


def test_custom_effect_can_continue_the_transition_through_a_registry():
    class RememberPrompt(Outcome):
        pass

    entered = []
    interpreter = default_outcome_interpreter().with_handler(
        RememberPrompt,
        lambda runtime, outcome: runtime.update({"prompt": "inspect repository"}),
        directive=OutcomeDirective.CONTINUE,
    )
    builder = GraphBuilder("agent-effect", initial="plan")
    builder.action("plan")
    builder.terminal(
        "execute",
        lambda context, event: entered.append(context.get("prompt")),
    )
    builder.transition(
        "plan",
        "next",
        "execute",
        action=lambda context, event: RememberPrompt(),
    )
    machine = Machine(builder.build(), outcome_interpreter=interpreter)

    machine.start()
    result = machine.dispatch("next")

    assert entered == ["inspect repository"]
    assert result.target == "execute"
    assert machine.status is Status.COMPLETED


def test_outcome_registry_composition_does_not_mutate_shared_defaults():
    class Extension(Outcome):
        pass

    defaults = default_outcome_interpreter()
    extended = defaults.with_handler(Extension, lambda machine, outcome: None)

    assert Extension not in defaults.rules
    assert Extension in extended.rules


def test_async_custom_effect_uses_the_same_transition_directive():
    class AsyncEffect(Outcome):
        pass

    async def handle(machine, outcome):
        await asyncio.sleep(0)
        machine.update({"handled": True})

    interpreter = default_outcome_interpreter().with_handler(
        AsyncEffect,
        handle,
        directive=OutcomeDirective.CONTINUE,
    )
    builder = GraphBuilder("async-agent-effect", initial="ready")
    builder.action("ready")
    builder.terminal("done")
    builder.transition(
        "ready", "go", "done", action=lambda context, event: AsyncEffect()
    )
    machine = Machine(builder.build(), outcome_interpreter=interpreter)

    async def run():
        await machine.start_async()
        await machine.dispatch_async("go")

    asyncio.run(run())
    assert machine.context.get("handled") is True
    assert machine.status is Status.COMPLETED


def test_fork_children_inherit_the_composed_outcome_interpreter():
    class ChildEffect(Outcome):
        pass

    interpreter = default_outcome_interpreter().with_handler(
        ChildEffect,
        lambda runtime, outcome: runtime.update({"child_effect": True}),
        directive=OutcomeDirective.CONTINUE,
    )
    builder = GraphBuilder("fork-interpreter", initial="ready")
    builder.action("ready")
    builder.action(
        "forking", lambda context, event: Outcome.fork({"event": "child"})
    )
    builder.terminal("done")
    builder.transition("ready", "fork", "forking")
    builder.transition(
        "ready", "child", "done", action=lambda context, event: ChildEffect()
    )
    machine = Machine(builder.build(), outcome_interpreter=interpreter)

    machine.start()
    machine.dispatch("fork")

    assert machine.fork_group is not None
    child = machine.fork_group.children[0]
    assert child.status is Status.COMPLETED
    assert child.context.get("child_effect") is True


def test_wait_node_and_manual_pause_have_distinct_lifecycle_semantics():
    builder = GraphBuilder("wait", initial="ready")
    builder.action("ready")
    builder.wait("sleep", delay=1, resume_event="wake")
    builder.terminal("done")
    builder.transition("ready", "sleep", "sleep")
    builder.transition("sleep", "wake", "done")
    machine = Machine(builder.build())
    machine.start()

    machine.dispatch("sleep")
    assert machine.status is Status.WAITING
    assert machine.context.waiting["delay"] == 1
    assert machine.context.waiting["resume_event"] == "wake"
    assert "due_at" in machine.context.waiting
    machine.resume("wake")
    assert machine.status is Status.COMPLETED

    builder = GraphBuilder("pause", initial="ready")
    builder.action("ready")
    pause_machine = Machine(builder.build())
    pause_machine.start()
    pause_machine.pause()
    assert pause_machine.status is Status.PAUSED
    pause_machine.resume_run()
    assert pause_machine.status is Status.RUNNING


def test_wait_and_retry_reject_negative_delays_at_definition_time():
    builder = GraphBuilder("negative-wait", initial="waiting")
    with pytest.raises(ValueError, match="negative"):
        builder.wait("waiting", delay=-0.1)
    with pytest.raises(ValueError, match="negative"):
        Outcome.wait(-0.1)
    with pytest.raises(ValueError, match="negative"):
        Outcome.retry(delay=-0.1)


def test_failed_wait_resume_keeps_the_waiting_boundary():
    builder = GraphBuilder("failed-wait-resume", initial="ready")
    builder.action("ready")
    builder.wait("waiting", resume_event="wake")
    builder.terminal("done")
    builder.transition("ready", "pause", "waiting")
    builder.transition(
        "waiting",
        "wake",
        "done",
        guard=lambda context, event: context.get("allowed", False),
    )
    machine = Machine(builder.build())
    machine.start()
    machine.dispatch("pause")
    waiting = machine.context.waiting.copy()

    with pytest.raises(NoTransition):
        machine.resume("wake")

    assert machine.status is Status.WAITING
    assert machine.context.waiting == waiting
    assert machine.node_id == "waiting"

    machine.context.set("allowed", True)
    machine.resume("wake")
    assert machine.status is Status.COMPLETED


def test_failed_async_wait_resume_keeps_the_waiting_boundary():
    allowed = False

    async def guard(context, event):
        await asyncio.sleep(0)
        return allowed

    builder = GraphBuilder("failed-async-wait-resume", initial="ready")
    builder.action("ready")
    builder.wait("waiting", resume_event="wake")
    builder.terminal("done")
    builder.transition("ready", "pause", "waiting")
    builder.transition("waiting", "wake", "done", guard=guard)
    machine = Machine(builder.build())

    async def run():
        nonlocal allowed
        await machine.start_async()
        await machine.dispatch_async("pause")
        waiting = machine.context.waiting.copy()
        with pytest.raises(NoTransition):
            await machine.resume_async("wake")
        assert machine.status is Status.WAITING
        assert machine.context.waiting == waiting
        allowed = True
        await machine.resume_async("wake")

    asyncio.run(run())
    assert machine.status is Status.COMPLETED


def test_transition_action_none_does_not_hide_sync_exit_outcome():
    builder = GraphBuilder("exit-outcome", initial="ready")
    builder.action(
        "ready",
        on_exit=lambda context, event: Outcome.wait(resume_event="approve"),
    )
    builder.action("review")
    builder.transition("ready", "review", "review")
    machine = Machine(builder.build())
    machine.start()

    result = machine.dispatch("review")

    assert result.outcome == Outcome.wait(resume_event="approve")
    assert machine.status is Status.WAITING
    assert machine.context.waiting == {"delay": None, "resume_event": "approve"}


def test_async_transition_action_none_does_not_hide_async_exit_outcome():
    async def on_exit(context, event):
        await asyncio.sleep(0)
        return Outcome.wait(resume_event="approve")

    builder = GraphBuilder("async-exit-outcome", initial="ready")
    builder.action("ready", on_exit=on_exit)
    builder.action("review")
    builder.transition("ready", "review", "review")
    machine = Machine(builder.build())

    async def run():
        await machine.start_async()
        return await machine.dispatch_async("review")

    result = asyncio.run(run())

    assert result.outcome == Outcome.wait(resume_event="approve")
    assert machine.status is Status.WAITING
    assert machine.context.waiting == {"delay": None, "resume_event": "approve"}


def test_terminal_node_next_outcome_is_drained_before_completion():
    builder = GraphBuilder("terminal-next", initial="ready")
    builder.action("ready")
    builder.terminal("middle", lambda context, event: Outcome.next("finish"))
    builder.terminal("done")
    builder.transition("ready", "go", "middle")
    builder.transition("middle", "finish", "done")
    machine = Machine(builder.build())
    machine.start()

    machine.dispatch("go")

    assert machine.node_id == "done"
    assert machine.status is Status.COMPLETED


def test_async_terminal_node_next_outcome_is_drained_before_completion():
    async def continue_from_terminal(context, event):
        await asyncio.sleep(0)
        return Outcome.next("finish")

    builder = GraphBuilder("async-terminal-next", initial="ready")
    builder.action("ready")
    builder.terminal("middle", continue_from_terminal)
    builder.terminal("done")
    builder.transition("ready", "go", "middle")
    builder.transition("middle", "finish", "done")
    machine = Machine(builder.build())

    async def run():
        await machine.start_async()
        await machine.dispatch_async("go")

    asyncio.run(run())
    assert machine.node_id == "done"
    assert machine.status is Status.COMPLETED


def test_async_cancellation_during_guard_stops_machine_and_releases_claim():
    guard_started = asyncio.Event()
    idempotency = InMemoryIdempotencyStore()

    async def guard(context, event):
        guard_started.set()
        await asyncio.Event().wait()

    builder = GraphBuilder("cancel-guard", initial="ready")
    builder.action("ready")
    builder.action("working")
    builder.transition("ready", "run", "working", guard=guard)
    machine = Machine(builder.build(), idempotency=idempotency)
    event = Event("run", event_id="cancel-guard-event")

    async def run():
        await machine.start_async()
        task = asyncio.create_task(machine.dispatch_async(event))
        await guard_started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(run())

    assert machine.status is Status.STOPPED
    assert machine.context.metadata["stop_reason"] == "task_cancelled"
    assert idempotency.claim(f"{machine.context.run_id}:{event.event_id}") is True


def test_async_cancellation_during_before_dispatch_hook_stops_machine():
    hook_started = asyncio.Event()

    async def before_dispatch(hook):
        hook_started.set()
        await asyncio.Event().wait()

    builder = GraphBuilder("cancel-before-dispatch", initial="ready")
    builder.action("ready")
    builder.action("working")
    builder.transition("ready", "run", "working")
    machine = Machine(builder.build())
    machine.hooks.on("machine.before_dispatch", before_dispatch)

    async def run():
        await machine.start_async()
        task = asyncio.create_task(machine.dispatch_async("run"))
        await hook_started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(run())

    assert machine.status is Status.STOPPED
    assert machine.context.metadata["stop_reason"] == "task_cancelled"


def test_action_outcomes_emit_retry_and_failure_without_bypassing_edges():
    observed = []
    events = EventBus()
    events.on("audit", lambda event: observed.append(event.payload))

    builder = GraphBuilder("outcomes", initial="ready")
    builder.action("ready")
    builder.action("work", lambda ctx, event: Outcome.emit("audit", "started"))
    builder.terminal("done")
    builder.transition("ready", "run", "work")
    builder.transition("work", "finish", "done")
    machine = Machine(builder.build(), events=events)
    machine.start()
    machine.dispatch("run")

    assert observed == ["started"]
    assert machine.node_id == "work"
    machine.dispatch("finish")
    assert machine.status is Status.COMPLETED

    retry_builder = GraphBuilder("retry", initial="ready")
    retry_builder.action("ready")
    retry_builder.action("retrying", lambda ctx, event: Retry("again", 0.5))
    retry_builder.action("done")
    retry_builder.transition("ready", "go", "retrying")
    retry_builder.transition("retrying", "again", "done")
    retry_machine = Machine(retry_builder.build())
    retry_machine.start()
    retry_machine.dispatch("go")
    assert retry_machine.status is Status.WAITING
    assert retry_machine.context.attempt == 1


def test_runtime_events_include_outcome_emitted_messages():
    builder = GraphBuilder("emitted-events", initial="ready")
    builder.action("ready")
    builder.action("work", lambda ctx, event: Outcome.emit("audit", {"ok": True}))
    builder.transition("ready", "run", "work")
    machine = Machine(builder.build())

    runtime_events = list(machine.stream_events(["run"]))

    emitted = [item for item in runtime_events if item.name == "event.emitted"]
    assert len(emitted) == 1
    assert emitted[0].event_name == "audit"
    assert emitted[0].payload == {"ok": True}


def test_actions_can_return_context_data_updates_without_a_state_object():
    builder = GraphBuilder("updates", initial="ready")
    builder.action("ready", lambda ctx, event: {"count": 1, "source": event.name})
    builder.terminal("done", lambda ctx, event: {"finished": True})
    builder.transition("ready", "finish", "done")
    machine = Machine(builder.build())

    machine.start()
    machine.dispatch("finish")

    assert machine.context.data == {
        "count": 1,
        "source": "__start__",
        "finished": True,
    }
    assert machine.status is Status.COMPLETED


def test_update_context_changes_data_without_moving_or_changing_lifecycle():
    builder = GraphBuilder("context-update", initial="ready")
    builder.action("ready")
    machine = Machine(builder.build())

    machine.update_context({"source": "human"}, priority="high")

    assert machine.node_id is None
    assert machine.status is Status.CREATED
    assert machine.context.data == {
        "source": "human",
        "priority": "high",
    }

    machine.start()
    node_id = machine.node_id
    status = machine.status
    machine.update_context(approved=True)

    assert machine.node_id == node_id
    assert machine.status is status
    assert machine.context.data["approved"] is True


def test_update_context_hook_receives_internal_event_and_payload():
    observed = []
    builder = GraphBuilder("context-update-hook", initial="ready")
    builder.action("ready")
    machine = Machine(builder.build())
    machine.hooks.on(
        "context.updated",
        lambda hook: observed.append((hook.event.name, hook.event.payload)),
    )

    machine.update_context({"source": "human"}, priority="high")

    assert observed == [
        (
            "__context_update__",
            {"source": "human", "priority": "high"},
        )
    ]


def test_async_update_context_waits_for_async_hooks():
    observed = []

    async def hook(context):
        await asyncio.sleep(0)
        observed.append(context.event.payload)

    builder = GraphBuilder("async-context-update", initial="ready")
    builder.action("ready")
    machine = Machine(builder.build())
    machine.hooks.on("context.updated", hook)

    async def run():
        await machine.update_context_async(source="human")

    asyncio.run(run())

    assert observed == [{"source": "human"}]


def test_async_actions_and_hooks_use_the_same_graph_definition():
    calls = []

    async def action(ctx, event):
        await asyncio.sleep(0)
        calls.append(event.name)

    async def hook(value):
        await asyncio.sleep(0)
        calls.append(value.name)

    builder = GraphBuilder("async", initial="one")
    builder.action("one", action)
    builder.terminal("two")
    builder.transition("one", "next", "two")
    machine = Machine(builder.build())
    machine.hooks.on("transition.after", hook)

    async def run():
        await machine.start_async()
        await machine.dispatch_async("next")

    asyncio.run(run())
    assert calls == ["__start__", "transition.after"]


def test_next_outcome_drains_internal_events_without_recursive_public_dispatch():
    builder = GraphBuilder("next", initial="one")
    builder.action("one", lambda ctx, event: Outcome.next("advance", event.payload))
    builder.action("two", lambda ctx, event: Outcome.next("finish"))
    builder.terminal("done")
    builder.transition("one", "advance", "two")
    builder.transition("two", "finish", "done")

    machine = Machine(builder.build())
    machine.start()

    assert machine.node_id == "done"
    assert machine.status is Status.COMPLETED
    assert machine.context.last_event.name == "finish"


def test_next_can_update_context_before_guarded_internal_routing():
    builder = GraphBuilder("next-update", initial="start")
    builder.action(
        "start",
        lambda context, event: Outcome.next(
            "classify", update={"category": "priority"}
        ),
    )
    builder.terminal("priority")
    builder.terminal("normal")
    builder.transition(
        "start",
        "classify",
        "priority",
        guard=lambda context, event: context.get("category") == "priority",
    )
    builder.transition(
        "start",
        "classify",
        "normal",
        guard=lambda context, event: context.get("category") == "normal",
    )

    machine = Machine(builder.build())
    machine.start()

    assert machine.node_id == "priority"
    assert machine.context.get("category") == "priority"
    assert machine.status is Status.COMPLETED


def test_next_outcome_has_a_safety_limit():
    builder = GraphBuilder("loop", initial="loop")
    builder.action("loop", lambda ctx, event: Outcome.next("again"))
    builder.transition("loop", "again", "loop")

    machine = Machine(builder.build(), max_internal_steps=2)
    with pytest.raises(InternalStepLimitExceeded):
        machine.start()

    assert machine.status is Status.FAILED


def test_retry_policy_waits_then_reexecutes_the_current_node():
    attempts = []

    def retrying(ctx, event):
        attempts.append(ctx.attempt)
        if len(attempts) < 3:
            return Retry(reason="temporary")
        return None

    builder = GraphBuilder("retry-policy", initial="work")
    builder.action("work", retrying)
    machine = Machine(
        builder.build(),
        retry_policy=RetryPolicy(max_attempts=3, backoff=0.25, exponential=True),
    )

    machine.start()
    assert machine.status is Status.WAITING
    assert machine.context.waiting["delay"] == 0.25
    assert machine.context.attempt == 1

    machine.resume_retry()
    assert machine.status is Status.WAITING
    assert machine.context.waiting["delay"] == 0.5
    assert machine.context.attempt == 2

    machine.resume_retry()
    assert machine.status is Status.RUNNING
    assert machine.context.attempt == 0
    assert attempts == [0, 1, 2]


def test_after_resume_hook_error_does_not_turn_a_successful_retry_into_failure():
    builder = GraphBuilder("retry-hook-error", initial="work")

    def work(context, event):
        if context.attempt == 0:
            return Retry(reason="temporary")
        return None

    builder.action("work", work)
    machine = Machine(builder.build())
    machine.hooks.on(
        "machine.after_resume",
        lambda hook: (_ for _ in ()).throw(RuntimeError("retry audit failed")),
    )
    machine.start()

    with pytest.raises(RuntimeError, match="retry audit failed"):
        machine.resume_retry()

    assert machine.status is Status.RUNNING
    assert machine.context.attempt == 0


def test_retry_policy_marks_the_run_failed_after_exhaustion():
    builder = GraphBuilder("retry-exhausted", initial="work")
    builder.action("work", lambda ctx, event: Retry(reason="permanent"))
    machine = Machine(builder.build(), retry_policy=RetryPolicy(max_attempts=1))

    machine.start()
    assert machine.status is Status.WAITING
    machine.resume_retry()

    assert machine.status is Status.FAILED
    assert machine.context.metadata["retry_exhausted"]["attempt"] == 1


def test_async_next_uses_the_same_runtime_semantics():
    async def initial(ctx, event):
        return Outcome.next("go")

    async def finish(ctx, event):
        return None

    builder = GraphBuilder("async-outcomes", initial="start")
    builder.action("start", initial)
    builder.terminal("done", finish)
    builder.transition("start", "go", "done")
    machine = Machine(builder.build())

    async def run():
        await machine.start_async()

    asyncio.run(run())
    assert machine.status is Status.COMPLETED


def test_invoke_and_stream_provide_small_orchestration_entrypoints():
    builder = GraphBuilder("invoke", initial="ready")
    builder.action("ready")
    builder.wait("waiting", resume_event="wake")
    builder.terminal("done")
    builder.transition("ready", "pause", "waiting")
    builder.transition("waiting", "wake", "done")
    graph = builder.build()

    machine = Machine(graph)
    context = machine.invoke([("pause", {"source": "test"}), "wake"])
    assert context.status is Status.COMPLETED
    assert context.last_event.name == "wake"

    streamed = Machine(graph)
    results = list(streamed.stream(["pause", "wake"]))
    assert [result.event.name for result in results] == ["pause", "wake"]
    assert results[-1].status is Status.COMPLETED


def test_async_invoke_and_stream_use_the_same_input_contract():
    builder = GraphBuilder("async-invoke", initial="ready")
    builder.action("ready")
    builder.terminal("done")
    builder.transition("ready", "finish", "done")

    async def run():
        machine = Machine(builder.build())
        context = await machine.ainvoke(["finish"])
        assert context.status is Status.COMPLETED

        streamed = Machine(builder.build())
        results = [result async for result in streamed.astream(["finish"])]
        assert results[0].status is Status.COMPLETED

    asyncio.run(run())


def test_stream_events_exposes_ordered_runtime_facts_without_changing_stream():
    builder = GraphBuilder("runtime-events", initial="ready")
    builder.action("ready")
    builder.terminal("done")
    builder.transition("ready", "finish", "done")
    machine = Machine(builder.build())

    runtime_events = list(machine.stream_events(["finish"]))

    assert runtime_events
    assert all(isinstance(item, RuntimeEvent) for item in runtime_events)
    assert [item.sequence for item in runtime_events] == list(
        range(1, len(runtime_events) + 1)
    )
    assert all(item.run_id == machine.context.run_id for item in runtime_events)
    assert all(item.graph_id == "runtime-events" for item in runtime_events)
    assert runtime_events[0].name == "machine.before_start"
    assert any(item.name == "transition.after" for item in runtime_events)
    dispatch_events = [item for item in runtime_events if item.event_name == "finish"]
    assert dispatch_events
    assert dispatch_events[-1].transition_id == "ready:finish:done:0"
    assert dispatch_events[-1].to_dict()["run_id"] == machine.context.run_id


def test_stream_events_can_filter_observations_without_filtering_execution():
    builder = GraphBuilder("filtered-runtime-events", initial="ready")
    builder.action("ready")
    builder.terminal("done")
    builder.transition("ready", "finish", "done")
    machine = Machine(builder.build())

    filtered = list(
        machine.stream_events(
            ["finish"],
            match=lambda item: item.name == "transition.after",
        )
    )

    assert filtered
    assert all(item.name == "transition.after" for item in filtered)
    assert machine.status is Status.COMPLETED


def test_async_stream_events_can_filter_child_runtime_events():
    child = GraphBuilder("filtered-child-events", initial="start")
    child.action("start")
    child.terminal("done")
    child.transition("start", "finish", "done")

    parent = GraphBuilder("filtered-parent-events", initial="start")
    parent.action("start")
    parent.subgraph(
        "child_flow",
        child.build(),
        entry_event="finish",
        return_event="returned",
    )
    parent.terminal("done")
    parent.transition("start", "enter", "child_flow")
    parent.transition("child_flow", "returned", "done")
    machine = Machine(parent.build())

    async def run():
        return [
            item
            async for item in machine.astream_events(
                ["enter"],
                match=lambda item: item.graph_id == "filtered-child-events",
            )
        ]

    filtered = asyncio.run(run())

    assert filtered
    assert all(item.graph_id == "filtered-child-events" for item in filtered)
    assert machine.status is Status.WAITING


def test_async_stream_events_includes_child_run_identity():
    child = GraphBuilder("child-events", initial="start")
    child.action("start")
    child.terminal("done")
    child.transition("start", "finish", "done")

    parent = GraphBuilder("parent-events", initial="start")
    parent.action("start")
    parent.subgraph(
        "child_flow",
        child.build(),
        entry_event="finish",
        return_event="returned",
    )
    parent.terminal("done")
    parent.transition("start", "enter", "child_flow")
    parent.transition("child_flow", "returned", "done")
    machine = Machine(parent.build())

    async def run():
        return [
            item
            async for item in machine.astream_events(["enter"])
        ]

    runtime_events = asyncio.run(run())
    child_events = [item for item in runtime_events if item.graph_id == "child-events"]
    assert child_events
    assert all(item.parent_run_id == machine.context.run_id for item in child_events)
    assert machine.status is Status.WAITING


def test_async_stream_events_yields_before_a_long_action_finishes():
    release = asyncio.Event()

    async def blocked(context, event):
        await release.wait()

    builder = GraphBuilder("live-events", initial="ready")
    builder.action("ready", blocked)
    machine = Machine(builder.build())

    async def run():
        stream = machine.astream_events()
        observed = []
        while True:
            item = await asyncio.wait_for(anext(stream), timeout=0.5)
            observed.append(item)
            if item.name == "node.enter":
                release.set()
                break
        async for item in stream:
            observed.append(item)
        return observed

    observed = asyncio.run(run())
    names = [item.name for item in observed]
    assert names.index("node.enter") < names.index("machine.after_start")
    assert machine.status is Status.RUNNING


def test_external_async_task_cancellation_stops_the_machine():
    started = asyncio.Event()

    async def blocked(context, event):
        started.set()
        await asyncio.Event().wait()

    builder = GraphBuilder("task-cancel", initial="ready")
    builder.action("ready")
    builder.action("working", blocked)
    builder.transition("ready", "run", "working")
    machine = Machine(builder.build())

    async def run():
        await machine.start_async()
        dispatch = asyncio.create_task(machine.dispatch_async("run"))
        await started.wait()
        dispatch.cancel()
        with pytest.raises(asyncio.CancelledError):
            await dispatch

    asyncio.run(run())
    assert machine.status is Status.STOPPED
    assert machine.context.metadata["stop_reason"] == "task_cancelled"


def test_interrupt_is_a_named_wait_outcome_for_human_or_external_input():
    assert Outcome.interrupt(resume_event="approval") == Outcome.wait(
        resume_event="approval"
    )
