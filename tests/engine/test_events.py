import asyncio
import json

import pytest

from bricks.engine import GraphBuilder, Machine, Outcome, Status
from bricks.engine.errors import (
    AmbiguousEventRoute,
    AsyncActionRequired,
    DuplicateEvent,
)
from bricks.engine.events import EventBus
from bricks.engine.events.hooks import HookContext, HookRegistry
from bricks.engine.events.messages import Event, RuntimeEvent
from bricks.engine.persistence import (
    InMemoryEventLog,
    InMemorySnapshotStore,
    PersistenceBinding,
)
from bricks.engine.semantics import ReactiveRuntime


def test_reactive_runtime_routes_events_and_wait_resumption():
    builder = GraphBuilder("reactive", initial="ready")
    builder.action("ready")
    builder.wait("waiting", resume_event="wake")
    builder.terminal("done")
    builder.transition("ready", "pause", "waiting")
    builder.transition("waiting", "wake", "done")
    events = EventBus()
    machine = Machine(builder.build(), events=events)
    binding = ReactiveRuntime(events).attach(machine)

    events.publish("pause")
    assert machine.status is Status.WAITING
    events.publish("wake")
    assert machine.status is Status.COMPLETED
    assert binding.close() is True
    assert binding.close() is False


def test_reactive_runtime_requires_explicit_routing_for_multiple_runs():
    builder = GraphBuilder("reactive-routing", initial="ready")
    builder.action("ready")
    builder.terminal("done")
    builder.transition("ready", "finish", "done")
    graph = builder.build()
    events = EventBus()
    runtime = ReactiveRuntime(events)
    first = Machine(graph)
    second = Machine(graph)
    runtime.attach(first)
    runtime.attach(second)

    with pytest.raises(AmbiguousEventRoute):
        events.publish("finish")
    runtime.route(first.context.run_id, "finish")

    assert first.status is Status.COMPLETED
    assert second.status is Status.RUNNING


def test_reactive_route_ambiguity_spans_runtimes_sharing_one_bus():
    builder = GraphBuilder("shared-reactive-routing", initial="ready")
    builder.action("ready")
    builder.terminal("done")
    builder.transition("ready", "finish", "done")
    graph = builder.build()
    events = EventBus()
    first = Machine(graph)
    second = Machine(graph)
    ReactiveRuntime(events).attach(first)
    ReactiveRuntime(events).attach(second)

    with pytest.raises(AmbiguousEventRoute):
        events.publish("finish")

    assert first.status is Status.RUNNING
    assert second.status is Status.RUNNING


def test_reactive_runtime_continues_after_machine_restore():
    builder = GraphBuilder("reactive-restore", initial="created")
    builder.action("created")
    builder.wait("waiting", resume_event="approved")
    builder.terminal("done")
    builder.transition("created", "submitted", "waiting")
    builder.transition("waiting", "approved", "done")
    graph = builder.build()

    store = InMemorySnapshotStore()
    log = InMemoryEventLog()
    first_events = EventBus()
    machine = Machine(graph)
    PersistenceBinding(machine, store, log).attach()
    first_binding = ReactiveRuntime(first_events).attach(machine)
    first_events.publish(Event("submitted", {"id": "A-1"}, event_id="message-1"))

    assert machine.status is Status.WAITING
    run_id = machine.context.run_id
    first_binding.close()

    restored = PersistenceBinding.restore(graph, run_id, store, event_log=log)
    restarted_events = EventBus()
    second_binding = ReactiveRuntime(restarted_events).attach(
        restored, auto_start=False
    )
    restarted_events.publish(
        Event("approved", {"by": "alice"}, event_id="message-2")
    )

    assert restored.status is Status.COMPLETED
    assert restored.context.last_event is not None
    assert restored.context.last_event.name == "approved"
    assert second_binding.close() is True


def test_async_reactive_runtime_uses_async_machine_entrypoints():
    async def action(ctx, event):
        await asyncio.sleep(0)

    builder = GraphBuilder("async-reactive", initial="ready")
    builder.action("ready", action)
    builder.terminal("done")
    builder.transition("ready", "go", "done")
    events = EventBus()
    machine = Machine(builder.build(), events=events)
    runtime = ReactiveRuntime(events)

    async def run():
        await runtime.attach_async(machine)
        await events.publish_async("go")

    asyncio.run(run())
    assert machine.status is Status.COMPLETED


def test_async_reactive_runtime_has_a_directed_route_entrypoint():
    builder = GraphBuilder("async-directed-route", initial="ready")
    builder.action("ready")
    builder.terminal("done")
    builder.transition("ready", "go", "done")
    events = EventBus()
    machine = Machine(builder.build())
    runtime = ReactiveRuntime(events)

    async def run():
        await runtime.attach_async(machine)
        await runtime.route_async(machine.context.run_id, "go")

    asyncio.run(run())
    assert machine.status is Status.COMPLETED


def test_event_bus_orders_priority_then_subscription_and_supports_once_and_match():
    events = EventBus()
    calls = []
    events.subscribe("ready", lambda event: calls.append("late"), priority=10)
    events.subscribe("ready", lambda event: calls.append("first"), priority=-1)
    events.subscribe("ready", lambda event: calls.append("same-a"), priority=0)
    events.subscribe("ready", lambda event: calls.append("same-b"), priority=0)
    events.subscribe("ready", lambda event: calls.append("ignored"), match=lambda event: False)
    events.subscribe("*", lambda event: calls.append("wildcard"), priority=5)
    events.subscribe("ready", lambda event: calls.append("once"), once=True)

    events.publish("ready")
    events.publish("ready")

    assert calls == [
        "first",
        "same-a",
        "same-b",
        "once",
        "wildcard",
        "late",
        "first",
        "same-a",
        "same-b",
        "wildcard",
        "late",
    ]


def test_event_and_runtime_event_payloads_are_detached_from_external_mutation():
    payload = {"items": [1]}
    event = Event("copied", payload=payload)
    payload["items"].append(2)
    assert event.payload == {"items": [1]}

    runtime_payload = {"items": [1, 2]}
    runtime = RuntimeEvent(
        name="observed",
        run_id="run",
        graph_id="graph",
        sequence=1,
        payload=runtime_payload,
    )
    runtime_payload["items"].append(3)
    assert runtime.payload == {"items": [1, 2]}
    with pytest.raises(AttributeError):
        event.payload["items"].append(3)
    assert event.payload["items"] != [2]
    assert json.loads(json.dumps(event.to_dict()))["payload"] == {"items": [1]}


def test_sync_publish_rejects_async_handlers_and_async_publish_awaits_them():
    calls = []

    async def handler(event):
        await asyncio.sleep(0)
        calls.append(event.payload)

    events = EventBus()
    events.on("work", handler)
    with pytest.raises(AsyncActionRequired):
        events.publish(Event("work", payload="sync"))

    asyncio.run(events.publish_async(Event("work", payload="async")))
    assert calls == ["async"]


def test_sync_publish_preflights_async_handlers_before_sync_side_effects():
    calls = []
    events = EventBus()
    events.on(
        "work",
        lambda event: calls.append("sync"),
        priority=-1,
        once=True,
    )

    async def async_handler(event):
        calls.append("async")

    events.on("work", async_handler)

    with pytest.raises(AsyncActionRequired):
        events.publish("work")

    assert calls == []
    asyncio.run(events.publish_async("work"))
    assert calls == ["sync", "async"]


def test_outcome_emit_reaches_reactive_machine_after_outer_transition_commits():
    events = EventBus()
    builder = GraphBuilder("deferred-emit", initial="ready")
    builder.action("ready")
    builder.action("work")
    builder.terminal("done")
    builder.transition(
        "ready",
        "go",
        "work",
        action=lambda context, event: Outcome.emit("advance"),
    )
    builder.transition("ready", "advance", "done")
    builder.transition("work", "advance", "done")
    machine = Machine(builder.build(), events=events)
    ReactiveRuntime(events).attach(machine)

    events.publish("go")

    assert machine.node_id == "done"
    assert machine.status is Status.COMPLETED


def test_sync_hook_preflights_async_handlers_before_sync_side_effects():
    calls = []
    hooks = HookRegistry()
    hooks.on("change", lambda hook: calls.append("sync"), priority=-1)

    async def async_hook(hook):
        calls.append("async")

    hooks.on("change", async_hook)
    context = HookContext(name="change", machine=None, context=None)

    with pytest.raises(AsyncActionRequired):
        hooks.emit("change", context)

    assert calls == []


def test_hooks_have_the_same_order_filter_once_and_async_contract():
    hooks = HookRegistry()
    calls = []

    def context(name="hook"):
        return HookContext(name=name, machine=None, context=None)

    hooks.on("transition.after", lambda hook: calls.append("late"), priority=2)
    hooks.on("transition.after", lambda hook: calls.append("early"), priority=-1)
    hooks.on("transition.after", lambda hook: calls.append("once"), once=True)
    hooks.on(
        "transition.after",
        lambda hook: calls.append("ignored"),
        match=lambda hook: False,
    )
    hooks.emit("transition.after", context())
    hooks.emit("transition.after", context())
    assert calls == ["early", "once", "late", "early", "late"]

    async def async_hook(hook):
        await asyncio.sleep(0)
        calls.append("async")

    hooks.on("async", async_hook)
    with pytest.raises(AsyncActionRequired):
        hooks.emit("async", context("async"))
    asyncio.run(hooks.emit_async("async", context("async")))
    assert calls[-1] == "async"


def test_machine_lifecycle_hooks_receive_the_start_event():
    builder = GraphBuilder("lifecycle", initial="ready")
    builder.action("ready")
    machine = Machine(builder.build())
    events = []
    machine.hooks.on("machine.before_start", lambda hook: events.append(hook.event.name))
    machine.hooks.on("machine.after_start", lambda hook: events.append(hook.event.name))

    machine.start()

    assert events == ["__start__", "__start__"]


def test_after_resume_hook_runs_after_wait_and_retry_work_is_finished():
    wait_builder = GraphBuilder("after-resume-wait", initial="ready")
    wait_builder.action("ready")
    wait_builder.wait("waiting", resume_event="wake")
    wait_builder.terminal("done")
    wait_builder.transition("ready", "pause", "waiting")
    wait_builder.transition("waiting", "wake", "done")
    wait_machine = Machine(wait_builder.build())
    wait_events = []
    wait_machine.hooks.on(
        "machine.after_resume",
        lambda hook: wait_events.append((hook.event.name, hook.result.status)),
    )
    wait_machine.start()
    wait_machine.dispatch("pause")
    wait_machine.resume("wake")

    assert wait_events == [("wake", Status.COMPLETED)]

    retry_builder = GraphBuilder("after-resume-retry", initial="work")
    retry_builder.action("work", lambda context, event: Outcome.retry())
    retry_machine = Machine(retry_builder.build())
    retry_events = []
    retry_machine.hooks.on(
        "machine.after_resume",
        lambda hook: retry_events.append(hook.event.name),
    )
    retry_machine.start()
    retry_machine.resume_retry()

    assert retry_events == ["__retry__"]


def test_hook_errors_propagate_without_rolling_back_completed_transition():
    builder = GraphBuilder("hook-error", initial="ready")
    builder.action("ready")
    builder.terminal("done")
    builder.transition("ready", "finish", "done")
    machine = Machine(builder.build())
    machine.start()

    def fail_after(hook):
        raise RuntimeError("audit hook failed")

    machine.hooks.on("transition.after", fail_after)
    with pytest.raises(RuntimeError, match="audit hook failed"):
        machine.dispatch("finish")

    assert machine.node_id == "done"
    assert machine.status is Status.COMPLETED


def test_hook_error_after_successful_transition_does_not_release_idempotency_key():
    builder = GraphBuilder("hook-idempotency", initial="ready")
    builder.action("ready")
    builder.transition("ready", "finish", "ready")
    from bricks.engine.policies import InMemoryIdempotencyStore

    idempotency = InMemoryIdempotencyStore()
    machine = Machine(builder.build(), idempotency=idempotency)
    machine.start()

    machine.hooks.on(
        "transition.after",
        lambda hook: (_ for _ in ()).throw(RuntimeError("audit failed")),
    )
    event = Event("finish", event_id="finish-1")
    with pytest.raises(RuntimeError, match="audit failed"):
        machine.dispatch(event)

    assert machine.status is Status.RUNNING
    with pytest.raises(DuplicateEvent):
        machine.dispatch(event)


def test_after_start_hook_error_does_not_mark_a_started_run_failed():
    builder = GraphBuilder("start-hook-error", initial="ready")
    builder.action("ready")
    machine = Machine(builder.build())
    machine.hooks.on(
        "machine.after_start",
        lambda hook: (_ for _ in ()).throw(RuntimeError("start audit failed")),
    )

    with pytest.raises(RuntimeError, match="start audit failed"):
        machine.start()

    assert machine.status is Status.RUNNING


def test_after_resume_hook_error_does_not_release_the_resumed_event():
    builder = GraphBuilder("resume-hook-idempotency", initial="ready")
    builder.action("ready")
    builder.wait("waiting", resume_event="wake")
    builder.transition("ready", "pause", "waiting")
    builder.transition("waiting", "wake", "ready")
    from bricks.engine.policies import InMemoryIdempotencyStore

    idempotency = InMemoryIdempotencyStore()
    machine = Machine(builder.build(), idempotency=idempotency)
    machine.start()
    machine.dispatch("pause")
    machine.hooks.on(
        "machine.after_resume",
        lambda hook: (_ for _ in ()).throw(RuntimeError("resume audit failed")),
    )
    event = Event("wake", event_id="wake-1")

    with pytest.raises(RuntimeError, match="resume audit failed"):
        machine.resume(event)

    assert machine.status is Status.RUNNING
    with pytest.raises(DuplicateEvent):
        machine.dispatch(event)


def test_transition_hook_error_during_resume_keeps_the_event_claim():
    calls = []
    builder = GraphBuilder("resume-transition-hook", initial="ready")
    builder.action("ready")
    builder.wait("waiting", resume_event="wake")
    builder.transition("ready", "pause", "waiting")
    builder.transition(
        "waiting",
        "wake",
        "waiting",
        action=lambda context, event: calls.append(event.event_id),
    )
    from bricks.engine.policies import InMemoryIdempotencyStore

    machine = Machine(
        builder.build(),
        idempotency=InMemoryIdempotencyStore(),
    )
    machine.start()
    machine.dispatch("pause")
    machine.hooks.on(
        "transition.after",
        lambda hook: (_ for _ in ()).throw(RuntimeError("audit failed")),
        once=True,
    )
    event = Event("wake", event_id="wake-transition-1")

    with pytest.raises(RuntimeError, match="audit failed"):
        machine.resume(event)

    assert machine.status is Status.WAITING
    with pytest.raises(DuplicateEvent):
        machine.resume(event)
    assert calls == ["wake-transition-1"]


def test_async_transition_hook_error_during_resume_keeps_the_event_claim():
    calls = []
    builder = GraphBuilder("async-resume-transition-hook", initial="ready")
    builder.action("ready")
    builder.wait("waiting", resume_event="wake")
    builder.transition("ready", "pause", "waiting")
    builder.transition(
        "waiting",
        "wake",
        "waiting",
        action=lambda context, event: calls.append(event.event_id),
    )
    from bricks.engine.policies import InMemoryIdempotencyStore

    machine = Machine(
        builder.build(),
        idempotency=InMemoryIdempotencyStore(),
    )
    event = Event("wake", event_id="wake-transition-async-1")

    async def run():
        await machine.start_async()
        await machine.dispatch_async("pause")

        async def fail_after(hook):
            raise RuntimeError("async audit failed")

        machine.hooks.on("transition.after", fail_after, once=True)
        with pytest.raises(RuntimeError, match="async audit failed"):
            await machine.resume_async(event)
        with pytest.raises(DuplicateEvent):
            await machine.resume_async(event)

    asyncio.run(run())

    assert machine.status is Status.WAITING
    assert calls == ["wake-transition-async-1"]


def test_persistence_can_commit_before_a_user_transition_hook_fails():
    builder = GraphBuilder("persistence-hook-order", initial="ready")
    builder.action("ready")
    builder.transition("ready", "tick", "ready")
    machine = Machine(builder.build())
    store = InMemorySnapshotStore()
    log = InMemoryEventLog()
    machine.hooks.on(
        "transition.after",
        lambda hook: (_ for _ in ()).throw(RuntimeError("audit failed")),
    )
    PersistenceBinding(machine, store, log).attach()
    machine.start()

    with pytest.raises(RuntimeError, match="audit failed"):
        machine.dispatch(Event("tick", event_id="tick-1"))

    saved = store.load(machine.context.run_id)
    assert saved is not None
    assert saved.context["node_id"] == "ready"
    assert saved.context["status"] == Status.RUNNING.value
