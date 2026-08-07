import asyncio
import time

import pytest

from bricks import Event
from bricks.engine import GraphBuilder, Machine, Outcome, Status
from bricks.engine.errors import ActionTimeout, CancellationError, DuplicateEvent
from bricks.engine.policies import (
    CancellationToken,
    InMemoryIdempotencyStore,
    RetryPolicy,
    TimeoutPolicy,
)
from bricks.engine.runtime.outcomes import ForkBranch


def _linear_graph(action=None):
    builder = GraphBuilder("policies", initial="ready")
    builder.action("ready", action)
    builder.terminal("done")
    builder.transition("ready", "finish", "done")
    return builder.build()


def test_cancelled_machine_does_not_start_or_execute_an_action():
    calls = []
    token = CancellationToken()
    token.cancel()
    machine = Machine(_linear_graph(lambda ctx, event: calls.append("called")), cancellation=token)

    with pytest.raises(CancellationError):
        machine.start()

    assert calls == []
    assert machine.status is Status.CREATED


def test_cancellation_after_action_is_detected_before_the_transition_finishes():
    token = CancellationToken()

    def cancel(context, event):
        token.cancel()

    machine = Machine(_linear_graph(cancel), cancellation=token)
    with pytest.raises(CancellationError):
        machine.start()

    assert machine.status is Status.FAILED


def test_sync_timeout_is_checked_after_action_returns():
    def slow(context, event):
        time.sleep(0.01)

    machine = Machine(_linear_graph(slow), timeout=TimeoutPolicy(0.001))
    with pytest.raises(ActionTimeout):
        machine.start()

    assert machine.status is Status.FAILED


def test_timeout_wraps_a_custom_action_executor():
    calls = []

    class Executor:
        def execute(self, action, context, event):
            calls.append(event.name)
            return action(context, event)

        async def execute_async(self, action, context, event):
            calls.append(event.name)
            result = action(context, event)
            if hasattr(result, "__await__"):
                return await result
            return result

    def slow(context, event):
        time.sleep(0.01)

    machine = Machine(
        _linear_graph(slow),
        executor=Executor(),
        timeout=TimeoutPolicy(0.001),
    )
    with pytest.raises(ActionTimeout):
        machine.start()

    assert calls == ["__start__"]
    assert machine.status is Status.FAILED


def test_cancellation_is_checked_around_a_custom_action_executor():
    token = CancellationToken()

    class Executor:
        def execute(self, action, context, event):
            value = action(context, event)
            token.cancel()
            return value

        async def execute_async(self, action, context, event):
            value = action(context, event)
            if hasattr(value, "__await__"):
                value = await value
            token.cancel()
            return value

    machine = Machine(
        _linear_graph(lambda context, event: None),
        executor=Executor(),
        cancellation=token,
    )
    with pytest.raises(CancellationError):
        machine.start()

    assert machine.status is Status.FAILED


def test_retry_policy_can_opt_into_retrying_matching_node_exceptions():
    attempts = []

    def work(context, event):
        attempts.append(context.attempt)
        if len(attempts) == 1:
            raise ValueError("temporary")

    builder = GraphBuilder("exception-retry", initial="work")
    builder.terminal("work", work)
    machine = Machine(
        builder.build(),
        retry_policy=RetryPolicy(
            max_attempts=2,
            backoff=0.25,
            retry_on=(ValueError,),
        ),
    )

    machine.start()

    assert machine.status is Status.WAITING
    assert machine.context.attempt == 1
    assert machine.context.waiting["reason"] == {
        "type": "ValueError",
        "message": "temporary",
    }

    machine.resume_retry()

    assert machine.status is Status.COMPLETED
    assert attempts == [0, 1]


def test_retry_policy_does_not_intercept_unmatched_node_exceptions():
    builder = GraphBuilder("exception-no-retry", initial="work")
    builder.action(
        "work",
        lambda context, event: (_ for _ in ()).throw(ValueError("bad")),
    )
    machine = Machine(
        builder.build(),
        retry_policy=RetryPolicy(retry_on=(TypeError,)),
    )

    with pytest.raises(ValueError, match="bad"):
        machine.start()

    assert machine.status is Status.FAILED


def test_async_retry_policy_can_recover_from_a_matching_node_exception():
    attempts = 0

    async def work(context, event):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise LookupError("temporary async error")

    builder = GraphBuilder("async-exception-retry", initial="work")
    builder.terminal("work", work)
    machine = Machine(
        builder.build(),
        retry_policy=RetryPolicy(max_attempts=2, retry_on=(LookupError,)),
    )

    async def run():
        await machine.start_async()
        assert machine.status is Status.WAITING
        await machine.resume_retry_async()

    asyncio.run(run())
    assert machine.status is Status.COMPLETED
    assert attempts == 2


def test_async_timeout_cancels_an_awaiting_action():
    async def slow(context, event):
        await asyncio.sleep(0.05)

    machine = Machine(_linear_graph(slow), timeout=TimeoutPolicy(0.001))

    async def run():
        with pytest.raises(ActionTimeout):
            await machine.start_async()

    asyncio.run(run())
    assert machine.status is Status.FAILED


def test_duplicate_event_is_rejected_and_failed_dispatch_releases_the_key():
    events = InMemoryIdempotencyStore()
    builder = GraphBuilder("idempotent", initial="ready")
    builder.action("ready")
    builder.action("middle")
    builder.transition("ready", "finish", "middle")
    graph = builder.build()
    machine = Machine(graph, idempotency=events)
    machine.start()

    event = Event("finish")
    machine.dispatch(event)
    with pytest.raises(DuplicateEvent):
        machine.dispatch(event)

    failing_builder = GraphBuilder("idempotent-failure", initial="ready")
    failing_builder.action("ready")
    failing_builder.terminal("done")
    failing_builder.transition(
        "ready",
        "finish",
        "done",
        action=lambda ctx, event: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    failing = Machine(failing_builder.build(), idempotency=events)
    failing.start()
    failed_event = Event("finish")
    with pytest.raises(RuntimeError, match="boom"):
        failing.dispatch(failed_event)

    assert events.claim(f"{failing.context.run_id}:{failed_event.event_id}") is True


def test_idempotency_store_contract_claims_once_and_releases():
    store = InMemoryIdempotencyStore()

    assert store.claim("event-1") is True
    assert store.claim("event-1") is False
    store.release("event-1")
    assert store.claim("event-1") is True


def test_fork_children_inherit_cancellation_timeout_and_idempotency_policies():
    token = CancellationToken()
    store = InMemoryIdempotencyStore()

    def fork(context, event):
        return Outcome.fork(ForkBranch("finish"), join_event="joined")

    builder = GraphBuilder("policy-fork", initial="start")
    builder.action("start")
    builder.action("forking", fork)
    builder.terminal("child_done")
    builder.terminal("done")
    builder.transition("start", "fork", "forking")
    builder.transition("start", "finish", "child_done")
    builder.transition("forking", "joined", "done")
    machine = Machine(
        builder.build(),
        cancellation=token,
        timeout=TimeoutPolicy(1),
        idempotency=store,
    )
    machine.start()
    machine.dispatch("fork")

    child = machine.fork_group.children[0]
    assert child.cancellation is token
    assert child.timeout == machine.timeout
    assert child.idempotency is store


def test_fork_children_inherit_the_custom_action_executor():
    class Executor:
        def execute(self, action, context, event):
            return action(context, event)

        async def execute_async(self, action, context, event):
            result = action(context, event)
            if hasattr(result, "__await__"):
                return await result
            return result

    executor = Executor()
    builder = GraphBuilder("executor-fork", initial="start")
    builder.action("start")
    builder.action(
        "forking",
        lambda context, event: Outcome.fork(
            {"event": "finish"}, join_event="joined"
        ),
    )
    builder.terminal("child_done")
    builder.terminal("done")
    builder.transition("start", "fork", "forking")
    builder.transition("start", "finish", "child_done")
    builder.transition("forking", "joined", "done")
    machine = Machine(builder.build(), executor=executor)

    machine.start()
    machine.dispatch("fork")

    assert machine.fork_group.children[0].executor is executor
