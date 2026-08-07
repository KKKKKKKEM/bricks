import asyncio
from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from bricks.engine import GraphBuilder, Machine, Outcome, Status
from bricks.engine.errors import MachineNotRunnable
from bricks.engine.scheduling import (
    AsyncWakeupBinding,
    InMemoryWakeupScheduler,
    WakeupBinding,
    dispatch_wakeup,
    dispatch_wakeup_async,
    wakeup_for,
)


NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


def test_wait_delay_becomes_a_durable_wakeup_and_resumes_the_run():
    builder = GraphBuilder("scheduled-wait", initial="ready")
    builder.action("ready")
    builder.wait("waiting", delay=5, resume_event="wake")
    builder.terminal("done")
    builder.transition("ready", "sleep", "waiting")
    builder.transition("waiting", "wake", "done")
    machine = Machine(builder.build(), clock=lambda: NOW)
    scheduler = InMemoryWakeupScheduler()
    WakeupBinding(machine, scheduler, clock=lambda: NOW).attach()

    machine.start()
    machine.dispatch("sleep")

    wakeup = scheduler.get(machine.context.run_id)
    assert wakeup is not None
    assert wakeup.due_at == NOW + timedelta(seconds=5)
    assert wakeup.to_dict()["event"] == "wake"
    assert scheduler.due(NOW + timedelta(seconds=4)) == []
    assert scheduler.due(NOW + timedelta(seconds=5)) == [wakeup]

    dispatch_wakeup(machine, wakeup)

    assert machine.status is Status.COMPLETED
    assert scheduler.get(machine.context.run_id) is None
    with pytest.raises(MachineNotRunnable, match="stale"):
        dispatch_wakeup(machine, wakeup)


def test_retry_delay_uses_the_same_external_wakeup_boundary():
    def work(context, event):
        if context.attempt == 0:
            return Outcome.retry(delay=2, reason="temporary")
        return Outcome.next("finish")

    builder = GraphBuilder("scheduled-retry", initial="work")
    builder.action("work", work)
    builder.terminal("done")
    builder.transition("work", "finish", "done")
    machine = Machine(builder.build(), clock=lambda: NOW)
    scheduler = InMemoryWakeupScheduler()
    WakeupBinding(machine, scheduler, clock=lambda: NOW).attach()

    machine.start()
    wakeup = scheduler.get(machine.context.run_id)

    assert wakeup is not None
    assert wakeup.kind == "retry"
    dispatch_wakeup(machine, wakeup)
    assert machine.status is Status.COMPLETED


def test_custom_retry_event_is_delivered_by_direct_and_scheduled_resume():
    direct_events = []

    def direct_work(context, event):
        direct_events.append(event.name)
        if event.name != "again":
            return Outcome.retry(event="again", delay=2)

    builder = GraphBuilder("custom-retry-direct", initial="work")
    builder.action("work", direct_work)
    direct = Machine(builder.build(), clock=lambda: NOW)
    direct.start()
    direct.resume_retry()

    assert direct_events == ["__start__", "again"]

    scheduled_events = []

    def scheduled_work(context, event):
        scheduled_events.append(event.name)
        if event.name != "again":
            return Outcome.retry(event="again", delay=2)

    scheduled_builder = GraphBuilder("custom-retry-scheduled", initial="work")
    scheduled_builder.action("work", scheduled_work)
    scheduled = Machine(scheduled_builder.build(), clock=lambda: NOW)
    scheduled.start()
    wakeup = wakeup_for(scheduled, now=NOW)
    assert wakeup is not None

    dispatch_wakeup(scheduled, wakeup)

    assert scheduled_events == ["__start__", "again"]


def test_async_wakeup_binding_awaits_an_external_scheduler():
    class Scheduler:
        def __init__(self):
            self.items = {}

        async def schedule(self, wakeup):
            await asyncio.sleep(0)
            self.items[wakeup.run_id] = wakeup

        async def cancel(self, run_id):
            await asyncio.sleep(0)
            self.items.pop(run_id, None)

    builder = GraphBuilder("async-scheduled", initial="waiting")
    builder.wait("waiting", delay=1, resume_event="wake")
    machine = Machine(builder.build(), clock=lambda: NOW)
    scheduler = Scheduler()
    AsyncWakeupBinding(machine, scheduler, clock=lambda: NOW).attach()

    asyncio.run(machine.start_async())

    wakeup = scheduler.items[machine.context.run_id]
    assert wakeup == wakeup_for(machine, now=NOW)


def test_async_wakeup_dispatch_resumes_the_run():
    builder = GraphBuilder("async-wakeup-dispatch", initial="waiting")
    builder.wait("waiting", delay=1, resume_event="wake")
    builder.terminal("done")
    builder.transition("waiting", "wake", "done")
    machine = Machine(builder.build(), clock=lambda: NOW)

    asyncio.run(machine.start_async())
    wakeup = wakeup_for(machine, now=NOW)
    assert wakeup is not None

    asyncio.run(dispatch_wakeup_async(machine, wakeup))

    assert machine.status is Status.COMPLETED


def test_wakeup_rejects_version_or_schedule_tampering():
    builder = GraphBuilder("versioned-wakeup", initial="waiting", version="2")
    builder.wait("waiting", delay=1, resume_event="wake")
    machine = Machine(builder.build(), clock=lambda: NOW)
    machine.start()
    wakeup = wakeup_for(machine, now=NOW)
    assert wakeup is not None

    with pytest.raises(MachineNotRunnable, match="graph version"):
        dispatch_wakeup(machine, replace(wakeup, graph_version="1"))
    with pytest.raises(MachineNotRunnable, match="stale"):
        dispatch_wakeup(machine, replace(wakeup, due_at=wakeup.due_at + timedelta(1)))


def test_wakeup_binding_can_reconcile_a_restored_waiting_run():
    builder = GraphBuilder("restored-schedule", initial="waiting")
    builder.wait("waiting", delay=3, resume_event="wake")
    graph = builder.build()
    machine = Machine(graph, clock=lambda: NOW)
    machine.start()
    restored = Machine.from_snapshot(graph, machine.snapshot())
    scheduler = InMemoryWakeupScheduler()
    binding = WakeupBinding(restored, scheduler, clock=lambda: NOW).attach()

    wakeup = binding.sync()

    assert wakeup is not None
    assert wakeup.due_at == NOW + timedelta(seconds=3)
    assert scheduler.get(restored.context.run_id) == wakeup
