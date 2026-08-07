import asyncio

import pytest
from typing import Any

from bricks.engine import Context, Event, GraphBuilder, Machine, Outcome, Status
from bricks.engine.errors import (
    AsyncActionRequired,
    DuplicateEvent,
    PersistenceError,
    SnapshotConflictError,
)
from bricks.engine.persistence import (
    AsyncAtomicPersistenceBinding,
    AsyncPersistenceBinding,
    AtomicCommit,
    AtomicPersistenceBinding,
    CURRENT_SNAPSHOT_VERSION,
    ContextSnapshot,
    EventRecord,
    InMemoryEventLog,
    InMemoryAtomicCommitStore,
    InMemorySnapshotStore,
    PersistenceBinding,
    replay_events,
)
from bricks.engine.runtime import (
    OutcomeDirective,
    StagedEffect,
    default_outcome_interpreter,
)
from bricks.engine.policies import InMemoryIdempotencyStore, RetryPolicy


def test_snapshot_rejects_a_context_with_a_different_run_id():
    context = Context(graph_id="graph", run_id="actual")

    with pytest.raises(ValueError, match="context run_id"):
        ContextSnapshot("graph", "indexed", context.snapshot())


def test_async_persistence_binding_awaits_async_storage():
    class Store:
        def __init__(self):
            self.snapshot = None

        async def save(self, snapshot):
            await asyncio.sleep(0)
            self.snapshot = snapshot

        async def load(self, run_id):
            if self.snapshot and self.snapshot.run_id == run_id:
                return self.snapshot
            return None

        async def delete(self, run_id):
            self.snapshot = None

    builder = GraphBuilder("async-store", initial="ready")
    builder.action("ready")
    machine = Machine(builder.build())
    store = Store()
    AsyncPersistenceBinding(machine, store).attach()

    asyncio.run(machine.start_async())

    assert store.snapshot is not None
    assert store.snapshot.run_id == machine.context.run_id


def test_async_persistence_binding_supports_pause_and_resume():
    class Store:
        def __init__(self):
            self.snapshots = []

        async def save(self, snapshot):
            self.snapshots.append(snapshot)

        async def load(self, run_id):
            return None

        async def delete(self, run_id):
            return None

    builder = GraphBuilder("async-pause", initial="ready")
    builder.action("ready")
    machine = Machine(builder.build())
    store = Store()
    AsyncPersistenceBinding(machine, store).attach()

    async def run():
        await machine.start_async()
        with pytest.raises(AsyncActionRequired):
            machine.pause()
        assert machine.status is Status.RUNNING
        await machine.pause_async()
        await machine.resume_run_async()

    asyncio.run(run())
    assert machine.status is Status.RUNNING
    assert [item.context["status"] for item in store.snapshots] == [
        Status.RUNNING.value,
        Status.PAUSED.value,
        Status.RUNNING.value,
        Status.RUNNING.value,
    ]


def test_atomic_binding_commits_snapshot_event_and_domain_effect_together():
    class Schedule(Outcome):
        pass

    seen_runtime = []

    def schedule(runtime, outcome):
        seen_runtime.append(runtime)
        runtime.stage_effect(
            "spider.request",
            {"url": "https://example.test"},
            effect_id="request-1",
        )

    interpreter = default_outcome_interpreter().with_handler(
        Schedule,
        schedule,
        directive=OutcomeDirective.CONTINUE,
    )
    builder = GraphBuilder("atomic-spider", initial="ready")
    builder.action("ready")
    builder.terminal("done")
    builder.transition(
        "ready", "crawl", "done", action=lambda context, event: Schedule()
    )
    machine = Machine(builder.build(), outcome_interpreter=interpreter)
    store = InMemoryAtomicCommitStore()
    AtomicPersistenceBinding(machine, store).attach()

    machine.start()
    machine.dispatch("crawl")

    assert not hasattr(seen_runtime[0], "dispatch")
    assert not hasattr(seen_runtime[0].context, "set")
    with pytest.raises(TypeError):
        seen_runtime[0].context.data["unsafe"] = True
    assert machine.staged_effects == ()
    saved = store.load(machine.context.run_id)
    assert saved is not None
    assert saved.context["status"] == "completed"
    assert [record.kind for record in store.read_events(machine.context.run_id)] == [
        "start",
        "event",
    ]
    effects = store.pending_effects(topic="spider.request")
    assert [effect.id for effect in effects] == ["request-1"]
    assert effects[0].payload == {"url": "https://example.test"}
    store.mark_effect_sent("request-1")
    assert store.pending_effects() == []


def test_atomic_binding_gives_pre_registration_fork_effects_a_child_snapshot():
    class Effect(Outcome):
        pass

    class RecordingStore:
        def __init__(self):
            self.inner = InMemoryAtomicCommitStore()
            self.batches = []

        def commit(self, batch):
            self.batches.append(batch)
            return self.inner.commit(batch)

        def load(self, run_id):
            return self.inner.load(run_id)

    interpreter = default_outcome_interpreter().with_handler(
        Effect,
        lambda runtime, outcome: runtime.stage_effect(
            "child.work", effect_id=f"effect:{runtime.run_id}"
        ),
        directive=OutcomeDirective.CONTINUE,
    )
    builder = GraphBuilder("atomic-fork-child", initial="start")
    builder.action("start")
    builder.action(
        "forking", lambda context, event: Outcome.fork({"event": "work"})
    )
    builder.terminal("child_done")
    builder.transition("start", "fork", "forking")
    builder.transition(
        "start", "work", "child_done", action=lambda context, event: Effect()
    )
    machine = Machine(builder.build(), outcome_interpreter=interpreter)
    store = RecordingStore()
    AtomicPersistenceBinding(machine, store).attach()

    machine.start()
    machine.dispatch("fork")

    assert machine.fork_group is not None
    child = machine.fork_group.children[0]
    effect_batch = next(batch for batch in store.batches if batch.effects)
    assert effect_batch.snapshot is not None
    assert effect_batch.snapshot.run_id == child.context.run_id
    assert store.load(child.context.run_id) is not None
    assert [effect.id for effect in store.inner.pending_effects()] == [
        f"effect:{child.context.run_id}"
    ]


def test_failed_atomic_commit_retains_the_exact_batch_for_flush():
    class Effect(Outcome):
        pass

    class FlakyStore:
        def __init__(self):
            self.inner = InMemoryAtomicCommitStore()
            self.fail = False

        def commit(self, batch):
            if self.fail:
                raise OSError("database unavailable")
            return self.inner.commit(batch)

        def load(self, run_id):
            return self.inner.load(run_id)

    interpreter = default_outcome_interpreter().with_handler(
        Effect,
        lambda runtime, outcome: runtime.stage_effect(
            "work", {"id": 1}, effect_id="effect-1"
        ),
        directive=OutcomeDirective.CONTINUE,
    )
    builder = GraphBuilder("atomic-retry", initial="ready")
    builder.action("ready")
    builder.terminal("done")
    builder.transition(
        "ready", "finish", "done", action=lambda context, event: Effect()
    )
    machine = Machine(builder.build(), outcome_interpreter=interpreter)
    store = FlakyStore()
    binding = AtomicPersistenceBinding(machine, store).attach()
    machine.start()
    store.fail = True

    with pytest.raises(OSError, match="database unavailable"):
        machine.dispatch("finish")

    assert machine.status is Status.COMPLETED
    assert [effect.id for effect in machine.staged_effects] == ["effect-1"]
    store.fail = False
    binding.flush()
    assert machine.staged_effects == ()
    assert [effect.id for effect in store.inner.pending_effects()] == ["effect-1"]


def test_atomic_flush_recognizes_a_commit_whose_response_was_lost():
    class LostResponseStore:
        def __init__(self):
            self.inner = InMemoryAtomicCommitStore()
            self.lose_response = False
            self.commit_ids = []

        def commit(self, batch):
            self.commit_ids.append(batch.commit_id)
            saved = self.inner.commit(batch)
            if self.lose_response:
                self.lose_response = False
                raise OSError("commit response lost")
            return saved

        def load(self, run_id):
            return self.inner.load(run_id)

    builder = GraphBuilder("atomic-lost-response", initial="ready")
    builder.action("ready")
    builder.terminal("done")
    builder.transition("ready", "finish", "done")
    machine = Machine(builder.build())
    store = LostResponseStore()
    binding = AtomicPersistenceBinding(machine, store).attach()
    machine.start()
    store.lose_response = True

    with pytest.raises(OSError, match="response lost"):
        machine.dispatch("finish")

    saved = binding.flush()

    assert saved is not None
    assert machine.context.metadata["snapshot_revision"] == saved.revision
    assert store.commit_ids[-2:] == [store.commit_ids[-1]] * 2


def test_async_atomic_binding_awaits_one_commit_boundary():
    class Store:
        def __init__(self):
            self.inner = InMemoryAtomicCommitStore()

        async def commit(self, batch):
            await asyncio.sleep(0)
            return self.inner.commit(batch)

        async def load(self, run_id):
            await asyncio.sleep(0)
            return self.inner.load(run_id)

    builder = GraphBuilder("async-atomic", initial="ready")
    builder.action("ready")
    machine = Machine(builder.build())
    store = Store()
    AsyncAtomicPersistenceBinding(machine, store).attach()

    asyncio.run(machine.start_async())

    assert store.inner.load(machine.context.run_id) is not None


def test_atomic_store_validates_the_whole_batch_before_writing():
    store = InMemoryAtomicCommitStore()
    store.commit(
        AtomicCommit(
            effects=(StagedEffect("run", "work", {"value": 1}, id="same"),)
        )
    )
    record = EventRecord("run", "attempt")

    with pytest.raises(PersistenceError, match="effect id conflict"):
        store.commit(
            AtomicCommit(
                records=(record,),
                effects=(
                    StagedEffect("run", "work", {"value": 2}, id="same"),
                ),
            )
        )

    assert store.read_events("run") == []


def test_failed_transition_discards_uncommitted_effect_intents():
    class Effect(Outcome):
        pass

    interpreter = default_outcome_interpreter().with_handler(
        Effect,
        lambda runtime, outcome: runtime.stage_effect(
            "work", effect_id="discard-me"
        ),
        directive=OutcomeDirective.CONTINUE,
    )

    def fail(context, event):
        raise RuntimeError("target failed")

    builder = GraphBuilder("discard-effects", initial="ready")
    builder.action("ready")
    builder.action("target", fail)
    builder.transition(
        "ready", "go", "target", action=lambda context, event: Effect()
    )
    machine = Machine(builder.build(), outcome_interpreter=interpreter)
    store = InMemoryAtomicCommitStore()
    AtomicPersistenceBinding(machine, store).attach()
    machine.start()

    with pytest.raises(RuntimeError, match="target failed"):
        machine.dispatch("go")

    assert machine.staged_effects == ()
    assert store.pending_effects() == []


class MinimalSnapshotStore:
    """只实现 SnapshotStore 必需方法的测试存储。"""

    def __init__(self) -> None:
        self.items: dict[str, dict[str, Any]] = {}

    def save(self, snapshot: ContextSnapshot) -> None:
        self.items[snapshot.run_id] = snapshot.to_dict()

    def load(self, run_id: str) -> ContextSnapshot | None:
        value = self.items.get(run_id)
        return None if value is None else ContextSnapshot.from_dict(value)

    def delete(self, run_id: str) -> None:
        self.items.pop(run_id, None)


def test_snapshots_and_event_log_are_engine_level_adapters():
    builder = GraphBuilder("snapshot", initial="ready")
    builder.action("ready")
    builder.terminal("done")
    builder.transition("ready", "finish", "done")
    graph = builder.build()
    machine = Machine(graph)
    machine.start()
    machine.dispatch("finish")
    snapshot = ContextSnapshot.from_context(machine.context)
    store = InMemorySnapshotStore()
    store.save(snapshot)
    assert store.load(machine.context.run_id) == snapshot
    assert snapshot.version == CURRENT_SNAPSHOT_VERSION
    assert ContextSnapshot.from_dict(snapshot.to_dict()) == snapshot
    assert snapshot.context["last_event"]["name"] == "finish"

    log = InMemoryEventLog()
    log.append(EventRecord(machine.context.run_id, "finished", {"ok": True}))
    assert log.read(machine.context.run_id)[0].name == "finished"

    mutable_payload = {"items": [1]}
    log.append(EventRecord(machine.context.run_id, "copied", mutable_payload))
    mutable_payload["items"].append(2)
    assert log.read(machine.context.run_id)[1].payload == {"items": [1]}

    restored = Machine.from_snapshot(graph, machine.snapshot())
    assert restored.context.last_event is not None
    assert restored.context.last_event.name == "finish"
    assert restored.status is Status.COMPLETED


def test_persistence_binding_accepts_a_minimal_snapshot_store_without_cas():
    builder = GraphBuilder("minimal-store", initial="ready")
    builder.action("ready")
    builder.terminal("done")
    builder.transition("ready", "finish", "done")
    graph = builder.build()
    store = MinimalSnapshotStore()
    machine = Machine(graph)
    PersistenceBinding(machine, store).attach()

    machine.start()
    machine.dispatch("finish")

    saved = store.load(machine.context.run_id)
    assert saved is not None
    assert saved.revision == 0
    restored = PersistenceBinding.restore(graph, machine.context.run_id, store)
    assert restored.status is Status.COMPLETED
    with pytest.raises(PersistenceError, match="read_history"):
        PersistenceBinding(restored, store).history()


def test_context_updates_are_persisted_as_control_plane_facts():
    builder = GraphBuilder("persistent-context-update", initial="ready")
    builder.action("ready")
    graph = builder.build()
    store = InMemorySnapshotStore()
    log = InMemoryEventLog()
    machine = Machine(graph)
    PersistenceBinding(machine, store, log).attach()

    machine.start()
    machine.update_context({"source": "human"}, priority="high")

    saved = store.load(machine.context.run_id)
    assert saved is not None
    assert saved.context["data"] == {
        "source": "human",
        "priority": "high",
    }
    record = log.read(machine.context.run_id)[-1]
    assert record.kind == "context_update"
    assert record.name == "__context_update__"
    assert record.payload == {
        "source": "human",
        "priority": "high",
    }


def test_replay_skips_context_update_records_until_control_plane_replay_is_defined():
    builder = GraphBuilder("context-update-replay", initial="ready")
    builder.action("ready")
    graph = builder.build()
    store = InMemorySnapshotStore()
    log = InMemoryEventLog()
    machine = Machine(graph)
    PersistenceBinding(machine, store, log).attach()

    machine.start()
    machine.update_context(approved=True)

    replayed = replay_events(graph, log.read(machine.context.run_id))

    assert replayed.status is Status.RUNNING
    assert replayed.context.data == {}


def test_snapshot_history_is_ordered_and_detached_from_the_store():
    builder = GraphBuilder("snapshot-history", initial="ready")
    builder.action("ready", lambda context, event: context.set("step", "ready"))
    builder.terminal("done")
    builder.transition("ready", "finish", "done")
    graph = builder.build()
    store = InMemorySnapshotStore()
    machine = Machine(graph)
    binding = PersistenceBinding(machine, store).attach()

    machine.start()
    machine.dispatch("finish")

    history = binding.history()
    assert [item.revision for item in history] == [1, 2]
    assert [item.context["status"] for item in history] == [
        Status.RUNNING.value,
        Status.COMPLETED.value,
    ]
    historical = Machine.from_snapshot(graph, history[0])
    assert historical.status is Status.RUNNING
    assert historical.node_id == "ready"
    with pytest.raises(TypeError):
        history[0].context["data"]["step"] = "changed-outside-store"
    assert binding.history()[0].context["data"]["step"] == "ready"

    store.delete(machine.context.run_id)
    assert binding.history() == []


def test_event_log_contract_preserves_order_and_payload_isolation():
    log = InMemoryEventLog()
    payload = {"items": [1]}
    log.append(EventRecord("run-1", "first", payload))
    log.append(EventRecord("run-1", "second", {"ok": True}))
    payload["items"].append(2)

    records = log.read("run-1")
    assert [record.name for record in records] == ["first", "second"]
    assert records[0].payload == {"items": [1]}
    records[0].payload["items"].append(99)
    assert log.read("run-1")[0].payload == {"items": [1]}


def test_context_snapshot_detaches_nested_data_from_the_live_context():
    context = Context(
        graph_id="copy",
        data={"nested": {"items": [1]}},
        metadata={"trace": {"tags": ["initial"]}},
    )

    snapshot = ContextSnapshot.from_context(context)
    context.data["nested"]["items"].append(2)
    context.metadata["trace"]["tags"].append("later")

    assert snapshot.context["data"] == {"nested": {"items": [1]}}
    assert snapshot.context["metadata"] == {"trace": {"tags": ["initial"]}}

    restored = Context.from_snapshot(snapshot.context)
    restored.data["nested"]["items"].append(3)
    assert snapshot.context["data"] == {"nested": {"items": [1]}}


def test_context_snapshot_isolated_from_constructor_and_export_mutation():
    raw = {"graph_id": "copy", "run_id": "run", "data": {"items": [1]}}
    snapshot = ContextSnapshot("copy", "run", raw)
    raw["data"]["items"].append(2)
    exported = snapshot.to_dict()
    exported["context"]["data"]["items"].append(3)

    assert snapshot.context["data"] == {"items": [1]}


def test_persistence_binds_to_machine_and_can_restore_waiting_runs():
    builder = GraphBuilder("persistent", initial="ready")
    builder.action("ready")
    builder.wait("waiting", resume_event="wake")
    builder.terminal("done")
    builder.transition("ready", "pause", "waiting")
    builder.transition("waiting", "wake", "done")
    graph = builder.build()
    store = InMemorySnapshotStore()
    log = InMemoryEventLog()
    machine = Machine(graph)
    persistence = PersistenceBinding(machine, store, log).attach()

    machine.start()
    machine.dispatch("pause")
    saved = store.load(machine.context.run_id)
    assert saved is not None
    assert saved.runtime is not None
    assert saved.revision > 0
    assert [record.name for record in log.read(machine.context.run_id)] == [
        "__start__",
        "pause",
    ]

    restored = PersistenceBinding.restore(
        graph,
        machine.context.run_id,
        store,
        event_log=log,
    )
    assert restored.status is Status.WAITING
    restored.resume("wake")
    assert restored.status is Status.COMPLETED
    assert store.load(machine.context.run_id).context["status"] == "completed"
    persistence.close()


def test_restore_rejects_a_snapshot_for_another_graph():
    first = GraphBuilder("first", initial="ready")
    first.action("ready")
    first_graph = first.build()
    second = GraphBuilder("second", initial="ready")
    second.action("ready")
    second_graph = second.build()

    snapshot = Machine(first_graph).snapshot()
    with pytest.raises(ValueError, match="graph_id"):
        Machine.from_snapshot(second_graph, snapshot)


def test_event_log_keeps_trace_fields_and_replay_is_explicit():
    builder = GraphBuilder("replay", initial="ready")
    builder.action("ready")
    builder.terminal("done")
    builder.transition("ready", "finish", "done")
    graph = builder.build()
    log = InMemoryEventLog()
    machine = Machine(graph)
    PersistenceBinding(machine, InMemorySnapshotStore(), log).attach()

    machine.start()
    machine.dispatch("finish")
    records = log.read(machine.context.run_id)

    assert records[0].kind == "start"
    assert records[0].graph_id == "replay"
    assert records[0].sequence is not None
    assert records[1].kind == "event"
    assert records[1].transition_id == "ready:finish:done:0"

    replayed = replay_events(graph, records)
    assert replayed.status is Status.COMPLETED
    assert replayed.context.last_event.name == "finish"


def test_failed_dispatch_is_audit_only_and_is_not_replayed():
    attempts = 0

    def unstable(context, event):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("temporary")

    builder = GraphBuilder("failed-replay", initial="ready")
    builder.action("ready")
    builder.terminal("done")
    builder.transition("ready", "finish", "done", action=unstable)
    graph = builder.build()
    log = InMemoryEventLog()
    machine = Machine(graph)
    PersistenceBinding(machine, InMemorySnapshotStore(), log).attach()
    machine.start()

    with pytest.raises(RuntimeError, match="temporary"):
        machine.dispatch("finish")

    records = log.read(machine.context.run_id)
    assert [record.kind for record in records] == ["start", "failed"]
    replayed = replay_events(graph, records)
    assert replayed.status is Status.RUNNING
    assert replayed.node_id == "ready"


def test_retry_resume_is_logged_and_explicit_replay_reaches_the_same_result():
    def work(context, event):
        if context.attempt < 2:
            return Outcome.retry(reason="temporary")
        return Outcome.next("finish")

    builder = GraphBuilder("retry-replay", initial="work")
    builder.action("work", work)
    builder.terminal("done")
    builder.transition("work", "finish", "done")
    graph = builder.build()
    log = InMemoryEventLog()
    machine = Machine(graph)
    PersistenceBinding(machine, InMemorySnapshotStore(), log).attach()

    machine.start()
    machine.resume_retry()
    machine.resume_retry()

    records = log.read(machine.context.run_id)
    assert [record.kind for record in records] == [
        "start",
        "retry",
        "internal",
        "retry",
    ]
    assert machine.status is Status.COMPLETED

    replayed = replay_events(graph, records)
    assert replayed.status is Status.COMPLETED
    assert replayed.context.attempt == 0


def test_persistence_restores_retry_wait_and_reexecutes_the_current_node():
    def work(context, event):
        if context.attempt == 0:
            return Outcome.retry(reason="temporary")
        return Outcome.next("finish")

    builder = GraphBuilder("persistent-retry", initial="work")
    builder.action("work", work)
    builder.terminal("done")
    builder.transition("work", "finish", "done")
    graph = builder.build()
    store = InMemorySnapshotStore()
    log = InMemoryEventLog()
    machine = Machine(graph)
    PersistenceBinding(machine, store, log).attach()

    machine.start()
    restored = PersistenceBinding.restore(
        graph,
        machine.context.run_id,
        store,
        event_log=log,
    )
    assert restored.status is Status.WAITING
    assert restored.context.waiting["kind"] == "retry"

    restored.resume_retry()

    assert restored.status is Status.COMPLETED
    assert restored.context.attempt == 0
    assert [record.kind for record in log.read(restored.context.run_id)] == [
        "start",
        "internal",
        "retry",
    ]


def test_persistence_restores_an_automatic_exception_retry_wait():
    attempts = []

    def work(context, event):
        attempts.append(context.attempt)
        if len(attempts) == 1:
            raise ConnectionError("temporary")

    builder = GraphBuilder("persistent-exception-retry", initial="work")
    builder.terminal("work", work)
    graph = builder.build()
    store = InMemorySnapshotStore()
    machine = Machine(
        graph,
        retry_policy=RetryPolicy(
            max_attempts=2,
            retry_on=(ConnectionError,),
        ),
    )
    PersistenceBinding(machine, store).attach()

    machine.start()
    restored = PersistenceBinding.restore(
        graph,
        machine.context.run_id,
        store,
        retry_policy=RetryPolicy(
            max_attempts=2,
            retry_on=(ConnectionError,),
        ),
    )

    assert restored.status is Status.WAITING
    assert restored.context.waiting["reason"] == {
        "type": "ConnectionError",
        "message": "temporary",
    }
    restored.resume_retry()
    assert restored.status is Status.COMPLETED


def test_persistence_restores_fork_children_before_join():
    builder = GraphBuilder("persistent-fork", initial="start")
    builder.action("start")
    builder.action(
        "forking",
        lambda context, event: Outcome.fork(
            {"event": "finish"},
            {"event": "wait"},
            join_event="joined",
        ),
    )
    builder.wait("waiting", resume_event="wake")
    builder.terminal("child_done")
    builder.terminal("done")
    builder.transition("start", "fork", "forking")
    builder.transition("start", "finish", "child_done")
    builder.transition("start", "wait", "waiting")
    builder.transition("waiting", "wake", "child_done")
    builder.transition("forking", "joined", "done")
    graph = builder.build()
    store = InMemorySnapshotStore()
    log = InMemoryEventLog()
    machine = Machine(graph)
    PersistenceBinding(machine, store, log).attach()

    machine.start()
    machine.dispatch("fork")
    child_run_ids = [child.context.run_id for child in machine.fork_group.children]
    restored = PersistenceBinding.restore(
        graph,
        machine.context.run_id,
        store,
        event_log=log,
    )

    assert restored.fork_group is not None
    restored.route(child_run_ids[1], "wake")
    assert all(
        child.status is Status.COMPLETED
        for child in restored.fork_group.children
    )
    restored.join()

    assert restored.status is Status.COMPLETED
    assert restored.fork_group is None


def test_persistence_saves_a_fork_join_without_a_return_event():
    builder = GraphBuilder("persistent-join", initial="start")
    builder.action("start")
    builder.action(
        "forking",
        lambda context, event: Outcome.fork({"event": "finish"}),
    )
    builder.terminal("child_done")
    builder.action("after_join")
    builder.terminal("done")
    builder.transition("start", "fork", "forking")
    builder.transition("start", "finish", "child_done")
    builder.transition("forking", "finish", "after_join")
    builder.transition("after_join", "finish", "done")
    graph = builder.build()
    store = InMemorySnapshotStore()
    log = InMemoryEventLog()
    machine = Machine(graph)
    PersistenceBinding(machine, store, log).attach()

    machine.start()
    machine.dispatch("fork")
    machine.join()

    saved = store.load(machine.context.run_id)
    assert saved is not None
    assert saved.context["status"] == Status.RUNNING.value
    assert saved.context["waiting"] is None

    join_records = [
        record for record in log.read(machine.context.run_id) if record.kind == "join"
    ]
    assert len(join_records) == 1
    replayed = replay_events(graph, log.read(machine.context.run_id))
    assert replayed.status is Status.RUNNING
    assert replayed.fork_group is None
    assert replayed.context.waiting is None


def test_context_snapshot_preserves_a_versioned_graph():
    builder = GraphBuilder("context-version", initial="ready", version="2")
    builder.action("ready")
    graph = builder.build()
    machine = Machine(graph)

    snapshot = ContextSnapshot.from_context(machine.context)
    restored = Machine.from_snapshot(graph, snapshot)

    assert snapshot.graph_version == "2"
    assert snapshot.context["graph_version"] == "2"
    assert restored.context.graph_version == "2"


def test_snapshot_store_rejects_stale_revisions_atomically():
    builder = GraphBuilder("revision", initial="ready")
    builder.action("ready")
    graph = builder.build()
    machine = Machine(graph)
    snapshot = ContextSnapshot.from_machine(machine)
    store = InMemorySnapshotStore()

    saved = store.save_if_current(snapshot)
    assert saved.revision == 1
    with pytest.raises(SnapshotConflictError, match="revision conflict"):
        store.save_if_current(snapshot)

    restored = Machine.from_snapshot(graph, saved)
    assert restored.context.metadata["snapshot_revision"] == 1


def test_restored_machine_reuses_idempotency_store_for_duplicate_events():
    builder = GraphBuilder("persistent-idempotency", initial="ready")
    builder.action("ready")
    builder.transition("ready", "tick", "ready")
    graph = builder.build()
    store = InMemorySnapshotStore()
    idempotency = InMemoryIdempotencyStore()
    machine = Machine(graph, idempotency=idempotency)
    PersistenceBinding(machine, store).attach()

    machine.start()
    event = Event("tick", event_id="event-1")
    machine.dispatch(event)

    restored = PersistenceBinding.restore(
        graph,
        machine.context.run_id,
        store,
        idempotency=idempotency,
    )
    with pytest.raises(DuplicateEvent):
        restored.dispatch(event)


def test_machine_snapshot_rejects_a_different_graph_definition_version():
    first_builder = GraphBuilder("versioned-snapshot", initial="ready", version="1")
    first_builder.action("ready")
    first = first_builder.build()
    second_builder = GraphBuilder("versioned-snapshot", initial="ready", version="2")
    second_builder.action("ready")
    second = second_builder.build()

    with pytest.raises(ValueError, match="graph_version"):
        Machine.from_snapshot(second, Machine(first).snapshot())


def test_nested_fork_snapshot_restores_routing_and_join_boundaries():
    child_builder = GraphBuilder(
        "persistent-nested-child", initial="start", version="child-1"
    )
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

    parent_builder = GraphBuilder(
        "persistent-nested-parent", initial="start", version="parent-1"
    )
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
    snapshot = machine.snapshot()
    child_run_id = machine.fork_group.children[0].context.run_id
    grandchild_run_id = machine.fork_group.children[0].fork_group.children[0].context.run_id

    restored = Machine.from_snapshot(
        parent_graph,
        snapshot,
        graph_resolver=lambda graph_id: {child_graph.id: child_graph}[graph_id],
    )
    assert restored.fork_group is not None
    restored_child = restored.fork_group.children[0]
    assert restored_child.context.run_id == child_run_id
    assert restored_child.context.metadata["parent_run_id"] == restored.context.run_id
    assert restored_child.fork_group is not None
    restored_grandchild = restored_child.fork_group.children[0]
    assert restored_grandchild.context.run_id == grandchild_run_id
    assert restored_grandchild.context.metadata["parent_run_id"] == child_run_id
    assert restored_grandchild.status is Status.WAITING

    restored.route(grandchild_run_id, "wake")
    restored.join(child_run_id)
    restored.join()

    assert restored.status is Status.COMPLETED
    assert restored_child.status is Status.COMPLETED
    assert restored_grandchild.status is Status.COMPLETED


def test_nested_fork_snapshot_checks_the_nested_graph_version():
    child_builder = GraphBuilder(
        "versioned-nested-child", initial="start", version="child-1"
    )
    child_builder.action("start")
    child_builder.terminal("done")
    child_builder.transition("start", "enter", "done")
    child_graph = child_builder.build()

    parent_builder = GraphBuilder("versioned-nested-parent", initial="start")
    parent_builder.action("start")
    parent_builder.subgraph(
        "child_flow", child_graph, entry_event="enter", return_event="returned"
    )
    parent_builder.terminal("done")
    parent_builder.transition("start", "go", "child_flow")
    parent_builder.transition("child_flow", "returned", "done")
    parent_graph = parent_builder.build()
    machine = Machine(parent_graph)
    machine.start()
    machine.dispatch("go")
    snapshot = machine.snapshot()

    incompatible_builder = GraphBuilder(
        "versioned-nested-child", initial="start", version="child-2"
    )
    incompatible_builder.action("start")
    incompatible_builder.terminal("done")
    incompatible_builder.transition("start", "finish", "done")
    incompatible_graph = incompatible_builder.build()

    with pytest.raises(ValueError, match="graph_version"):
        Machine.from_snapshot(
            parent_graph,
            snapshot,
            graph_resolver=lambda graph_id: {
                incompatible_graph.id: incompatible_graph
            }[graph_id],
        )


def test_parent_persistence_binding_saves_nested_child_changes():
    child_builder = GraphBuilder("bound-child", initial="start")
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

    parent_builder = GraphBuilder("bound-parent", initial="start")
    parent_builder.action("start")
    parent_builder.subgraph(
        "child_flow", child_graph, entry_event="fork", return_event="returned"
    )
    parent_builder.terminal("done")
    parent_builder.transition("start", "enter", "child_flow")
    parent_builder.transition("child_flow", "returned", "done")
    parent_graph = parent_builder.build()

    store = InMemorySnapshotStore()
    log = InMemoryEventLog()
    machine = Machine(parent_graph)
    PersistenceBinding(machine, store, log).attach()
    machine.start()
    machine.dispatch("enter")

    child = machine.fork_group.children[0]
    grandchild = child.fork_group.children[0]
    machine.route(grandchild.context.run_id, "wake")

    saved = store.load(machine.context.run_id)
    assert saved is not None
    saved_grandchild = saved.runtime["fork_group"]["children"][0]["fork_group"][
        "children"
    ][0]
    assert saved_grandchild["context"]["status"] == Status.COMPLETED.value
    assert any(
        record.run_id == grandchild.context.run_id and record.name == "wake"
        for record in log.read(grandchild.context.run_id)
    )

    machine.join(child.context.run_id)
    machine.join()
    assert machine.status is Status.COMPLETED


def test_parent_persistence_binding_saves_child_context_updates_and_restores_them():
    builder = GraphBuilder("bound-context-child", initial="start")
    builder.action("start")
    builder.action(
        "forking",
        lambda context, event: Outcome.fork(
            {"event": "wait"}, join_event="joined"
        ),
    )
    builder.wait("waiting", resume_event="wake")
    builder.terminal("done")
    builder.transition("start", "fork", "forking")
    builder.transition("start", "wait", "waiting")
    builder.transition("waiting", "wake", "done")
    builder.transition("forking", "joined", "done")
    graph = builder.build()

    store = InMemorySnapshotStore()
    log = InMemoryEventLog()
    machine = Machine(graph)
    PersistenceBinding(machine, store, log).attach()
    machine.start()
    machine.dispatch("fork")

    child = machine.fork_group.children[0]
    child.update_context(approved=True, reviewer="alice")

    saved = store.load(machine.context.run_id)
    assert saved is not None
    saved_child = saved.runtime["fork_group"]["children"][0]
    assert saved_child["context"]["data"] == {
        "approved": True,
        "reviewer": "alice",
    }
    child_record = log.read(child.context.run_id)[-1]
    assert child_record.kind == "context_update"
    assert child_record.payload == {"approved": True, "reviewer": "alice"}

    restored = PersistenceBinding.restore(
        graph,
        machine.context.run_id,
        store,
        event_log=log,
    )
    restored_child = restored.fork_group.children[0]
    assert restored_child.context.data == {
        "approved": True,
        "reviewer": "alice",
    }

    restored.route(restored_child.context.run_id, "wake")
    restored.join()
    assert restored.status is Status.COMPLETED


def test_child_event_log_can_be_replayed_independently_by_child_run_id():
    builder = GraphBuilder("child-replay", initial="start")
    builder.action("start")
    builder.action(
        "forking",
        lambda context, event: Outcome.fork(
            {"event": "wait"}, join_event="joined"
        ),
    )
    builder.wait("waiting", resume_event="wake")
    builder.terminal("done")
    builder.transition("start", "fork", "forking")
    builder.transition("start", "wait", "waiting")
    builder.transition("waiting", "wake", "done")
    builder.transition("forking", "joined", "done")
    graph = builder.build()

    log = InMemoryEventLog()
    machine = Machine(graph)
    PersistenceBinding(machine, InMemorySnapshotStore(), log).attach()
    machine.start()
    machine.dispatch("fork")
    child = machine.fork_group.children[0]
    machine.route(child.context.run_id, "wake")

    replayed = replay_events(graph, log.read(child.context.run_id))

    assert replayed.status is Status.COMPLETED
    assert replayed.context.last_event is not None
    assert replayed.context.last_event.name == "wake"
