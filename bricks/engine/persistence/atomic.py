"""Atomic snapshot, event fact, and outbox-effect commit boundary."""

from __future__ import annotations

import asyncio
import threading
import uuid
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, Any, Optional, Protocol

from ..errors import PersistenceError, SnapshotConflictError
from ..events.bus import Subscription
from ..events.hooks import HookContext
from ..runtime.context import ROOT_RUN_ID_METADATA
from ..runtime.effects import StagedEffect
from .binding import _PERSISTENCE_PRIORITY, _event_record
from .event_log import EventRecord
from .snapshot import ContextSnapshot

if TYPE_CHECKING:
    from ..graph.graph import Graph
    from ..runtime.machine import Machine


@dataclass(frozen=True, slots=True)
class AtomicCommit:
    """Everything that must become visible as one execution commit."""

    snapshot: Optional[ContextSnapshot] = None
    records: tuple[EventRecord, ...] = ()
    effects: tuple[StagedEffect, ...] = ()
    commit_id: str = field(default_factory=lambda: uuid.uuid4().hex)

    def __post_init__(self) -> None:
        if not isinstance(self.commit_id, str) or not self.commit_id:
            raise ValueError("atomic commit_id cannot be empty")


class AtomicCommitStore(Protocol):
    def commit(self, batch: AtomicCommit) -> Optional[ContextSnapshot]: ...

    def load(self, run_id: str) -> Optional[ContextSnapshot]: ...


class AsyncAtomicCommitStore(Protocol):
    async def commit(self, batch: AtomicCommit) -> Optional[ContextSnapshot]: ...

    async def load(self, run_id: str) -> Optional[ContextSnapshot]: ...


class InMemoryAtomicCommitStore:
    """Reference implementation that proves the all-or-nothing store contract."""

    def __init__(self) -> None:
        self._snapshots: dict[str, ContextSnapshot] = {}
        self._records: dict[str, EventRecord] = {}
        self._effects: dict[str, StagedEffect] = {}
        self._sent_effects: set[str] = set()
        self._commits: dict[
            str, tuple[AtomicCommit, Optional[ContextSnapshot]]
        ] = {}
        self._lock = threading.RLock()

    def commit(self, batch: AtomicCommit) -> Optional[ContextSnapshot]:
        with self._lock:
            previous = self._commits.get(batch.commit_id)
            if previous is not None:
                previous_batch, previous_snapshot = previous
                if previous_batch != batch:
                    raise PersistenceError(
                        f"atomic commit id conflict: {batch.commit_id!r}"
                    )
                return (
                    None
                    if previous_snapshot is None
                    else _copy_snapshot(previous_snapshot)
                )
            saved = self._next_snapshot(batch.snapshot)
            for record in batch.records:
                existing_record = self._records.get(record.record_id)
                if existing_record is not None and existing_record != record:
                    raise PersistenceError(
                        f"event record id conflict: {record.record_id!r}"
                    )
            for effect in batch.effects:
                existing_effect = self._effects.get(effect.id)
                if existing_effect is not None and existing_effect != effect:
                    raise PersistenceError(
                        f"outbox effect id conflict: {effect.id!r}"
                    )
            for record in batch.records:
                self._records.setdefault(record.record_id, replace(record))
            for effect in batch.effects:
                self._effects[effect.id] = effect
            if saved is not None:
                self._snapshots[saved.run_id] = saved
            self._commits[batch.commit_id] = (batch, saved)
            return None if saved is None else _copy_snapshot(saved)

    def load(self, run_id: str) -> Optional[ContextSnapshot]:
        with self._lock:
            snapshot = self._snapshots.get(run_id)
            return None if snapshot is None else _copy_snapshot(snapshot)

    def read_events(self, run_id: str) -> list[EventRecord]:
        with self._lock:
            return [
                replace(record)
                for record in self._records.values()
                if record.run_id == run_id
            ]

    def pending_effects(self, *, topic: Optional[str] = None) -> list[StagedEffect]:
        with self._lock:
            return [
                effect
                for effect_id, effect in self._effects.items()
                if effect_id not in self._sent_effects
                and (topic is None or effect.topic == topic)
            ]

    def mark_effect_sent(self, effect_id: str) -> None:
        with self._lock:
            if effect_id not in self._effects:
                raise PersistenceError(f"outbox effect not found: {effect_id!r}")
            self._sent_effects.add(effect_id)

    def _next_snapshot(
        self, snapshot: Optional[ContextSnapshot]
    ) -> Optional[ContextSnapshot]:
        if snapshot is None:
            return None
        current = self._snapshots.get(snapshot.run_id)
        if current is not None and current.revision != snapshot.revision:
            raise SnapshotConflictError(
                f"snapshot revision conflict for run {snapshot.run_id!r}: "
                f"expected {snapshot.revision}, current {current.revision}"
            )
        return _copy_snapshot(replace(snapshot, revision=snapshot.revision + 1))


class _AtomicBindingBase:
    def __init__(self, machine: "Machine") -> None:
        self.machine = machine
        self._subscriptions: list[Subscription] = []

    def close(self) -> None:
        for subscription in self._subscriptions:
            self.machine.hooks.unsubscribe(subscription)
        self._subscriptions.clear()

    def _batch(self, hook: HookContext) -> tuple[AtomicCommit, "Machine"]:
        runtime_machine = hook.machine
        belongs = self._belongs_to_machine(hook)
        record = self._record(hook) if belongs else None
        snapshot = None
        if belongs:
            # A child emits lifecycle hooks before ForkController has installed the
            # completed group on its parent. Persist that child independently so
            # its facts/effects never become visible without recoverable state.
            snapshot = ContextSnapshot.from_machine(
                self.machine if self._is_registered(hook) else runtime_machine
            )
        effects = runtime_machine.staged_effects if belongs else ()
        return AtomicCommit(
            snapshot=snapshot,
            records=() if record is None else (record,),
            effects=effects,
        ), runtime_machine

    def _record(self, hook: HookContext) -> Optional[EventRecord]:
        event = hook.event
        if event is None:
            return None
        kinds = {
            "machine.after_start": "start",
            "transition.after": "event",
            "transition.error": "failed",
            "machine.after_join": "join",
            "context.updated": "context_update",
        }
        kind = kinds.get(hook.name)
        if hook.name == "machine.after_resume" and hook.result is None:
            kind = "retry"
        if kind is None:
            return None
        return _event_record(hook.machine, event, kind, hook.runtime_event)

    def _belongs_to_machine(self, hook: HookContext) -> bool:
        if hook.machine is self.machine:
            return True
        if (
            hook.machine.context.metadata.get(ROOT_RUN_ID_METADATA)
            == self.machine.context.run_id
        ):
            return True
        group = self.machine.fork_group
        return group is not None and group.contains(hook.machine.context.run_id)

    def _is_registered(self, hook: HookContext) -> bool:
        if hook.machine is self.machine:
            return True
        group = self.machine.fork_group
        return group is not None and group.contains(hook.machine.context.run_id)


class AtomicPersistenceBinding(_AtomicBindingBase):
    """Commit snapshots, execution facts, and staged effects atomically."""

    def __init__(self, machine: "Machine", store: AtomicCommitStore) -> None:
        super().__init__(machine)
        self.store = store
        self._pending: Optional[tuple[AtomicCommit, "Machine"]] = None

    def attach(self) -> "AtomicPersistenceBinding":
        if self._subscriptions:
            return self
        for name in _ATOMIC_HOOKS:
            self._subscriptions.append(
                self.machine.hooks.on(name, self._on_change, priority=_PERSISTENCE_PRIORITY)
            )
        return self

    def _on_change(self, hook: HookContext) -> None:
        if self._pending is not None:
            raise PersistenceError("atomic commit is pending; call flush() first")
        batch, runtime_machine = self._batch(hook)
        if batch.snapshot is None and not batch.records and not batch.effects:
            return
        self._commit(batch, runtime_machine)

    def flush(self) -> Optional[ContextSnapshot]:
        """Retry the exact batch retained after an atomic store failure."""
        if self._pending is None:
            return None
        batch, runtime_machine = self._pending
        return self._commit(batch, runtime_machine)

    def _commit(
        self, batch: AtomicCommit, runtime_machine: "Machine"
    ) -> Optional[ContextSnapshot]:
        try:
            saved = self.store.commit(batch)
        except BaseException:
            self._pending = (batch, runtime_machine)
            raise
        if saved is not None:
            self._record_revision(saved, runtime_machine)
        runtime_machine._ack_staged_effects({effect.id for effect in batch.effects})
        self._pending = None
        return saved

    def _record_revision(
        self, saved: ContextSnapshot, runtime_machine: "Machine"
    ) -> None:
        owner = (
            runtime_machine
            if saved.run_id == runtime_machine.context.run_id
            else self.machine
        )
        owner.context.metadata["snapshot_revision"] = saved.revision

    @classmethod
    def restore(
        cls,
        graph: "Graph",
        run_id: str,
        store: AtomicCommitStore,
        **options: Any,
    ) -> "Machine":
        snapshot = store.load(run_id)
        if snapshot is None:
            raise PersistenceError(f"run snapshot not found: {run_id!r}")
        from ..runtime.machine import Machine

        machine = Machine.from_snapshot(graph, snapshot, **options)
        cls(machine, store).attach()
        return machine


class AsyncAtomicPersistenceBinding(_AtomicBindingBase):
    def __init__(self, machine: "Machine", store: AsyncAtomicCommitStore) -> None:
        super().__init__(machine)
        self.store = store
        self._pending: Optional[tuple[AtomicCommit, "Machine"]] = None
        self._commit_lock = asyncio.Lock()

    def attach(self) -> "AsyncAtomicPersistenceBinding":
        if self._subscriptions:
            return self
        for name in _ATOMIC_HOOKS:
            self._subscriptions.append(
                self.machine.hooks.on(name, self._on_change, priority=_PERSISTENCE_PRIORITY)
            )
        return self

    async def _on_change(self, hook: HookContext) -> None:
        async with self._commit_lock:
            if self._pending is not None:
                raise PersistenceError("atomic commit is pending; call flush() first")
            batch, runtime_machine = self._batch(hook)
            if batch.snapshot is None and not batch.records and not batch.effects:
                return
            await self._commit(batch, runtime_machine)

    async def flush(self) -> Optional[ContextSnapshot]:
        async with self._commit_lock:
            if self._pending is None:
                return None
            batch, runtime_machine = self._pending
            return await self._commit(batch, runtime_machine)

    async def _commit(
        self, batch: AtomicCommit, runtime_machine: "Machine"
    ) -> Optional[ContextSnapshot]:
        try:
            saved = await self.store.commit(batch)
        except BaseException:
            self._pending = (batch, runtime_machine)
            raise
        if saved is not None:
            owner = (
                runtime_machine
                if saved.run_id == runtime_machine.context.run_id
                else self.machine
            )
            owner.context.metadata["snapshot_revision"] = saved.revision
        runtime_machine._ack_staged_effects({effect.id for effect in batch.effects})
        self._pending = None
        return saved

    @classmethod
    async def restore(
        cls,
        graph: "Graph",
        run_id: str,
        store: AsyncAtomicCommitStore,
        **options: Any,
    ) -> "Machine":
        snapshot = await store.load(run_id)
        if snapshot is None:
            raise PersistenceError(f"run snapshot not found: {run_id!r}")
        from ..runtime.machine import Machine

        machine = Machine.from_snapshot(graph, snapshot, **options)
        cls(machine, store).attach()
        return machine


_ATOMIC_HOOKS = (
    "machine.after_start",
    "transition.after",
    "transition.error",
    "machine.paused",
    "machine.resumed",
    "machine.after_resume",
    "machine.after_join",
    "context.updated",
)


def _copy_snapshot(snapshot: ContextSnapshot) -> ContextSnapshot:
    return ContextSnapshot.from_dict(snapshot.to_dict())
