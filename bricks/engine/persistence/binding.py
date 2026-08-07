"""把 Machine 绑定到快照存储和事件日志。

持久化是运行实例的附加能力，不让 Machine 的构造函数感知具体存储介质。
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TYPE_CHECKING, Optional

from ..events.bus import Subscription
from ..events.hooks import HookContext
from ..events.messages import Event
from ..errors import PersistenceError
from ..runtime.context import ROOT_RUN_ID_METADATA
from .event_log import AsyncEventLog, EventLog, EventRecord
from .snapshot import ContextSnapshot
from .store import AsyncSnapshotStore, SnapshotStore

if TYPE_CHECKING:
    from ..graph.graph import Graph
    from ..runtime.machine import Machine


_PERSISTENCE_PRIORITY = -1000


def _event_record(
    machine: "Machine",
    event: Event,
    kind: str,
    runtime_event: Any,
) -> EventRecord:
    if kind == "event" and event.source == machine.context.run_id:
        kind = "internal"
    if (
        kind == "event"
        and isinstance(event.payload, Mapping)
        and "fork_id" in event.payload
    ):
        kind = "join"
    return EventRecord(
        run_id=machine.context.run_id,
        name=event.name,
        payload=event.payload,
        event_id=event.event_id,
        source=event.source,
        target_run_id=event.target_run_id,
        created_at=event.created_at,
        kind=kind,
        graph_id=machine.graph.id,
        sequence=getattr(runtime_event, "sequence", None),
        node_id=getattr(runtime_event, "node_id", machine.node_id),
        status=getattr(runtime_event, "status", machine.status.value),
        transition_id=getattr(runtime_event, "transition_id", None),
        parent_run_id=getattr(runtime_event, "parent_run_id", None),
        graph_version=machine.graph.version,
    )


class PersistenceBinding:
    """将一个 Machine 的运行结果保存到外部存储。"""

    def __init__(
        self,
        machine: "Machine",
        store: SnapshotStore,
        event_log: Optional[EventLog] = None,
    ) -> None:
        self.machine = machine
        self.store = store
        self.event_log = event_log
        self._subscriptions: list[Subscription] = []

    def attach(self) -> "PersistenceBinding":
        if self._subscriptions:
            return self
        hooks = self.machine.hooks
        self._subscriptions.extend(
            [
                hooks.on(
                    "machine.after_start",
                    self._on_start,
                    priority=_PERSISTENCE_PRIORITY,
                ),
                hooks.on(
                    "transition.after",
                    self._on_change,
                    priority=_PERSISTENCE_PRIORITY,
                ),
                hooks.on(
                    "transition.error",
                    self._on_change,
                    priority=_PERSISTENCE_PRIORITY,
                ),
                hooks.on(
                    "machine.paused",
                    self._on_change,
                    priority=_PERSISTENCE_PRIORITY,
                ),
                hooks.on(
                    "machine.resumed",
                    self._on_change,
                    priority=_PERSISTENCE_PRIORITY,
                ),
                hooks.on(
                    "machine.after_resume",
                    self._on_resume,
                    priority=_PERSISTENCE_PRIORITY,
                ),
                hooks.on(
                    "machine.after_join",
                    self._on_change,
                    priority=_PERSISTENCE_PRIORITY,
                ),
                hooks.on(
                    "context.updated",
                    self._on_context_update,
                    priority=_PERSISTENCE_PRIORITY,
                ),
            ]
        )
        return self

    def close(self) -> None:
        for subscription in self._subscriptions:
            self.machine.hooks.unsubscribe(subscription)
        self._subscriptions.clear()

    def save(self) -> ContextSnapshot:
        snapshot = ContextSnapshot.from_machine(self.machine)
        save_if_current = getattr(self.store, "save_if_current", None)
        if save_if_current is None:
            self.store.save(snapshot)
            return snapshot
        saved = save_if_current(snapshot)
        self.machine.context.metadata["snapshot_revision"] = saved.revision
        return saved

    def history(self, run_id: Optional[str] = None) -> list[ContextSnapshot]:
        """读取快照历史；存储未提供该可选能力时明确失败。"""
        reader = getattr(self.store, "read_history", None)
        if not callable(reader):
            raise PersistenceError(
                "snapshot store does not support optional read_history()"
            )
        return list(reader(run_id or self.machine.context.run_id))

    @classmethod
    def restore(
        cls,
        graph: "Graph",
        run_id: str,
        store: SnapshotStore,
        *,
        event_log: Optional[EventLog] = None,
        **options: Any,
    ) -> "Machine":
        snapshot = store.load(run_id)
        if snapshot is None:
            raise PersistenceError(f"run snapshot not found: {run_id!r}")
        from ..runtime.machine import Machine

        machine = Machine.from_snapshot(graph, snapshot, **options)
        cls(machine, store, event_log).attach()
        return machine

    def _on_start(self, hook: HookContext) -> None:
        if self._belongs_to_machine(hook):
            self._record(
                hook.event,
                machine=hook.machine,
                kind="start",
                runtime_event=hook.runtime_event,
            )
            if self._is_registered(hook):
                self.save()

    def _on_change(self, hook: HookContext) -> None:
        if hook.name == "transition.after" and self._belongs_to_machine(hook):
            self._record(
                hook.event,
                machine=hook.machine,
                runtime_event=hook.runtime_event,
            )
        elif hook.name == "transition.error" and self._belongs_to_machine(hook):
            self._record(
                hook.event,
                machine=hook.machine,
                kind="failed",
                runtime_event=hook.runtime_event,
            )
        if hook.name == "machine.after_join" and hook.event is not None:
            self._record(
                hook.event,
                machine=hook.machine,
                kind="join",
                runtime_event=hook.runtime_event,
            )
        if self._is_registered(hook):
            self.save()

    def _on_resume(self, hook: HookContext) -> None:
        if not self._belongs_to_machine(hook):
            return
        if hook.event is not None and hook.result is None:
            self._record(
                hook.event,
                machine=hook.machine,
                kind="retry",
                runtime_event=hook.runtime_event,
            )
        if self._is_registered(hook):
            self.save()

    def _on_context_update(self, hook: HookContext) -> None:
        if self._belongs_to_machine(hook):
            self._record(
                hook.event,
                machine=hook.machine,
                kind="context_update",
                runtime_event=hook.runtime_event,
            )
            if self._is_registered(hook):
                self.save()

    def _record(
        self,
        event: Optional[Event],
        *,
        machine: Optional["Machine"] = None,
        kind: str = "event",
        runtime_event: Any = None,
    ) -> None:
        if self.event_log is None or event is None:
            return
        runtime_machine = machine or self.machine
        self.event_log.append(
            _event_record(runtime_machine, event, kind, runtime_event)
        )

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
        """判断后代是否已经进入父级 ForkGroup，避免保存未完成的中间快照。"""
        if hook.machine is self.machine:
            return True
        group = self.machine.fork_group
        return group is not None and group.contains(hook.machine.context.run_id)


class AsyncPersistenceBinding:
    """使用异步存储协议的持久化绑定；Machine 必须通过异步入口推进。"""

    def __init__(
        self,
        machine: "Machine",
        store: AsyncSnapshotStore,
        event_log: Optional[AsyncEventLog] = None,
    ) -> None:
        self.machine = machine
        self.store = store
        self.event_log = event_log
        self._subscriptions: list[Subscription] = []

    def attach(self) -> "AsyncPersistenceBinding":
        if self._subscriptions:
            return self
        hooks = self.machine.hooks
        self._subscriptions.extend(
            [
                hooks.on(
                    "machine.after_start",
                    self._on_start,
                    priority=_PERSISTENCE_PRIORITY,
                ),
                hooks.on(
                    "transition.after",
                    self._on_change,
                    priority=_PERSISTENCE_PRIORITY,
                ),
                hooks.on(
                    "transition.error",
                    self._on_change,
                    priority=_PERSISTENCE_PRIORITY,
                ),
                hooks.on(
                    "machine.paused",
                    self._on_change,
                    priority=_PERSISTENCE_PRIORITY,
                ),
                hooks.on(
                    "machine.resumed",
                    self._on_change,
                    priority=_PERSISTENCE_PRIORITY,
                ),
                hooks.on(
                    "machine.after_resume",
                    self._on_resume,
                    priority=_PERSISTENCE_PRIORITY,
                ),
                hooks.on(
                    "machine.after_join",
                    self._on_change,
                    priority=_PERSISTENCE_PRIORITY,
                ),
                hooks.on(
                    "context.updated",
                    self._on_context_update,
                    priority=_PERSISTENCE_PRIORITY,
                ),
            ]
        )
        return self

    def close(self) -> None:
        for subscription in self._subscriptions:
            self.machine.hooks.unsubscribe(subscription)
        self._subscriptions.clear()

    async def save(self) -> ContextSnapshot:
        snapshot = ContextSnapshot.from_machine(self.machine)
        save_if_current = getattr(self.store, "save_if_current", None)
        if save_if_current is None:
            await self.store.save(snapshot)
            return snapshot
        saved = await save_if_current(snapshot)
        self.machine.context.metadata["snapshot_revision"] = saved.revision
        return saved

    async def history(self, run_id: Optional[str] = None) -> list[ContextSnapshot]:
        reader = getattr(self.store, "read_history", None)
        if not callable(reader):
            raise PersistenceError(
                "snapshot store does not support optional read_history()"
            )
        return list(await reader(run_id or self.machine.context.run_id))

    @classmethod
    async def restore(
        cls,
        graph: "Graph",
        run_id: str,
        store: AsyncSnapshotStore,
        *,
        event_log: Optional[AsyncEventLog] = None,
        **options: Any,
    ) -> "Machine":
        snapshot = await store.load(run_id)
        if snapshot is None:
            raise PersistenceError(f"run snapshot not found: {run_id!r}")
        from ..runtime.machine import Machine

        machine = Machine.from_snapshot(graph, snapshot, **options)
        cls(machine, store, event_log).attach()
        return machine

    async def _on_start(self, hook: HookContext) -> None:
        if self._belongs_to_machine(hook):
            await self._record(
                hook.event,
                machine=hook.machine,
                kind="start",
                runtime_event=hook.runtime_event,
            )
            if self._is_registered(hook):
                await self.save()

    async def _on_change(self, hook: HookContext) -> None:
        if hook.name == "transition.after" and self._belongs_to_machine(hook):
            await self._record(
                hook.event,
                machine=hook.machine,
                runtime_event=hook.runtime_event,
            )
        elif hook.name == "transition.error" and self._belongs_to_machine(hook):
            await self._record(
                hook.event,
                machine=hook.machine,
                kind="failed",
                runtime_event=hook.runtime_event,
            )
        if hook.name == "machine.after_join" and hook.event is not None:
            await self._record(
                hook.event,
                machine=hook.machine,
                kind="join",
                runtime_event=hook.runtime_event,
            )
        if self._is_registered(hook):
            await self.save()

    async def _on_resume(self, hook: HookContext) -> None:
        if not self._belongs_to_machine(hook):
            return
        if hook.event is not None and hook.result is None:
            await self._record(
                hook.event,
                machine=hook.machine,
                kind="retry",
                runtime_event=hook.runtime_event,
            )
        if self._is_registered(hook):
            await self.save()

    async def _on_context_update(self, hook: HookContext) -> None:
        if self._belongs_to_machine(hook):
            await self._record(
                hook.event,
                machine=hook.machine,
                kind="context_update",
                runtime_event=hook.runtime_event,
            )
            if self._is_registered(hook):
                await self.save()

    async def _record(
        self,
        event: Optional[Event],
        *,
        machine: Optional["Machine"] = None,
        kind: str = "event",
        runtime_event: Any = None,
    ) -> None:
        if self.event_log is None or event is None:
            return
        runtime_machine = machine or self.machine
        await self.event_log.append(
            _event_record(runtime_machine, event, kind, runtime_event)
        )

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
