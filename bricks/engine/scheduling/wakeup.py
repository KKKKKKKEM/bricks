"""Persistable wake-up requests; timer infrastructure remains external."""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Callable, Optional, Protocol

from ..errors import MachineNotRunnable
from ..events.bus import Subscription
from ..events.hooks import HookContext
from ..events.messages import Event
from ..runtime.context import ROOT_RUN_ID_METADATA
from ..runtime.lifecycle import Status
from ..types import freeze_value, thaw_value

if TYPE_CHECKING:
    from ..runtime.machine import Machine


def _now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True, slots=True)
class Wakeup:
    """A durable request for an external scheduler to resume one run."""

    id: str
    run_id: str
    graph_id: str
    due_at: datetime
    event: str
    kind: str = "event"
    payload: Any = None
    graph_version: str = "1"
    node_id: Optional[str] = None
    attempt: int = 0
    created_at: datetime = field(default_factory=_now)

    def __post_init__(self) -> None:
        if not self.id or not self.run_id or not self.graph_id:
            raise ValueError("wakeup identity fields cannot be empty")
        if not self.event:
            raise ValueError("wakeup event cannot be empty")
        if self.kind not in {"event", "retry"}:
            raise ValueError("wakeup kind must be 'event' or 'retry'")
        if self.attempt < 0:
            raise ValueError("wakeup attempt cannot be negative")
        if self.due_at.tzinfo is None or self.created_at.tzinfo is None:
            raise ValueError("wakeup timestamps must be timezone-aware")
        object.__setattr__(self, "payload", freeze_value(self.payload))

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "run_id": self.run_id,
            "graph_id": self.graph_id,
            "graph_version": self.graph_version,
            "due_at": self.due_at.isoformat(),
            "event": self.event,
            "kind": self.kind,
            "payload": thaw_value(self.payload),
            "node_id": self.node_id,
            "attempt": self.attempt,
            "created_at": self.created_at.isoformat(),
        }


class WakeupScheduler(Protocol):
    def schedule(self, wakeup: Wakeup) -> None: ...

    def cancel(self, run_id: str) -> None: ...


class AsyncWakeupScheduler(Protocol):
    async def schedule(self, wakeup: Wakeup) -> None: ...

    async def cancel(self, run_id: str) -> None: ...


class InMemoryWakeupScheduler:
    """Deterministic reference scheduler; production adapters may use Redis or SQL."""

    def __init__(self) -> None:
        self._items: dict[str, Wakeup] = {}
        self._lock = threading.RLock()

    def schedule(self, wakeup: Wakeup) -> None:
        with self._lock:
            self._items[wakeup.run_id] = wakeup

    def cancel(self, run_id: str) -> None:
        with self._lock:
            self._items.pop(run_id, None)

    def get(self, run_id: str) -> Optional[Wakeup]:
        with self._lock:
            return self._items.get(run_id)

    def due(self, now: Optional[datetime] = None) -> list[Wakeup]:
        current = now or _now()
        with self._lock:
            due = sorted(
                (item for item in self._items.values() if item.due_at <= current),
                key=lambda item: (item.due_at, item.id),
            )
            return due


def wakeup_for(
    machine: "Machine",
    *,
    now: Optional[datetime] = None,
) -> Optional[Wakeup]:
    """Translate current waiting state into an infrastructure-neutral request."""
    waiting = machine.context.waiting or {}
    delay = waiting.get("delay")
    if machine.status is not Status.WAITING or delay is None:
        return None
    kind = "retry" if waiting.get("kind") == "retry" else "event"
    event = waiting.get("resume_event") or "__timer__"
    attempt = int(waiting.get("attempt", machine.context.attempt))
    identity = ":".join(
        (
            machine.context.run_id,
            kind,
            str(machine.node_id or ""),
            str(attempt),
            event,
        )
    )
    created_at = now or _now()
    due_at_value = waiting.get("due_at")
    due_at = (
        datetime.fromisoformat(due_at_value)
        if isinstance(due_at_value, str)
        else created_at + timedelta(seconds=float(delay))
    )
    return Wakeup(
        id=identity,
        run_id=machine.context.run_id,
        graph_id=machine.graph.id,
        graph_version=machine.graph.version,
        due_at=due_at,
        event=event,
        kind=kind,
        node_id=machine.node_id,
        attempt=attempt,
        created_at=created_at,
    )


def dispatch_wakeup(machine: "Machine", wakeup: Wakeup) -> Any:
    _validate_target(machine, wakeup)
    if wakeup.kind == "retry":
        return machine.resume_retry(
            event=Event(
                wakeup.event,
                wakeup.payload,
                source="scheduler",
                event_id=wakeup.id,
                target_run_id=wakeup.run_id,
            )
        )
    return machine.resume(
        Event(
            wakeup.event,
            wakeup.payload,
            source="scheduler",
            event_id=wakeup.id,
            target_run_id=wakeup.run_id,
        )
    )


async def dispatch_wakeup_async(machine: "Machine", wakeup: Wakeup) -> Any:
    _validate_target(machine, wakeup)
    if wakeup.kind == "retry":
        return await machine.resume_retry_async(
            event=Event(
                wakeup.event,
                wakeup.payload,
                source="scheduler",
                event_id=wakeup.id,
                target_run_id=wakeup.run_id,
            )
        )
    return await machine.resume_async(
        Event(
            wakeup.event,
            wakeup.payload,
            source="scheduler",
            event_id=wakeup.id,
            target_run_id=wakeup.run_id,
        )
    )


class WakeupBinding:
    """Keep a synchronous scheduler aligned with Machine waiting state."""

    def __init__(
        self,
        machine: "Machine",
        scheduler: WakeupScheduler,
        *,
        clock: Callable[[], datetime] = _now,
    ) -> None:
        self.machine = machine
        self.scheduler = scheduler
        self.clock = clock
        self._subscriptions: list[Subscription] = []

    def attach(self) -> "WakeupBinding":
        if self._subscriptions:
            return self
        for name in _WAKEUP_HOOKS:
            self._subscriptions.append(
                self.machine.hooks.on(name, self._sync, priority=-900)
            )
        return self

    def sync(self, machine: Optional["Machine"] = None) -> Optional[Wakeup]:
        """Reconcile after restore, closing the commit-to-schedule crash window."""
        current = machine or self.machine
        wakeup = wakeup_for(current, now=self.clock())
        if wakeup is None:
            self.scheduler.cancel(current.context.run_id)
        else:
            self.scheduler.schedule(wakeup)
        return wakeup

    def close(self) -> None:
        for subscription in self._subscriptions:
            self.machine.hooks.unsubscribe(subscription)
        self._subscriptions.clear()

    def _sync(self, hook: HookContext) -> None:
        if not _belongs_to(self.machine, hook.machine):
            return
        self.sync(hook.machine)


class AsyncWakeupBinding:
    """Async storage counterpart of WakeupBinding."""

    def __init__(
        self,
        machine: "Machine",
        scheduler: AsyncWakeupScheduler,
        *,
        clock: Callable[[], datetime] = _now,
    ) -> None:
        self.machine = machine
        self.scheduler = scheduler
        self.clock = clock
        self._subscriptions: list[Subscription] = []

    def attach(self) -> "AsyncWakeupBinding":
        if self._subscriptions:
            return self
        for name in _WAKEUP_HOOKS:
            self._subscriptions.append(
                self.machine.hooks.on(name, self._sync, priority=-900)
            )
        return self

    async def sync(self, machine: Optional["Machine"] = None) -> Optional[Wakeup]:
        current = machine or self.machine
        wakeup = wakeup_for(current, now=self.clock())
        if wakeup is None:
            await self.scheduler.cancel(current.context.run_id)
        else:
            await self.scheduler.schedule(wakeup)
        return wakeup

    def close(self) -> None:
        for subscription in self._subscriptions:
            self.machine.hooks.unsubscribe(subscription)
        self._subscriptions.clear()

    async def _sync(self, hook: HookContext) -> None:
        if not _belongs_to(self.machine, hook.machine):
            return
        await self.sync(hook.machine)


_WAKEUP_HOOKS = (
    "machine.after_start",
    "transition.after",
    "transition.error",
    "machine.after_resume",
    "machine.after_join",
)


def _belongs_to(root: "Machine", candidate: "Machine") -> bool:
    return candidate is root or candidate.context.metadata.get(
        ROOT_RUN_ID_METADATA
    ) == root.context.run_id


def _validate_target(machine: "Machine", wakeup: Wakeup) -> None:
    if wakeup.run_id != machine.context.run_id:
        raise MachineNotRunnable(
            f"wakeup targets run {wakeup.run_id!r}, not {machine.context.run_id!r}"
        )
    if wakeup.graph_id != machine.graph.id:
        raise MachineNotRunnable(
            f"wakeup graph {wakeup.graph_id!r} does not match {machine.graph.id!r}"
        )
    if wakeup.graph_version != machine.graph.version:
        raise MachineNotRunnable(
            f"wakeup graph version {wakeup.graph_version!r} does not match "
            f"{machine.graph.version!r}"
        )
    expected = wakeup_for(machine, now=wakeup.created_at)
    if expected is None or any(
        (
            expected.id != wakeup.id,
            expected.kind != wakeup.kind,
            expected.event != wakeup.event,
            expected.node_id != wakeup.node_id,
            expected.attempt != wakeup.attempt,
            expected.due_at != wakeup.due_at,
        )
    ):
        raise MachineNotRunnable("wakeup is stale for the current waiting state")
