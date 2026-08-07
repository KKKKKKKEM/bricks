"""Narrow capabilities available while interpreting an Outcome."""

from __future__ import annotations

from datetime import timedelta
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Mapping, Optional, Protocol

from ..events.messages import Event
from .context import Context
from .effects import StagedEffect
from .lifecycle import Status
from .outcomes import Fork, Retry

if TYPE_CHECKING:
    from .machine import Machine


class ContextView:
    """Read-only facade exposed to domain Outcome handlers."""

    __slots__ = ("_context",)

    def __init__(self, context: Context) -> None:
        self._context = context

    @property
    def graph_id(self) -> str:
        return self._context.graph_id

    @property
    def run_id(self) -> str:
        return self._context.run_id

    @property
    def node_id(self) -> Optional[str]:
        return self._context.node_id

    @property
    def status(self) -> Status:
        return self._context.status

    @property
    def data(self) -> Mapping[str, Any]:
        return MappingProxyType(self._context.data)

    @property
    def metadata(self) -> Mapping[str, Any]:
        return MappingProxyType(self._context.metadata)

    @property
    def attempt(self) -> int:
        return self._context.attempt

    @property
    def waiting(self) -> Optional[Mapping[str, Any]]:
        if self._context.waiting is None:
            return None
        return MappingProxyType(self._context.waiting)

    def get(self, name: str, default: Any = None) -> Any:
        return self._context.get(name, default)


class OutcomeRuntime(Protocol):
    """Capability port for domain handlers; it deliberately omits dispatch APIs."""

    @property
    def context(self) -> ContextView: ...

    @property
    def run_id(self) -> str: ...

    def update(self, values: Mapping[str, Any]) -> None: ...

    def publish(self, name: str, payload: Any = None) -> None: ...

    async def publish_async(self, name: str, payload: Any = None) -> None: ...

    def stage_effect(
        self,
        topic: str,
        payload: Any = None,
        *,
        effect_id: str | None = None,
    ) -> StagedEffect: ...

    def wait(
        self,
        delay: float | None = None,
        resume_event: str | None = None,
    ) -> None: ...

    def retry(self, outcome: Retry) -> None: ...

    def stop(self, reason: Any = None) -> None: ...

    def fail(self, error: Any) -> None: ...

    def start_fork(self, outcome: Fork) -> None: ...

    async def start_fork_async(self, outcome: Fork) -> None: ...


class MachineOutcomeRuntime:
    """Internal adapter exposing only supported Outcome-side capabilities."""

    __slots__ = ("_machine", "_context_view")

    def __init__(self, machine: "Machine") -> None:
        self._machine = machine
        self._context_view = ContextView(machine.context)

    @property
    def context(self) -> ContextView:
        return self._context_view

    @property
    def run_id(self) -> str:
        return self.context.run_id

    def update(self, values: Mapping[str, Any]) -> None:
        self._machine.context.update(values)
        self._machine.context.attempt = 0

    def publish(self, name: str, payload: Any = None) -> None:
        emitted = Event(name, payload, self.run_id)
        self._machine._defer_emitted_event(emitted)

    async def publish_async(self, name: str, payload: Any = None) -> None:
        emitted = Event(name, payload, self.run_id)
        self._machine._defer_emitted_event(emitted)

    def stage_effect(
        self,
        topic: str,
        payload: Any = None,
        *,
        effect_id: str | None = None,
    ) -> StagedEffect:
        effect = (
            StagedEffect(self.run_id, topic, payload)
            if effect_id is None
            else StagedEffect(self.run_id, topic, payload, id=effect_id)
        )
        self._machine._stage_effect(effect)
        return effect

    def wait(
        self,
        delay: float | None = None,
        resume_event: str | None = None,
    ) -> None:
        waiting = {
            "delay": delay,
            "resume_event": resume_event,
        }
        if delay is not None:
            due_at = self._machine.clock() + timedelta(seconds=delay)
            if due_at.tzinfo is None:
                raise ValueError("Machine clock must return a timezone-aware datetime")
            waiting["due_at"] = due_at.isoformat()
        self._machine.context.waiting = waiting
        self._machine.context.status = Status.WAITING

    def retry(self, outcome: Retry) -> None:
        policy = self._machine.retry_policy
        context = self._machine.context
        if not policy.allows(context.attempt):
            context.status = Status.FAILED
            context.waiting = None
            context.metadata["retry_exhausted"] = {
                "attempt": context.attempt,
                "reason": outcome.reason,
            }
            return
        context.attempt += 1
        delay = (
            outcome.delay
            if outcome.delay is not None
            else policy.delay_for(context.attempt)
        )
        due_at = self._machine.clock() + timedelta(seconds=delay)
        if due_at.tzinfo is None:
            raise ValueError("Machine clock must return a timezone-aware datetime")
        context.waiting = {
            "delay": delay,
            "due_at": due_at.isoformat(),
            "resume_event": outcome.event or "__retry__",
            "kind": "retry",
            "attempt": context.attempt,
            "reason": outcome.reason,
        }
        context.status = Status.WAITING

    def stop(self, reason: Any = None) -> None:
        context = self._machine.context
        context.status = Status.STOPPED
        context.metadata["stop_reason"] = reason
        context.attempt = 0

    def fail(self, error: Any) -> None:
        context = self._machine.context
        context.status = Status.FAILED
        context.metadata["failure"] = error
        context.attempt = 0

    def start_fork(self, outcome: Fork) -> None:
        self._machine._forks.start(outcome)

    async def start_fork_async(self, outcome: Fork) -> None:
        await self._machine._forks.start_async(outcome)

    @property
    def _legacy_machine(self) -> "Machine":
        return self._machine
