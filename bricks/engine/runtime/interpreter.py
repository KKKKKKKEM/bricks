"""Composable interpretation of outcomes produced by actions.

Machine owns transition ordering. OutcomeRegistry owns the meaning of each
Outcome type, keeping domain-specific effects out of the execution loop.
"""

from __future__ import annotations

import inspect
from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Callable, Optional, Protocol

from ..errors import AsyncActionRequired
from .outcomes import Emit, Fail, Fork, Next, Outcome, Retry, Stop, Update, Wait
from .outcome_runtime import OutcomeRuntime

if TYPE_CHECKING:
    from .machine import Machine


class OutcomeDirective(str, Enum):
    """Whether the current transition may continue to its next phase."""

    CONTINUE = "continue"
    STOP = "stop"


OutcomeHandler = Callable[[OutcomeRuntime, Outcome], Any]
LegacyOutcomeHandler = Callable[["Machine", Outcome], Any]


class OutcomeInterpreter(Protocol):
    """Minimal extension boundary used by Machine."""

    def apply(
        self, runtime: OutcomeRuntime, outcome: Outcome
    ) -> OutcomeDirective: ...

    async def apply_async(
        self, runtime: OutcomeRuntime, outcome: Outcome
    ) -> OutcomeDirective: ...


@dataclass(frozen=True, slots=True)
class OutcomeRule:
    handler: OutcomeHandler
    directive: OutcomeDirective = OutcomeDirective.STOP
    async_handler: Optional[OutcomeHandler] = None
    legacy_machine: bool = False

    def __post_init__(self) -> None:
        if not callable(self.handler):
            raise TypeError("outcome handler must be callable")
        if self.async_handler is not None and not callable(self.async_handler):
            raise TypeError("async outcome handler must be callable")
        object.__setattr__(self, "directive", OutcomeDirective(self.directive))


class OutcomeRegistry:
    """Immutable type-to-handler registry with sync and async interpretation."""

    def __init__(
        self,
        rules: Optional[Mapping[type[Outcome], OutcomeRule]] = None,
    ) -> None:
        normalized = dict(rules or {})
        for outcome_type, rule in normalized.items():
            if not isinstance(outcome_type, type) or not issubclass(
                outcome_type, Outcome
            ):
                raise TypeError("outcome rule keys must be Outcome subclasses")
            if not isinstance(rule, OutcomeRule):
                raise TypeError("outcome rules must be OutcomeRule instances")
        self._rules = MappingProxyType(normalized)

    @property
    def rules(self) -> Mapping[type[Outcome], OutcomeRule]:
        return self._rules

    def with_handler(
        self,
        outcome_type: type[Outcome],
        handler: OutcomeHandler,
        *,
        directive: OutcomeDirective = OutcomeDirective.STOP,
        async_handler: Optional[OutcomeHandler] = None,
    ) -> "OutcomeRegistry":
        """Return a new registry, leaving shared defaults unchanged."""
        if not isinstance(outcome_type, type) or not issubclass(outcome_type, Outcome):
            raise TypeError("outcome_type must be an Outcome subclass")
        if not callable(handler):
            raise TypeError("outcome handler must be callable")
        if async_handler is not None and not callable(async_handler):
            raise TypeError("async outcome handler must be callable")
        rules = dict(self._rules)
        rules[outcome_type] = OutcomeRule(handler, directive, async_handler)
        return OutcomeRegistry(rules)

    def apply(
        self, runtime: OutcomeRuntime, outcome: Outcome
    ) -> OutcomeDirective:
        rule = self._resolve(outcome)
        target = (
            runtime._legacy_machine  # type: ignore[attr-defined]
            if rule.legacy_machine
            else runtime
        )
        value = rule.handler(target, outcome)
        if inspect.isawaitable(value):
            close = getattr(value, "close", None)
            if close is not None:
                close()
            raise AsyncActionRequired(
                "async outcome handler requires an async Machine entrypoint"
            )
        return rule.directive

    async def apply_async(
        self, runtime: OutcomeRuntime, outcome: Outcome
    ) -> OutcomeDirective:
        rule = self._resolve(outcome)
        handler = rule.async_handler or rule.handler
        target = (
            runtime._legacy_machine  # type: ignore[attr-defined]
            if rule.legacy_machine
            else runtime
        )
        value = handler(target, outcome)
        if inspect.isawaitable(value):
            await value
        return rule.directive

    def _resolve(self, outcome: Outcome) -> OutcomeRule:
        for outcome_type in type(outcome).__mro__:
            rule = self._rules.get(outcome_type)
            if rule is not None:
                return rule
        raise TypeError(f"unsupported outcome: {outcome!r}")


def default_outcome_interpreter(
    custom_handlers: Optional[Mapping[type[Outcome], LegacyOutcomeHandler]] = None,
) -> OutcomeRegistry:
    """Build the standard interpreter plus compatibility custom handlers."""
    registry = _DEFAULT_OUTCOME_INTERPRETER
    for outcome_type, handler in (custom_handlers or {}).items():
        rules = dict(registry.rules)
        rules[outcome_type] = OutcomeRule(handler, legacy_machine=True)  # type: ignore[arg-type]
        registry = OutcomeRegistry(rules)
    return registry


def _emit(runtime: OutcomeRuntime, outcome: Outcome) -> None:
    assert isinstance(outcome, Emit)
    runtime.publish(outcome.event, outcome.payload)


async def _emit_async(runtime: OutcomeRuntime, outcome: Outcome) -> None:
    assert isinstance(outcome, Emit)
    await runtime.publish_async(outcome.event, outcome.payload)


def _update(runtime: OutcomeRuntime, outcome: Outcome) -> None:
    assert isinstance(outcome, Update)
    runtime.update(outcome.data)


def _wait(runtime: OutcomeRuntime, outcome: Outcome) -> None:
    assert isinstance(outcome, Wait)
    runtime.wait(outcome.delay, outcome.resume_event)


def _retry(runtime: OutcomeRuntime, outcome: Outcome) -> None:
    assert isinstance(outcome, Retry)
    runtime.retry(outcome)


def _stop(runtime: OutcomeRuntime, outcome: Outcome) -> None:
    assert isinstance(outcome, Stop)
    runtime.stop(outcome.reason)


def _fail(runtime: OutcomeRuntime, outcome: Outcome) -> None:
    assert isinstance(outcome, Fail)
    runtime.fail(outcome.error)


def _next(runtime: OutcomeRuntime, outcome: Outcome) -> None:
    assert isinstance(outcome, Next)
    runtime.update(outcome.updates)


def _fork(runtime: OutcomeRuntime, outcome: Outcome) -> None:
    assert isinstance(outcome, Fork)
    runtime.start_fork(outcome)


async def _fork_async(runtime: OutcomeRuntime, outcome: Outcome) -> None:
    assert isinstance(outcome, Fork)
    await runtime.start_fork_async(outcome)


_DEFAULT_OUTCOME_INTERPRETER = OutcomeRegistry(
    {
        Emit: OutcomeRule(_emit, OutcomeDirective.CONTINUE, _emit_async),
        Update: OutcomeRule(_update, OutcomeDirective.CONTINUE),
        Wait: OutcomeRule(_wait),
        Retry: OutcomeRule(_retry),
        Stop: OutcomeRule(_stop),
        Fail: OutcomeRule(_fail),
        Next: OutcomeRule(_next),
        Fork: OutcomeRule(_fork, async_handler=_fork_async),
    }
)
