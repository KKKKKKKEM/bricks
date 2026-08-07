"""补偿流程 / Saga 语义。"""

from __future__ import annotations

import inspect
from dataclasses import dataclass
from typing import Any, Iterable, Optional, TYPE_CHECKING

from ..events.messages import Event
from ..runtime.executor import ActionExecutor
from ..runtime.lifecycle import Status
from ..types import Action

if TYPE_CHECKING:
    from ..events.hooks import HookContext
    from ..runtime.machine import Machine


@dataclass(frozen=True, slots=True)
class CompensationStep:
    """一个正向操作对应的补偿 Action。"""

    name: str
    action: Action


@dataclass(frozen=True, slots=True)
class CompensationResult:
    """一次补偿 Action 的执行结果。"""

    name: str
    ok: bool
    value: Any = None
    error: Optional[BaseException] = None


class CompensationPlan:
    """按 LIFO 顺序记录和执行补偿步骤。

    计划本身不决定何时触发补偿；SagaRuntime 通过 Machine Hook 自动记录，
    上层也可以手动调用 ``record``，再在失败边界调用 ``compensate``。
    """

    def __init__(self, steps: Iterable[CompensationStep] = ()) -> None:
        self._definitions = {step.name: step for step in steps}
        self._completed: list[CompensationStep] = []

    @property
    def completed(self) -> tuple[CompensationStep, ...]:
        return tuple(self._completed)

    def add(self, name: str, action: Action) -> "CompensationPlan":
        self._definitions[name] = CompensationStep(name, action)
        return self

    def record(
        self, step: CompensationStep | str, action: Optional[Action] = None
    ) -> CompensationStep:
        if isinstance(step, str):
            if action is None:
                try:
                    resolved = self._definitions[step]
                except KeyError as exc:
                    raise KeyError(f"unknown compensation step: {step!r}") from exc
            else:
                resolved = CompensationStep(step, action)
        else:
            resolved = step
        self._completed.append(resolved)
        return resolved

    def clear(self) -> None:
        self._completed.clear()

    def compensate(
        self,
        machine: "Machine",
        *,
        event: Optional[Event] = None,
        executor: Optional[ActionExecutor] = None,
    ) -> tuple[CompensationResult, ...]:
        """同步逆序执行已记录的补偿步骤，并继续执行其它步骤。"""
        executor = executor or machine.action_executor
        event = event or Event("__compensate__", source=machine.context.run_id)
        results = []
        for step in reversed(self._completed):
            try:
                value = executor.execute(step.action, machine.context, event)
                results.append(CompensationResult(step.name, True, value=value))
            except Exception as error:
                results.append(CompensationResult(step.name, False, error=error))
        return tuple(results)

    async def compensate_async(
        self,
        machine: "Machine",
        *,
        event: Optional[Event] = None,
        executor: Optional[ActionExecutor] = None,
    ) -> tuple[CompensationResult, ...]:
        """异步逆序执行已记录的补偿步骤，并继续执行其它步骤。"""
        executor = executor or machine.action_executor
        event = event or Event("__compensate__", source=machine.context.run_id)
        results = []
        for step in reversed(self._completed):
            try:
                value = executor.execute_async(step.action, machine.context, event)
                if inspect.isawaitable(value):
                    value = await value
                results.append(CompensationResult(step.name, True, value=value))
            except Exception as error:
                results.append(CompensationResult(step.name, False, error=error))
        return tuple(results)


class SagaRuntime:
    """从 transition.after Hook 自动收集图迁移上的补偿声明。"""

    def __init__(self, machine: "Machine", plan: CompensationPlan) -> None:
        self.machine = machine
        self.plan = plan
        self._subscription = machine.hooks.on("transition.after", self._record_transition)

    def _record_transition(self, hook: "HookContext") -> None:
        result = hook.result
        if result is None or result.status in {Status.FAILED, Status.STOPPED}:
            return
        action = (hook.transition.metadata or {}).get("compensate")
        if callable(action):
            self.plan.record(hook.transition.id, action)

    def close(self) -> bool:
        return self.machine.hooks.unsubscribe(self._subscription)

    def compensate(self) -> tuple[CompensationResult, ...]:
        return self.plan.compensate(self.machine)

    async def compensate_async(self) -> tuple[CompensationResult, ...]:
        return await self.plan.compensate_async(self.machine)
