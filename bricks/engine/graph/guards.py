"""可组合的迁移 Guard。"""

from __future__ import annotations

import inspect
from dataclasses import dataclass
from typing import Any

from ..types import Guard as GuardProtocol

Guard = GuardProtocol


def always(context: Any, event: Any) -> bool:
    """始终允许迁移的 Guard。"""
    return True


@dataclass(frozen=True, slots=True)
class Predicate:
    """将可调用对象包装成 Guard 对象。"""

    predicate: Guard

    def __call__(self, context: Any, event: Any) -> Any:
        result = self.predicate(context, event)
        if inspect.isawaitable(result):

            async def evaluate() -> bool:
                return bool(await result)

            return evaluate()
        return bool(result)


@dataclass(frozen=True, slots=True)
class AllOf:
    guards: tuple[Guard, ...]

    def __init__(self, *guards: Guard) -> None:
        object.__setattr__(self, "guards", tuple(guards))

    def __call__(self, context: Any, event: Any) -> Any:
        for index, guard in enumerate(self.guards):
            result = guard(context, event)
            if inspect.isawaitable(result):
                return self._finish_async(index, result, context, event)
            if not result:
                return False
        return True

    async def _finish_async(
        self,
        index: int,
        result: Any,
        context: Any,
        event: Any,
    ) -> bool:
        if not await result:
            return False
        for guard in self.guards[index + 1 :]:
            result = guard(context, event)
            if inspect.isawaitable(result):
                result = await result
            if not result:
                return False
        return True


@dataclass(frozen=True, slots=True)
class AnyOf:
    guards: tuple[Guard, ...]

    def __init__(self, *guards: Guard) -> None:
        object.__setattr__(self, "guards", tuple(guards))

    def __call__(self, context: Any, event: Any) -> Any:
        for index, guard in enumerate(self.guards):
            result = guard(context, event)
            if inspect.isawaitable(result):
                return self._finish_async(index, result, context, event)
            if result:
                return True
        return False

    async def _finish_async(
        self,
        index: int,
        result: Any,
        context: Any,
        event: Any,
    ) -> bool:
        if await result:
            return True
        for guard in self.guards[index + 1 :]:
            result = guard(context, event)
            if inspect.isawaitable(result):
                result = await result
            if result:
                return True
        return False


@dataclass(frozen=True, slots=True)
class Not:
    guard: Guard

    def __call__(self, context: Any, event: Any) -> Any:
        result = self.guard(context, event)
        if inspect.isawaitable(result):

            async def evaluate() -> bool:
                return not await result

            return evaluate()
        return not result
