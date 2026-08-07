"""可替换的迁移选择策略。"""

from __future__ import annotations

import inspect
from typing import Any, Protocol

from ..errors import AsyncGuardRequired
from ..events.messages import Event
from ..graph.graph import Graph
from ..graph.transitions import Transition


class TransitionSelector(Protocol):
    """从静态 Graph 中选择一次事件对应的迁移。"""

    def select(
        self,
        graph: Graph,
        source: str,
        event: Event,
        context: Any,
    ) -> Transition | None: ...

    async def select_async(
        self,
        graph: Graph,
        source: str,
        event: Event,
        context: Any,
    ) -> Transition | None: ...


class DefaultTransitionSelector:
    """按优先级、声明顺序和 Guard 选择第一条符合条件的边。"""

    def select(
        self,
        graph: Graph,
        source: str,
        event: Event,
        context: Any,
    ) -> Transition | None:
        for transition in graph.transitions_from(source, event.name):
            if transition.guard is None:
                return transition
            allowed = transition.guard(context, event)
            if inspect.isawaitable(allowed):
                close = getattr(allowed, "close", None)
                if close is not None:
                    close()
                raise AsyncGuardRequired(
                    "async guard requires select_async() or dispatch_async()"
                )
            if allowed:
                return transition
        return None

    async def select_async(
        self,
        graph: Graph,
        source: str,
        event: Event,
        context: Any,
    ) -> Transition | None:
        for transition in graph.transitions_from(source, event.name):
            if transition.guard is None:
                return transition
            allowed = transition.guard(context, event)
            if inspect.isawaitable(allowed):
                allowed = await allowed
            if allowed:
                return transition
        return None
