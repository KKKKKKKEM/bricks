"""与领域 EventBus 分离的生命周期 Hook 注册表。"""

from __future__ import annotations

import inspect
import threading
import uuid
from dataclasses import dataclass
from typing import Any, Callable, Optional

from ..errors import AsyncActionRequired
from .bus import Subscription


@dataclass(frozen=True, slots=True)
class HookContext:
    name: str
    machine: Any
    context: Any
    event: Any = None
    transition: Any = None
    result: Any = None
    error: Optional[BaseException] = None
    runtime_event: Any = None


@dataclass
class _Hook:
    token: str
    handler: Callable[[HookContext], Any]
    priority: int
    once: bool
    match: Optional[Callable[[HookContext], bool]]


class HookRegistry:
    def __init__(self) -> None:
        self._hooks: dict[str, list[_Hook]] = {}
        self._lock = threading.RLock()

    def subscribe(
        self,
        name: str,
        handler: Callable[[HookContext], Any],
        *,
        priority: int = 0,
        once: bool = False,
        match: Optional[Callable[[HookContext], bool]] = None,
    ) -> Subscription:
        hook = _Hook(uuid.uuid4().hex, handler, priority, once, match)
        with self._lock:
            bucket = self._hooks.setdefault(name, [])
            bucket.append(hook)
            bucket.sort(key=lambda item: item.priority)
        return Subscription(hook.token, name)

    on = subscribe

    def unsubscribe(self, subscription: Subscription | str) -> bool:
        token = subscription.token if isinstance(subscription, Subscription) else subscription
        with self._lock:
            for name, hooks in list(self._hooks.items()):
                for index, hook in enumerate(hooks):
                    if hook.token == token:
                        hooks.pop(index)
                        if not hooks:
                            self._hooks.pop(name, None)
                        return True
        return False

    def emit(self, name: str, context: HookContext) -> list[Any]:
        results = []
        hooks = self._claim(name, context)
        async_hook = next(
            (hook for hook in hooks if _is_async_callable(hook.handler)), None
        )
        if async_hook is not None:
            raise AsyncActionRequired(
                f"async hook requires emit_async(): {async_hook.handler!r}"
            )
        for hook in hooks:
            value = hook.handler(context)
            if inspect.isawaitable(value):
                close = getattr(value, "close", None)
                close and close()
                raise AsyncActionRequired(
                    f"async hook requires emit_async(): {hook.handler!r}"
                )
            results.append(value)
        return results

    def ensure_sync(self, *names: str) -> None:
        """Reject known async hooks before a synchronous state change starts."""
        with self._lock:
            hooks = [hook for name in names for hook in self._hooks.get(name, ())]
        async_hook = next(
            (hook for hook in hooks if _is_async_callable(hook.handler)), None
        )
        if async_hook is not None:
            raise AsyncActionRequired(
                f"async hook requires an async Machine entrypoint: "
                f"{async_hook.handler!r}"
            )

    async def emit_async(self, name: str, context: HookContext) -> list[Any]:
        results = []
        for hook in self._claim(name, context):
            value = hook.handler(context)
            if inspect.isawaitable(value):
                value = await value
            results.append(value)
        return results

    def _claim(self, name: str, context: HookContext) -> list[_Hook]:
        with self._lock:
            hooks = list(self._hooks.get(name, ()))
            selected = []
            for hook in hooks:
                if hook.match is not None and not hook.match(context):
                    continue
                if hook.once:
                    self.unsubscribe(hook.token)
                selected.append(hook)
            return selected


def _is_async_callable(handler: Callable[[HookContext], Any]) -> bool:
    return inspect.iscoroutinefunction(handler) or inspect.iscoroutinefunction(
        getattr(handler, "__call__", None)
    )
