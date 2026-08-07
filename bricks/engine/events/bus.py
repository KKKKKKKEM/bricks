"""支持有序、带作用域订阅的进程内事件总线。"""

from __future__ import annotations

import inspect
import threading
import uuid
from dataclasses import dataclass
from typing import Any, Callable, Optional

from ..errors import AsyncActionRequired
from .messages import Event


@dataclass(frozen=True, slots=True)
class Subscription:
    token: str
    name: str


@dataclass
class _Listener:
    token: str
    handler: Callable[[Event], Any]
    priority: int
    once: bool
    match: Optional[Callable[[Event], bool]]
    order: int = 0


class EventBus:
    def __init__(self) -> None:
        self._listeners: dict[str, list[_Listener]] = {}
        self._reactive_routes: dict[str, tuple[str, frozenset[str]]] = {}
        self._lock = threading.RLock()
        self._order = 0

    def subscribe(
        self,
        name: str,
        handler: Callable[[Event], Any],
        *,
        priority: int = 0,
        once: bool = False,
        match: Optional[Callable[[Event], bool]] = None,
    ) -> Subscription:
        if not callable(handler):
            raise TypeError("event handler must be callable")
        with self._lock:
            self._order += 1
            listener = _Listener(
                uuid.uuid4().hex, handler, priority, once, match, self._order
            )
            bucket = self._listeners.setdefault(name, [])
            bucket.append(listener)
            bucket.sort(key=lambda item: item.priority)
        return Subscription(listener.token, name)

    on = subscribe

    def subscribe_any(
        self,
        handler: Callable[[Event], Any],
        *,
        priority: int = 0,
        once: bool = False,
        match: Optional[Callable[[Event], bool]] = None,
    ) -> Subscription:
        """订阅所有事件，可通过 match 进一步缩小范围。"""
        return self.subscribe(
            "*", handler, priority=priority, once=once, match=match
        )

    on_any = subscribe_any

    def unsubscribe(self, subscription: Subscription | str) -> bool:
        token = subscription.token if isinstance(subscription, Subscription) else subscription
        with self._lock:
            for name, listeners in list(self._listeners.items()):
                for index, listener in enumerate(listeners):
                    if listener.token == token:
                        listeners.pop(index)
                        self._reactive_routes.pop(token, None)
                        if not listeners:
                            self._listeners.pop(name, None)
                        return True
        return False

    def publish(self, event: Event | str, payload: Any = None) -> list[Any]:
        event = event if isinstance(event, Event) else Event(event, payload)
        results = []
        listeners = self._claim(event, consume_once=False)
        async_listener = next(
            (
                listener
                for listener in listeners
                if _is_async_callable(listener.handler)
            ),
            None,
        )
        if async_listener is not None:
            raise AsyncActionRequired(
                "async event handler requires publish_async(): "
                f"{async_listener.handler!r}"
            )
        self._consume_once(listeners)
        for listener in listeners:
            value = listener.handler(event)
            if inspect.isawaitable(value):
                close = getattr(value, "close", None)
                close and close()
                raise AsyncActionRequired(
                    f"async event handler requires publish_async(): {listener.handler!r}"
                )
            results.append(value)
        return results

    async def publish_async(
        self, event: Event | str, payload: Any = None
    ) -> list[Any]:
        event = event if isinstance(event, Event) else Event(event, payload)
        results = []
        for listener in self._claim(event):
            value = listener.handler(event)
            if inspect.isawaitable(value):
                value = await value
            results.append(value)
        return results

    def _claim(
        self,
        event: Event,
        *,
        consume_once: bool = True,
    ) -> list[_Listener]:
        with self._lock:
            listeners = list(self._listeners.get(event.name, ()))
            if event.name != "*":
                listeners.extend(self._listeners.get("*", ()))
            listeners.sort(key=lambda item: (item.priority, item.order))
            selected = []
            for listener in listeners:
                if listener.match is not None and not listener.match(event):
                    continue
                selected.append(listener)
            if consume_once:
                self._consume_once(selected)
            return selected

    def _consume_once(self, listeners: list[_Listener]) -> None:
        for listener in listeners:
            if listener.once:
                self.unsubscribe(listener.token)

    def _register_reactive_route(
        self, token: str, run_id: str, event_names: frozenset[str]
    ) -> None:
        """Share route ownership across ReactiveRuntime instances on this bus."""
        with self._lock:
            self._reactive_routes[token] = (run_id, event_names)

    def _unregister_reactive_route(self, token: str) -> None:
        with self._lock:
            self._reactive_routes.pop(token, None)

    def _reactive_candidates(self, event_name: str) -> frozenset[str]:
        with self._lock:
            return frozenset(
                run_id
                for run_id, names in self._reactive_routes.values()
                if event_name in names
            )


def _is_async_callable(handler: Callable[[Event], Any]) -> bool:
    return inspect.iscoroutinefunction(handler) or inspect.iscoroutinefunction(
        getattr(handler, "__call__", None)
    )
