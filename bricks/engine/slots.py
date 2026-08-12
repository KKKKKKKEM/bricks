"""可跨 Work 传递并长期复用状态的逻辑执行槽。"""

from __future__ import annotations

import time
from collections import deque
from collections.abc import Callable, Iterable, Iterator, Mapping, MutableMapping
from threading import Condition, RLock
from typing import Any
from uuid import uuid4

from .core import _validate_timeout, require_non_empty_string


class Slot(MutableMapping[str, Any]):
    """保存一条逻辑执行链可复用的状态，与线程和 Worker 无关。"""

    def __init__(
        self,
        values: Mapping[str, Any] | None = None,
        *,
        id: str | None = None,
    ) -> None:
        self.id = require_non_empty_string(
            str(uuid4()) if id is None else id,
            "slot id",
        )
        self._values: dict[str, Any] = {} if values is None else dict(values)
        self._execution_lock = RLock()

    def __getitem__(self, key: str) -> Any:
        return self._values[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self._values[key] = value

    def __delitem__(self, key: str) -> None:
        del self._values[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._values)

    def __len__(self) -> int:
        return len(self._values)

    def __repr__(self) -> str:
        return f"Slot(id={self.id!r})"


class SlotPool:
    """管理一组同构 Slot；同一个池可以被多个 Consumer 共享。"""

    def __init__(
        self,
        size: int | None = None,
        *,
        slots: Iterable[Slot] | None = None,
        factory: Callable[[], Slot] | None = None,
    ) -> None:
        if slots is not None and size is not None:
            raise TypeError("size and slots are mutually exclusive")
        if slots is not None and factory is not None:
            raise TypeError("factory cannot be combined with slots")
        if factory is not None and not callable(factory):
            raise TypeError("slot factory must be callable")
        if slots is None:
            if type(size) is not int:
                raise TypeError("slot pool size must be an integer")
            if size < 1:
                raise ValueError("slot pool size must be at least 1")
            if factory is None:
                factory = Slot
            created = tuple(factory() for _ in range(size))
        else:
            created = tuple(slots)
            if not created:
                raise ValueError("slot pool must contain at least one Slot")
        if any(not isinstance(slot, Slot) for slot in created):
            raise TypeError("slot pool accepts only Slot instances")
        if len({id(slot) for slot in created}) != len(created):
            raise ValueError("slot pool must not contain duplicate Slot instances")
        if len({slot.id for slot in created}) != len(created):
            raise ValueError("slot pool must not contain duplicate Slot ids")

        self._slots = created
        self._available = deque(created)
        self._condition = Condition(RLock())
        self._listeners: list[Callable[[], None]] = []
        self._closed = False

    @property
    def size(self) -> int:
        return len(self._slots)

    @property
    def available(self) -> int:
        with self._condition:
            return len(self._available)

    def close(self) -> None:
        with self._condition:
            if self._closed:
                return
            self._closed = True
            self._condition.notify_all()

    def _try_acquire(self) -> _SlotLease | None:
        with self._condition:
            if self._closed:
                raise RuntimeError("slot pool is closed")
            if not self._available:
                return None
            return _SlotLease(self, self._available.popleft())

    def _subscribe(self, listener: Callable[[], None]) -> Callable[[], None]:
        if not callable(listener):
            raise TypeError("slot pool listener must be callable")
        with self._condition:
            if self._closed:
                raise RuntimeError("slot pool is closed")
            self._listeners.append(listener)

        def unsubscribe() -> None:
            with self._condition:
                if listener in self._listeners:
                    self._listeners.remove(listener)

        return unsubscribe

    def _acquire(self, timeout: float | None = None) -> _SlotLease:
        _validate_timeout(timeout)
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._condition:
            while not self._available:
                if self._closed:
                    raise RuntimeError("slot pool is closed")
                remaining = (
                    None if deadline is None else deadline - time.monotonic()
                )
                if remaining is not None and remaining <= 0:
                    raise TimeoutError("slot pool did not provide a Slot")
                self._condition.wait(remaining)
            if self._closed:
                raise RuntimeError("slot pool is closed")
            return _SlotLease(self, self._available.popleft())

    def _release(self, slot: Slot) -> None:
        with self._condition:
            if all(slot is not item for item in self._slots):
                raise ValueError("Slot does not belong to this pool")
            if any(slot is item for item in self._available):
                raise RuntimeError("Slot is already available")
            self._available.append(slot)
            self._condition.notify()
            listeners = tuple(self._listeners)
        for listener in listeners:
            listener()


class _SlotLease:
    """对执行链所持 Slot 做内部引用计数，最后一个 Work 结束时归还。"""

    __slots__ = ("_lock", "_pool", "_references", "slot")

    def __init__(self, pool: SlotPool, slot: Slot) -> None:
        self.slot = slot
        self._pool = pool
        self._references = 1
        self._lock = RLock()

    def retain(self) -> None:
        with self._lock:
            if self._references == 0:
                raise RuntimeError("slot lease is already released")
            self._references += 1

    def release(self) -> None:
        slot: Slot | None = None
        with self._lock:
            if self._references == 0:
                raise RuntimeError("slot lease is already released")
            self._references -= 1
            if self._references == 0:
                slot = self.slot
        if slot is not None:
            self._pool._release(slot)
