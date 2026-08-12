"""Runtime 的默认进程内 backend 实现。"""

from __future__ import annotations

import time
from collections import defaultdict, deque
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from threading import Condition, RLock

from ..core import _validate_timeout, require_non_empty_string
from ..events import Event
from .protocols import EventHandler, Work, WorkHandler


class MemoryEventBus:
    """同步、进程内的 EventBus 默认实现。"""

    def __init__(self) -> None:
        self._handlers: dict[str, list[EventHandler]] = defaultdict(list)
        self._condition = Condition(RLock())
        self._active_dispatches = 0
        self._closed = False

    @property
    def idle(self) -> bool:
        with self._condition:
            return self._active_dispatches == 0

    def subscribe(self, event_type: str, handler: EventHandler) -> None:
        event_type = require_non_empty_string(
            event_type,
            "subscription event type",
        )
        if not callable(handler):
            raise TypeError("event handler must be callable")
        with self._condition:
            if self._closed:
                raise RuntimeError("event bus is closed")
            self._handlers[event_type].append(handler)

    def publish(self, event: Event) -> None:
        if not isinstance(event, Event):
            raise TypeError("event bus accepts only Event")
        with self._condition:
            if self._closed:
                raise RuntimeError("event bus is closed")
            handlers = tuple(self._handlers.get(event.type, ()))
            if event.type != "*":
                handlers += tuple(self._handlers.get("*", ()))
            self._active_dispatches += 1
        failure: Exception | None = None
        try:
            for handler in handlers:
                try:
                    handler(event)
                except Exception as exc:  # noqa: BLE001
                    # 一个观察者失败不应阻断同一 Event 的其他订阅者。
                    if failure is None:
                        failure = exc
            if failure is not None:
                raise failure
        finally:
            with self._condition:
                self._active_dispatches -= 1
                self._condition.notify_all()

    def wait_idle(self, timeout: float | None = None) -> None:
        _validate_timeout(timeout)
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._condition:
            while self._active_dispatches:
                remaining = None if deadline is None else deadline - time.monotonic()
                if remaining is not None and remaining <= 0:
                    raise TimeoutError("event bus did not become idle")
                self._condition.wait(remaining)

    def close(self) -> None:
        with self._condition:
            self._closed = True


@dataclass(slots=True)
class _Channel:
    handler: WorkHandler
    concurrency: int
    executor: ThreadPoolExecutor


class MemoryTaskBackend:
    """使用内存队列和线程池执行 Work 的默认实现。"""

    def __init__(self) -> None:
        self._channels: dict[str, _Channel] = {}
        self._pending: set[Future[None]] = set()
        self._failures: deque[BaseException] = deque()
        self._condition = Condition(RLock())
        self._closed = False

    @property
    def idle(self) -> bool:
        with self._condition:
            return not self._pending

    def bind(
        self,
        queue: str,
        handler: WorkHandler,
        *,
        concurrency: int,
    ) -> None:
        queue = require_non_empty_string(queue, "task queue")
        if not callable(handler):
            raise TypeError("work handler must be callable")
        if type(concurrency) is not int:
            raise TypeError("queue concurrency must be an integer")
        if concurrency < 1:
            raise ValueError("queue concurrency must be at least 1")
        with self._condition:
            if self._closed:
                raise RuntimeError("task backend is closed")
            existing = self._channels.get(queue)
            if existing is not None:
                if existing.concurrency != concurrency:
                    raise RuntimeError(
                        f"queue {queue!r} already uses concurrency "
                        f"{existing.concurrency}"
                    )
                return
            executor = ThreadPoolExecutor(
                max_workers=concurrency,
                thread_name_prefix=f"bricks-{queue}",
            )
            self._channels[queue] = _Channel(handler, concurrency, executor)

    def submit(self, queue: str, work: Work) -> None:
        queue = require_non_empty_string(queue, "task queue")
        if not isinstance(work, Work):
            raise TypeError("task backend accepts only Work")
        with self._condition:
            if self._closed:
                raise RuntimeError("task backend is closed")
            try:
                channel = self._channels[queue]
            except KeyError as exc:
                raise RuntimeError(f"unknown task queue {queue!r}") from exc
            future = channel.executor.submit(channel.handler, work)
            self._pending.add(future)
        future.add_done_callback(self._done)

    def wait_idle(self, timeout: float | None = None) -> None:
        _validate_timeout(timeout)
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._condition:
            while self._pending:
                remaining = (
                    None if deadline is None else deadline - time.monotonic()
                )
                if remaining is not None and remaining <= 0:
                    raise TimeoutError("task backend did not become idle")
                self._condition.wait(remaining)
            if self._failures:
                failures = tuple(self._failures)
                self._failures.clear()
                raise failures[0]

    def close(self) -> None:
        with self._condition:
            if self._closed:
                return
        failure: BaseException | None = None
        try:
            self.wait_idle()
        except Exception as exc:  # noqa: BLE001
            failure = exc
        with self._condition:
            self._closed = True
            channels = tuple(self._channels.values())
        for channel in channels:
            channel.executor.shutdown(wait=True)
        if failure is not None:
            raise failure

    def _done(self, future: Future[None]) -> None:
        failure = future.exception()
        with self._condition:
            self._pending.discard(future)
            if failure is not None:
                self._failures.append(failure)
            self._condition.notify_all()
