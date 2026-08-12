"""Runtime 的默认进程内 backend 实现。"""

from __future__ import annotations

import time
from collections import defaultdict, deque
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, replace
from functools import partial
from threading import Condition, RLock

from ..core import _validate_timeout, require_non_empty_string
from ..events import Event
from ..slots import SlotPool
from .protocols import EventHandler, Work, WorkHandler


class MemoryEventBus:
    """同步、进程内的 EventBus 默认实现。"""

    def __init__(self) -> None:
        self._handlers: dict[str, dict[str, list[EventHandler]]] = defaultdict(
            lambda: defaultdict(list)
        )
        self._anonymous = 0
        self._next_handler: dict[tuple[str, str], int] = defaultdict(int)
        self._condition = Condition(RLock())
        self._active_dispatches = 0
        self._closed = False

    @property
    def idle(self) -> bool:
        with self._condition:
            return self._active_dispatches == 0

    def subscribe(
        self,
        event_type: str,
        handler: EventHandler,
        *,
        subscription: str | None = None,
    ) -> None:
        event_type = require_non_empty_string(
            event_type,
            "subscription event type",
        )
        if not callable(handler):
            raise TypeError("event handler must be callable")
        with self._condition:
            if self._closed:
                raise RuntimeError("event bus is closed")
            if subscription is None:
                self._anonymous += 1
                subscription = f"__anonymous__:{self._anonymous}"
            else:
                subscription = require_non_empty_string(
                    subscription, "event subscription"
                )
            self._handlers[event_type][subscription].append(handler)

    def publish(self, event: Event) -> None:
        if not isinstance(event, Event):
            raise TypeError("event bus accepts only Event")
        with self._condition:
            if self._closed:
                if event._slot_lease is not None:
                    event._slot_lease.release()
                raise RuntimeError("event bus is closed")
            handlers = self._select_handlers(event.type)
            if event.type != "*":
                handlers += self._select_handlers("*")
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
            if event._slot_lease is not None:
                event._slot_lease.release()
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

    def _select_handlers(self, event_type: str) -> tuple[EventHandler, ...]:
        selected: list[EventHandler] = []
        for subscription, handlers in self._handlers.get(event_type, {}).items():
            key = (event_type, subscription)
            index = self._next_handler[key] % len(handlers)
            self._next_handler[key] += 1
            selected.append(handlers[index])
        return tuple(selected)


@dataclass(slots=True)
class _Consumer:
    handler: WorkHandler
    concurrency: int
    executor: ThreadPoolExecutor
    slots: SlotPool
    active: int = 0


@dataclass(slots=True)
class _Channel:
    consumers: list[_Consumer]
    queued: deque[Work]
    next_consumer: int = 0


class MemoryTaskBackend:
    """使用内存队列和线程池执行 Work 的默认实现。"""

    def __init__(self) -> None:
        self._channels: dict[str, _Channel] = {}
        self._pending: set[Future[None]] = set()
        self._failures: deque[BaseException] = deque()
        self._condition = Condition(RLock())
        self._slot_subscriptions: dict[int, tuple[SlotPool, Callable[[], None]]] = {}
        self._owned_slot_pools: list[SlotPool] = []
        self._next_channel = 0
        self._closed = False

    @property
    def idle(self) -> bool:
        with self._condition:
            return not self._pending and not any(
                channel.queued for channel in self._channels.values()
            )

    def bind(
        self,
        queue: str,
        handler: WorkHandler,
        *,
        concurrency: int,
        slots: SlotPool | None = None,
    ) -> None:
        queue = require_non_empty_string(queue, "task queue")
        if not callable(handler):
            raise TypeError("work handler must be callable")
        if type(concurrency) is not int:
            raise TypeError("queue concurrency must be an integer")
        if concurrency < 1:
            raise ValueError("queue concurrency must be at least 1")
        if slots is not None and not isinstance(slots, SlotPool):
            raise TypeError("slots must be a SlotPool or None")
        with self._condition:
            if self._closed:
                raise RuntimeError("task backend is closed")
            if slots is None:
                slots = SlotPool(concurrency)
                self._owned_slot_pools.append(slots)
            executor = ThreadPoolExecutor(
                max_workers=concurrency,
                thread_name_prefix=f"bricks-{queue}",
            )
            consumer = _Consumer(handler, concurrency, executor, slots)
            channel = self._channels.setdefault(queue, _Channel([], deque()))
            channel.consumers.append(consumer)
            self._subscribe_slots(slots)
            self._drain_channel(channel)
            self._condition.notify_all()

    def submit(self, queue: str, work: Work) -> None:
        queue = require_non_empty_string(queue, "task queue")
        if not isinstance(work, Work):
            raise TypeError("task backend accepts only Work")
        with self._condition:
            if self._closed:
                raise RuntimeError("task backend is closed")
            channel = self._channels.setdefault(queue, _Channel([], deque()))
            channel.queued.append(work)
            self._drain_channel(channel)

    def wait_idle(self, timeout: float | None = None) -> None:
        _validate_timeout(timeout)
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._condition:
            while not self.idle:
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
            consumers = tuple(
                consumer
                for channel in self._channels.values()
                for consumer in channel.consumers
            )
        for consumer in consumers:
            consumer.executor.shutdown(wait=True)
        for _, unsubscribe in tuple(self._slot_subscriptions.values()):
            unsubscribe()
        for slots in reversed(self._owned_slot_pools):
            slots.close()
        if failure is not None:
            raise failure

    def _done(
        self,
        consumer: _Consumer,
        work: Work,
        future: Future[None],
    ) -> None:
        failure = future.exception()
        with self._condition:
            self._pending.discard(future)
            consumer.active -= 1
            if failure is not None:
                self._failures.append(failure)
        if work._slot_lease is not None:
            work._slot_lease.release()
        with self._condition:
            self._drain_all()
            self._condition.notify_all()

    def _dispatch(self, consumer: _Consumer, work: Work) -> None:
        consumer.active += 1
        try:
            future = consumer.executor.submit(consumer.handler, work)
        except BaseException:
            consumer.active -= 1
            if work._slot_lease is not None:
                work._slot_lease.release()
            raise
        self._pending.add(future)
        future.add_done_callback(partial(self._done, consumer, work))

    def _drain_all(self) -> None:
        channels = tuple(self._channels.values())
        if not channels:
            return
        start = self._next_channel % len(channels)
        for offset in range(len(channels)):
            self._drain_channel(channels[(start + offset) % len(channels)])
        self._next_channel = (start + 1) % len(channels)

    def _drain_channel(self, channel: _Channel) -> None:
        while channel.queued and channel.consumers:
            available = self._available_consumers(channel)
            if not available:
                return
            continuation = next(
                (
                    index
                    for index, work in enumerate(channel.queued)
                    if work._slot_lease is not None
                ),
                None,
            )
            if continuation is not None:
                work = channel.queued[continuation]
                del channel.queued[continuation]
                consumer = available[0]
                self._advance_consumer(channel, consumer)
                self._dispatch(consumer, work)
                continue

            work = channel.queued[0]
            assigned: tuple[_Consumer, Work] | None = None
            for consumer in available:
                lease = consumer.slots._try_acquire()
                if lease is not None:
                    assigned = (consumer, replace(work, _slot_lease=lease))
                    break
            if assigned is None:
                return
            channel.queued.popleft()
            consumer, work = assigned
            self._advance_consumer(channel, consumer)
            self._dispatch(consumer, work)

    @staticmethod
    def _advance_consumer(channel: _Channel, consumer: _Consumer) -> None:
        index = channel.consumers.index(consumer)
        channel.next_consumer = index + 1

    @staticmethod
    def _available_consumers(channel: _Channel) -> list[_Consumer]:
        count = len(channel.consumers)
        start = channel.next_consumer % count
        ordered = channel.consumers[start:] + channel.consumers[:start]
        return [consumer for consumer in ordered if consumer.active < consumer.concurrency]

    def _subscribe_slots(self, slots: SlotPool) -> None:
        key = id(slots)
        if key in self._slot_subscriptions:
            return

        def available() -> None:
            with self._condition:
                if not self._closed:
                    self._drain_all()
                    self._condition.notify_all()

        self._slot_subscriptions[key] = (slots, slots._subscribe(available))
