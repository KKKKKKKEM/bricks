"""Runtime 的默认进程内 backend 实现。"""

from __future__ import annotations

import time
from collections import defaultdict, deque
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, replace
from functools import partial
from threading import Condition, RLock

from ..engine.core import _validate_timeout, require_non_empty_string
from ..engine.events import Event
from ..engine.slots import SlotPool
from ..spi import (
    Delivery,
    DeliveryOutcome,
    DeliveryResult,
    EventHandler,
    SlotLease,
    SlotProvider,
    Work,
    WorkHandler,
)


class EventBus:
    """同步、进程内的 EventBus 默认实现。

    Attributes:
        _handlers: 按事件类型和订阅组组织的处理函数。
        _anonymous: 生成独立匿名订阅名称的计数器。
        _next_handler: 各订阅组下一次轮询使用的处理器游标。
        _condition: 协调共享状态访问及同步等待的锁或条件变量。
        _active_dispatches: 尚未结束的事件分发数量。
        _closed: 当前组件是否已停止接受新工作。
    """

    def __init__(self) -> None:
        """创建独立订阅表、轮询游标和事件分发计数。"""

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
        """判断当前组件是否没有尚未完成的工作。

        Returns:
            没有未完成工作时为 True，否则为 False。
        """

        with self._condition:
            return self._active_dispatches == 0

    def subscribe(
        self,
        event_type: str,
        handler: EventHandler,
        *,
        subscription: str | None = None,
    ) -> None:
        """注册事件订阅，同名订阅组中的处理器竞争消费。

        Args:
            event_type: 用于订阅或路由匹配的事件类型。
            handler: 接收事件或投递的处理函数。
            subscription: 竞争消费组名称，None 创建独立订阅。

        Raises:
            RuntimeError: 当前生命周期状态或操作顺序不允许此操作。
            TypeError: 参数类型或接口实现不符合当前契约。
        """

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
        """发布事件并推进对应的投递或观察流程。

        Args:
            event: 需要发布、观察或处理的事件。

        Raises:
            RuntimeError: 当前生命周期状态或操作顺序不允许此操作。
            TypeError: 参数类型或接口实现不符合当前契约。
        """

        if not isinstance(event, Event):
            raise TypeError("event bus accepts only Event")
        with self._condition:
            if self._closed:
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
            with self._condition:
                self._active_dispatches -= 1
                self._condition.notify_all()

    def wait_idle(self, timeout: float | None = None) -> None:
        """等待已接受的工作完成，并传播已记录的失败。

        Args:
            timeout: 等待或执行时限，单位秒；None 表示不设置时限。

        Raises:
            TimeoutError: 等待未在指定时限内完成。
        """

        _validate_timeout(timeout)
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._condition:
            while self._active_dispatches:
                remaining = None if deadline is None else deadline - time.monotonic()
                if remaining is not None and remaining <= 0:
                    raise TimeoutError("event bus did not become idle")
                self._condition.wait(remaining)

    def close(self) -> None:
        """停止接受新的事件订阅和发布。"""

        with self._condition:
            self._closed = True

    def _select_handlers(self, event_type: str) -> tuple[EventHandler, ...]:
        """为每个订阅组轮询选择一个事件处理器。

        Args:
            event_type: 用于订阅或路由匹配的事件类型。

        Returns:
            用于卸载本次注册的句柄。
        """

        selected: list[EventHandler] = []
        for subscription, handlers in self._handlers.get(event_type, {}).items():
            key = (event_type, subscription)
            index = self._next_handler[key] % len(handlers)
            self._next_handler[key] += 1
            selected.append(handlers[index])
        return tuple(selected)


@dataclass(slots=True)
class _Consumer:
    """单个内存消费者的线程池、资源池与并发计数。

    Attributes:
        handler: 当前消费者处理投递的函数。
        concurrency: 当前消费者的完整 Graph 执行并发上限。
        executor: 当前消费者拥有的线程池执行器。
        slots: 消费者申请根执行链资源所用的 SlotProvider。
        active: 当前消费者正在执行的 Graph 数量。
    """

    handler: WorkHandler
    concurrency: int
    executor: ThreadPoolExecutor
    slots: SlotProvider
    active: int = 0


@dataclass(slots=True)
class _Channel:
    """命名消费通道的排队投递与轮询状态。

    Attributes:
        consumers: 通道内按注册顺序排列的消费者。
        queued: 等待执行的投递队列。
        next_consumer: 下一次消费者轮询的起始游标。
    """

    consumers: list[_Consumer]
    queued: deque[Delivery]
    next_consumer: int = 0


class TaskBackend:
    """使用内存队列和线程池执行 Work 的默认实现。

    Attributes:
        _max_delivery_attempts: 最大投递次数，包含首次尝试。
        _channels: 按名称组织的内存消费通道。
        _pending: 已经提交、尚未完成的消费任务。
        _failures: 等待向调用方传播的失败记录。
        _condition: 协调共享状态访问及同步等待的锁或条件变量。
        _slot_subscriptions: 资源池身份对应的可用通知取消函数。
        _owned_slot_pools: 由当前组件创建并负责关闭的 Slot 池。
        _next_channel: 下一次通道轮询的起始游标。
        _closed: 当前组件是否已停止接受新工作。
    """

    def __init__(self, *, max_delivery_attempts: int = 3) -> None:
        """创建内存消费通道容器，并校验最大交付次数。

        Args:
            max_delivery_attempts: 内存任务后端允许的最大交付次数，包含首次交付。

        Raises:
            ValueError: 参数值或字段组合不合法。
        """

        if type(max_delivery_attempts) is not int or max_delivery_attempts < 1:
            raise ValueError(
                "max_delivery_attempts must be an integer greater than zero"
            )
        self._max_delivery_attempts = max_delivery_attempts
        self._channels: dict[str, _Channel] = {}
        self._pending: set[Future[DeliveryResult]] = set()
        self._failures: deque[BaseException] = deque()
        self._condition = Condition(RLock())
        self._slot_subscriptions: dict[
            int, tuple[SlotProvider, Callable[[], None]]
        ] = {}
        self._owned_slot_pools: list[SlotPool] = []
        self._next_channel = 0
        self._closed = False

    @property
    def idle(self) -> bool:
        """判断当前组件是否没有尚未完成的工作。

        Returns:
            没有未完成工作时为 True，否则为 False。
        """

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
        slots: SlotProvider | None = None,
    ) -> None:
        """绑定消费通道及其处理器和本地并发配置。

        Args:
            queue: 命名消费通道。
            handler: 接收事件或投递的处理函数。
            concurrency: 当前消费者允许并行执行的完整 Graph 数量。
            slots: 提供本地执行槽的资源池能力。

        Raises:
            RuntimeError: 当前生命周期状态或操作顺序不允许此操作。
            TypeError: 参数类型或接口实现不符合当前契约。
            ValueError: 参数值或字段组合不合法。
        """

        queue = require_non_empty_string(queue, "task queue")
        if not callable(handler):
            raise TypeError("work handler must be callable")
        if type(concurrency) is not int:
            raise TypeError("queue concurrency must be an integer")
        if concurrency < 1:
            raise ValueError("queue concurrency must be at least 1")
        if slots is not None and not isinstance(slots, SlotProvider):
            raise TypeError("slots must implement SlotProvider or be None")
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
        """向命名执行通道提交工作。

        Args:
            queue: 命名消费通道。
            work: 需要投递或执行的工作请求。

        Raises:
            RuntimeError: 当前生命周期状态或操作顺序不允许此操作。
            TypeError: 参数类型或接口实现不符合当前契约。
        """

        queue = require_non_empty_string(queue, "task queue")
        if not isinstance(work, Work):
            raise TypeError("task backend accepts only Work")
        with self._condition:
            if self._closed:
                raise RuntimeError("task backend is closed")
            channel = self._channels.setdefault(queue, _Channel([], deque()))
            channel.queued.append(Delivery(work))
            self._drain_channel(channel)

    def submit_local(self, queue: str, work: Work, lease: SlotLease) -> None:
        """提交延续当前进程 Slot 链的 Work。

        Args:
            queue: 命名消费通道。
            work: 需要投递或执行的工作请求。
            lease: 当前进程内执行槽的引用与串行执行能力。

        Raises:
            RuntimeError: 当前生命周期状态或操作顺序不允许此操作。
            TypeError: 参数类型或接口实现不符合当前契约。
        """

        queue = require_non_empty_string(queue, "task queue")
        if not isinstance(work, Work):
            raise TypeError("task backend accepts only Work")
        with self._condition:
            if self._closed:
                raise RuntimeError("task backend is closed")
            channel = self._channels.setdefault(queue, _Channel([], deque()))
            channel.queued.append(Delivery(work, slot_lease=lease))
            self._drain_channel(channel)

    def wait_idle(self, timeout: float | None = None) -> None:
        """等待已接受的工作完成，并传播已记录的失败。

        Args:
            timeout: 等待或执行时限，单位秒；None 表示不设置时限。

        Raises:
            TimeoutError: 等待未在指定时限内完成。
        """

        _validate_timeout(timeout)
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._condition:
            while not self.idle:
                remaining = None if deadline is None else deadline - time.monotonic()
                if remaining is not None and remaining <= 0:
                    raise TimeoutError("task backend did not become idle")
                self._condition.wait(remaining)
            if self._failures:
                failures = tuple(self._failures)
                self._failures.clear()
                raise failures[0]

    def close(self) -> None:
        """结束当前组件的生命周期并释放其拥有的资源。"""

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
        delivery: Delivery,
        future: Future[DeliveryResult],
    ) -> None:
        """处理消费结果、重投递和失败记录，并归还投递持有的引用。

        Args:
            consumer: 本次消费对应的消费者记录或消费能力。
            delivery: 携带尝试次数和可选 Slot lease 的本次投递。
            future: 线程池或事件循环提交返回的结果句柄。
        """

        failure = future.exception()
        result = None if failure is not None else future.result()
        with self._condition:
            self._pending.discard(future)
            consumer.active -= 1
            if failure is not None:
                self._failures.append(failure)
            elif not isinstance(result, DeliveryResult):
                failure = TypeError("work handler must return DeliveryResult")
                self._failures.append(failure)
            elif result.outcome is DeliveryOutcome.RETRY:
                if delivery.attempt >= self._max_delivery_attempts:
                    self._failures.append(
                        result.error
                        or RuntimeError(
                            f"work {delivery.work.id!r} exhausted "
                            f"max_delivery_attempts={self._max_delivery_attempts}"
                        )
                    )
                else:
                    if delivery.slot_lease is not None:
                        delivery.slot_lease.retain()
                    channel = next(
                        channel
                        for channel in self._channels.values()
                        if consumer in channel.consumers
                    )
                    channel.queued.appendleft(
                        Delivery(
                            delivery.work,
                            delivery.attempt + 1,
                            slot_lease=delivery.slot_lease,
                        )
                    )
            elif result.outcome is DeliveryOutcome.REJECT and result.error is not None:
                self._failures.append(result.error)
        if delivery.slot_lease is not None:
            delivery.slot_lease.release()
        with self._condition:
            self._drain_all()
            self._condition.notify_all()

    def _dispatch(self, consumer: _Consumer, delivery: Delivery) -> None:
        """将已取得执行资源的投递交给消费者线程池。

        Args:
            consumer: 本次消费对应的消费者记录或消费能力。
            delivery: 携带尝试次数和可选 Slot lease 的本次投递。
        """

        consumer.active += 1
        try:
            future = consumer.executor.submit(consumer.handler, delivery)
        except BaseException as exc:
            consumer.active -= 1
            self._failures.append(exc)
            if delivery.slot_lease is not None:
                delivery.slot_lease.release()
            self._condition.notify_all()
            return
        self._pending.add(future)
        future.add_done_callback(partial(self._done, consumer, delivery))

    def _drain_all(self) -> None:
        """轮询各通道，尝试推进已经具备资源的投递。"""

        channels = tuple(self._channels.values())
        if not channels:
            return
        start = self._next_channel % len(channels)
        for offset in range(len(channels)):
            self._drain_channel(channels[(start + offset) % len(channels)])
        self._next_channel = (start + 1) % len(channels)

    def _drain_channel(self, channel: _Channel) -> None:
        """优先推进携带 Slot 的延续任务，再为根任务申请空闲 Slot。

        Args:
            channel: 当前处理的内存消费通道记录。
        """

        while channel.queued and channel.consumers:
            available = self._available_consumers(channel)
            if not available:
                return
            continuation = next(
                (
                    index
                    for index, delivery in enumerate(channel.queued)
                    if delivery.slot_lease is not None
                ),
                None,
            )
            if continuation is not None:
                delivery = channel.queued[continuation]
                del channel.queued[continuation]
                consumer = available[0]
                self._advance_consumer(channel, consumer)
                self._dispatch(consumer, delivery)
                continue

            delivery = channel.queued[0]
            assigned: tuple[_Consumer, Delivery] | None = None
            for consumer in available:
                lease = consumer.slots.try_acquire()
                if lease is not None:
                    assigned = (
                        consumer,
                        replace(
                            delivery,
                            slot_lease=lease,
                        ),
                    )
                    break
            if assigned is None:
                return
            channel.queued.popleft()
            consumer, delivery = assigned
            self._advance_consumer(channel, consumer)
            self._dispatch(consumer, delivery)

    @staticmethod
    def _advance_consumer(channel: _Channel, consumer: _Consumer) -> None:
        """推进通道的轮询游标，让后续投递从下一个消费者开始。

        Args:
            channel: 当前处理的内存消费通道记录。
            consumer: 本次消费对应的消费者记录或消费能力。
        """

        index = channel.consumers.index(consumer)
        channel.next_consumer = index + 1

    @staticmethod
    def _available_consumers(channel: _Channel) -> list[_Consumer]:
        """按轮询顺序取得尚未达到本地并发上限的消费者。

        Args:
            channel: 当前处理的内存消费通道记录。

        Returns:
            符合当前可用条件的选择结果，没有可执行输入时不触发。
        """

        count = len(channel.consumers)
        start = channel.next_consumer % count
        ordered = channel.consumers[start:] + channel.consumers[:start]
        return [
            consumer for consumer in ordered if consumer.active < consumer.concurrency
        ]

    def _subscribe_slots(self, slots: SlotProvider) -> None:
        """订阅 Slot 可用通知，避免重复订阅同一个池。

        Args:
            slots: 提供本地执行槽的资源池能力。
        """

        key = id(slots)
        if key in self._slot_subscriptions:
            return

        def available() -> None:
            """资源可用时重新推进等待通道，并唤醒空闲等待方。"""

            with self._condition:
                if not self._closed:
                    self._drain_all()
                    self._condition.notify_all()

        self._slot_subscriptions[key] = (slots, slots.subscribe_available(available))
