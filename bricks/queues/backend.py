"""将爬虫任务存储接入 Interlace 的公开 TaskBackend SPI。"""

from __future__ import annotations

import asyncio
import inspect
import time
from collections import deque
from collections.abc import Callable, Mapping
from functools import partial
from itertools import pairwise
from threading import Event, Lock, RLock, Thread
from uuid import uuid4

from interlace.adapters.memory import TaskBackend as LocalBackend
from interlace.engine.errors import ExecutionControlError
from interlace.spi import (
    Delivery,
    DeliveryOutcome,
    DeliveryResult,
    SlotLease,
    SlotProvider,
    Work,
    WorkHandler,
)

from .base import TaskQueue
from .models import LeaseLostError, LocalSubmission, TaskLease, seconds, text


class _Binding:
    """一个独立的队列领取器及其本地执行装配。

    Attributes:
        queue: 借用的任务存储。
        local: 自有的 Interlace 内存调度器，负责 Slot 和执行线程。
        active: 已领取但尚未结束处理的交付，按 Work ID 索引。
        lock: 保护 active、errors 和关闭状态。
        stop: 停止领取新任务的通知，已领取任务继续续租并排空。
        thread: 自有的轮询与心跳线程。
        errors: 尚未交给调用方的失败记录。
        _handler: Interlace 注入的完整执行处理器。
        _concurrency: 最多预取的交付数，等于本地执行并发。
        _ttl: 交付租约秒数。
        _interval: 轮询间隔秒数。
        _owner: 此绑定独有的 Worker 身份。
        _unbind: 本地调度器的独立注销句柄。
        _closed: 是否已完成关闭。
        _close_lock: 串行化重复关闭，所有调用均等待资源清理完成。
        _lost: 已确认失效的租约 token。
        _finishing: 正在提交结束决定的 token，不再续租。
        _expires: token 到本地租约到期时刻的映射，单位为 time.monotonic 秒；
            在发出领取或续租前取时，只与本机时钟比较，不使用存储侧 deadline。
        _waiting: 本地旧交付仍在执行时重新领取到的同 ID 租约，按 Work ID 索引；
            持续续租，旧交付结束后再调度，避免并发执行或空耗交付次数。
        _reserved: 正在执行 put_claim 的本地延续数量，非零时轮询线程不得退出。
        _retired: 轮询线程是否已退出；退出后不再接受本地延续。
        _wake: 交付结束时唤醒轮询线程的事件。
        _claim_lock: 串行化领取操作与交付结束标记。
        _before_claim: 领取前同步门控，返回延迟秒数时暂缓领取，None 放行。
        _before_execute: 获得 Slot 后、执行 Graph 前的同步交付决策。
        _raise_dead: 判断已写入死信的交付错误是否交给等待方，None 全部交给等待方。
    """

    def __init__(
        self,
        queue: TaskQueue,
        handler: WorkHandler,
        concurrency: int,
        slots: SlotProvider | None,
        ttl: float,
        interval: float,
        before_claim: Callable[[TaskQueue], float | None] | None = None,
        before_execute: Callable[[TaskLease, Delivery], DeliveryResult | None]
        | None = None,
        raise_dead: Callable[[Work, BaseException], bool] | None = None,
    ) -> None:
        """装配本地执行器，不复制引擎或 Slot 调度逻辑。

        Args:
            queue: 任务存储。
            handler: 完整 Graph 执行处理器。
            concurrency: 本地并发和预取上限。
            slots: 调用方提供的 Slot 池。
            ttl: 租约秒数。
            interval: 轮询间隔秒数。
            before_claim: 可选领取前门控，不执行异步回调。
            before_execute: 可选交付前决策，在本地执行线程运行，续租继续。
            raise_dead: 可选死信错误判定，返回 False 时只保留存储中的死信记录。
        """
        self.queue = queue
        self.local = LocalBackend(max_delivery_attempts=1)
        self.active: dict[str, TaskLease] = {}
        self.lock = RLock()
        self._claim_lock = Lock()
        self._finishing: set[str] = set()
        self._wake = Event()
        self.stop = Event()
        self.errors: deque[BaseException] = deque()
        self._handler = handler
        self._concurrency = concurrency
        self._ttl = ttl
        self._interval = interval
        self._owner = uuid4().hex
        self._closed = False
        self._close_lock = RLock()
        self._lost: set[str] = set()
        self._expires: dict[str, float] = {}
        self._waiting: dict[str, TaskLease] = {}
        self._reserved = 0
        self._retired = False
        self._before_claim = before_claim
        self._before_execute = before_execute
        self._raise_dead = raise_dead
        self._unbind = self.local.bind(
            queue.name, self._handle, concurrency=concurrency, slots=slots
        )
        self.thread = Thread(
            target=self._poll, name=f"bricks-queue-{queue.name}", daemon=True
        )

    def _handle(self, delivery: Delivery) -> DeliveryResult:
        """将交付结果写回存储，再让本地调度器释放 Slot。

        Args:
            delivery: 本地调度器提供的 Slot 和 Work。

        Returns:
            本地调度器的 ACK；业务错误保留给外层 wait_idle。
        """
        with self.lock:
            lease = self.active[delivery.work.id]

        failure: BaseException | None = None
        error_text = ""
        dead = False  # 本次结束决定已成功写入死信。
        try:
            current = Delivery(
                delivery.work, lease.attempt, slot_lease=delivery.slot_lease
            )
            if self._before_execute is None:
                result = None
            elif delivery.slot_lease is None:
                result = self._before_execute(lease, current)
            else:
                with delivery.slot_lease.execution():
                    result = self._before_execute(lease, current)
            if result is None:
                result = self._handler(current)
            if not isinstance(result, DeliveryResult):
                if inspect.iscoroutine(result):
                    result.close()
                raise TypeError("work handler must return DeliveryResult")
        except BaseException as exc:
            failure = exc
            error_text = f"{type(exc).__name__}: {exc}"
            if isinstance(
                exc,
                (
                    asyncio.CancelledError,
                    KeyboardInterrupt,
                    SystemExit,
                    ExecutionControlError,
                ),
            ):
                result = DeliveryResult.retry(exc)
            else:
                result = DeliveryResult.reject(exc)
        with self._claim_lock:
            with self.lock:
                self._finishing.add(lease.token)
        try:
            if lease.token in self._lost:
                failure = failure or LeaseLostError("queue lease was lost")
            elif result.outcome is DeliveryOutcome.ACK:
                self.queue.ack(lease)
            elif result.outcome is DeliveryOutcome.RETRY:
                status = self.queue.retry(
                    lease,
                    error=error_text
                    or (
                        ""
                        if result.error is None
                        else f"{type(result.error).__name__}: {result.error}"
                    ),
                )
                if status == "dead":
                    dead = True
                    failure = (
                        failure
                        or result.error
                        or RuntimeError("maximum delivery attempts exhausted")
                    )
            else:
                self.queue.reject(
                    lease,
                    error=error_text
                    or (
                        ""
                        if result.error is None
                        else f"{type(result.error).__name__}: {result.error}"
                    ),
                )
                dead = True
                failure = result.error
        except BaseException as exc:
            failure = failure or exc
        finally:
            if dead and failure is not None and self._raise_dead is not None:
                # 判定函数自身失败时以其异常替代，原错误经 __context__ 保留。
                try:
                    if not self._raise_dead(delivery.work, failure):
                        failure = None
                except BaseException as exc:
                    failure = exc
            with self.lock:
                if failure is not None:
                    self.errors.append(failure)
                self.active.pop(delivery.work.id, None)
                self._expires.pop(lease.token, None)
                self._lost.discard(lease.token)
                self._finishing.discard(lease.token)
                waiting = self._waiting.pop(delivery.work.id, None)
                self._wake.set()
        if waiting is not None:
            self._resume(waiting)
        return DeliveryResult.ack()

    def _resume(self, lease: TaskLease) -> None:
        """旧交付结束后调度等待中的同 ID 租约，已停止领取时交还存储。

        Args:
            lease: 本地旧交付执行期间重新领取到的租约。
        """
        with self.lock:
            resume = not self.stop.is_set()
            if resume:
                self.active[lease.work.id] = lease
            else:
                self._expires.pop(lease.token, None)
        try:
            if not resume:
                self.queue.retry(lease, error="local queue binding is closing")
                return
            try:
                self.local.submit(self.queue.name, lease.work)
            except BaseException:
                with self.lock:
                    self.active.pop(lease.work.id, None)
                    self._expires.pop(lease.token, None)
                self.queue.retry(lease, error="local scheduling failed")
                raise
        except BaseException as exc:
            with self.lock:
                self.errors.append(exc)
        finally:
            self._wake.set()

    @property
    def accepts_local(self) -> bool:
        """轮询线程仍在续租时接受本地延续，包括停止领取后的排空阶段。

        Returns:
            绑定尚未关闭且轮询线程尚未退出时为 True。
        """
        with self.lock:
            return not (self._closed or self._retired)

    def _holds(self, lease: TaskLease) -> bool:
        """持锁检查租约仍由本绑定持有、未失效且未进入结束提交。

        Args:
            lease: 待检查的执行中或等待中租约。

        Returns:
            仍需续租时为 True。
        """
        current = self.active.get(lease.work.id)
        if current is None or current.token != lease.token:
            current = self._waiting.get(lease.work.id)
        return (
            current is not None
            and current.token == lease.token
            and lease.token not in self._lost
            and lease.token not in self._finishing
        )

    def _lose(self, lease: TaskLease, error: BaseException) -> None:
        """持锁登记失效租约；等待中的租约直接放弃，执行中的租约在结束时报告。

        Args:
            lease: 已失效的租约。
            error: 交给等待方的失效原因。
        """
        waiting = self._waiting.get(lease.work.id)
        if waiting is not None and waiting.token == lease.token:
            del self._waiting[lease.work.id]
            self._expires.pop(lease.token, None)
        else:
            self._lost.add(lease.token)
        self.errors.append(error)

    def submit_local(self, work: Work, slot_lease: SlotLease) -> None:
        """原子持久化并领取 Work，再延续当前 Slot 执行。

        停止领取后的排空阶段仍接受在途交付发起的延续，延续属于排空的一部分。

        Args:
            work: 已包含稳定 ID 与执行配置的目标 Work。
            slot_lease: Router 已为本次交接保留的 Slot 引用。

        Raises:
            RuntimeError: 本地调度器已关闭。
            TypeError: Work 或 lease 不符合 Interlace 契约。
        """
        with self.lock:
            if self._closed or self._retired:
                raise RuntimeError("queue binding is closed")
            self._reserved += 1
        try:
            started = time.monotonic()
            lease = self.queue.put_claim(work, self._owner, lease_seconds=self._ttl)
            if lease is None:
                slot_lease.release()
                return
            with self.lock:
                duplicate = work.id in self.active
                if not duplicate:
                    self.active[work.id] = lease
                    self._expires[lease.token] = started + self._ttl
            if duplicate:
                self.queue.retry(lease, error="local task is already active")
                slot_lease.release()
                return
            try:
                self.local.submit_local(self.queue.name, lease.work, slot_lease)
            except BaseException:
                with self.lock:
                    self.active.pop(work.id, None)
                    self._expires.pop(lease.token, None)
                self.queue.retry(lease, error="local scheduling failed")
                raise
        finally:
            with self.lock:
                self._reserved -= 1
            self._wake.set()

    def _poll(self) -> None:
        """有界预取并续租，按租约丢失与暂时存储故障分别处理。"""
        next_heartbeat = time.monotonic() + self._ttl / 3
        next_claim = 0.0
        claim_failures = 0
        claim_failed_at: float | None = None
        while True:
            self._wake.clear()
            with self.lock:
                held = (*self.active.values(), *self._waiting.values())
                if self.stop.is_set() and not held and not self._reserved:
                    # 与本地延续的登记互斥，退出后 submit_local 明确拒绝。
                    self._retired = True
                    return
            now = time.monotonic()
            if now >= next_heartbeat:
                for lease in held:
                    with self.lock:
                        if not self._holds(lease):
                            continue
                    started = time.monotonic()
                    try:
                        renewed = self.queue.renew(lease, lease_seconds=self._ttl)
                    except LeaseLostError as exc:
                        with self.lock:
                            # 续租往返期间交付可能已经确认；只登记仍持有的租约，避免误报。
                            if self._holds(lease):
                                self._lose(lease, exc)
                    except BaseException as exc:
                        with self.lock:
                            if self._holds(lease) and time.monotonic() >= (
                                self._expires.get(lease.token, 0.0)
                            ):
                                # 存储持续不可用约一个租约时长，与领取失败一致停止绑定并传播。
                                self._lose(lease, exc)
                                self.stop.set()
                    else:
                        with self.lock:
                            if self._holds(lease):
                                if lease.work.id in self._waiting and (
                                    self._waiting[lease.work.id].token == lease.token
                                ):
                                    self._waiting[lease.work.id] = renewed
                                else:
                                    self.active[lease.work.id] = renewed
                                self._expires[lease.token] = started + self._ttl
                next_heartbeat = time.monotonic() + self._ttl / 3
            if (
                not self.stop.is_set()
                and len(held) < self._concurrency
                and time.monotonic() >= next_claim
            ):
                claiming = False
                try:
                    delay = (
                        None
                        if self._before_claim is None
                        else self._before_claim(self.queue)
                    )
                    if delay is not None:
                        if inspect.iscoroutine(delay):
                            delay.close()
                        next_claim = time.monotonic() + seconds(delay, positive=False)
                    else:
                        with self._claim_lock:
                            with self.lock:
                                finishing = bool(self._finishing)
                            claiming = not finishing
                            started = time.monotonic()
                            claimed = (
                                None
                                if finishing
                                else self.queue.claim(
                                    self._owner, lease_seconds=self._ttl
                                )
                            )
                        claiming = False
                        claim_failures = 0
                        claim_failed_at = None
                        if claimed is not None:
                            with self.lock:
                                self._expires[claimed.token] = started + self._ttl
                                duplicate = claimed.work.id in self.active
                                if duplicate:
                                    # 旧租约失效后本绑定重新领到仍在执行的任务：保留并续租新租约，
                                    # 旧交付结束后再调度，不并发执行也不空耗交付次数。
                                    stale = self._waiting.get(claimed.work.id)
                                    if stale is not None:
                                        self._expires.pop(stale.token, None)
                                    self._waiting[claimed.work.id] = claimed
                                else:
                                    self.active[claimed.work.id] = claimed
                            if not duplicate:
                                try:
                                    self.local.submit(self.queue.name, claimed.work)
                                except BaseException:
                                    with self.lock:
                                        self.active.pop(claimed.work.id, None)
                                        self._expires.pop(claimed.token, None)
                                    self.queue.retry(
                                        claimed, error="local scheduling failed"
                                    )
                                    raise
                                self._wake.set()
                                continue
                except BaseException as exc:
                    if not claiming or isinstance(exc, (LeaseLostError, TimeoutError)):
                        with self.lock:
                            self.errors.append(exc)
                        self.stop.set()
                    else:
                        claim_failures += 1
                        if claim_failed_at is None:
                            claim_failed_at = time.monotonic()
                        if time.monotonic() - claim_failed_at >= self._ttl:
                            with self.lock:
                                self.errors.append(exc)
                            self.stop.set()
                        else:
                            next_claim = time.monotonic() + min(
                                self._interval * (2 ** min(claim_failures, 6)),
                                self._ttl / 3,
                            )
            timeout = min(self._interval, self._ttl / 3)
            if time.monotonic() < next_claim:
                timeout = min(timeout, max(0.0, next_claim - time.monotonic()))
            if time.monotonic() < next_heartbeat:
                timeout = min(timeout, max(0.0, next_heartbeat - time.monotonic()))
            self._wake.wait(timeout)

    def close(self) -> None:
        """停止领取并等待所有已领取交付及 Slot 清理，不关闭借用存储。"""
        with self._close_lock:
            self.stop.set()
            self._wake.set()
            if self.thread.ident is not None:
                self.thread.join()
            if self._closed:
                return
            self._closed = True
            try:
                self._unbind()
            finally:
                self.local.close()


class QueueBackend:
    """将显式装配的爬虫队列映射适配为 Interlace TaskBackend。

    持久存储交付创建本地 Slot 链；submit_local 接受 Interlace 已保留的 lease，
    原子持久化并领取目标 Work 后在当前进程延续同一 Slot。

    Attributes:
        _queues: 构造时复制的队列映射，存储与 Redis 客户端归调用方管理。
        _bindings: 当前实例创建的独立消费者绑定。
        _ttl: 交付租约秒数。
        _interval: 非阻塞领取之间的轮询间隔秒数。
        _lock: 保护提交、绑定和关闭入口。
        _closed: 是否已停止接受提交和绑定。
        _close_lock: 保证并发 close 均等待清理完成。
        _errors: 已注销绑定留下的未报告错误，不保留完整消费者对象。
        _local_cursor: 相同通道多个 binding 之间的本地延续轮询位置。
        _before_claim: 可选领域领取前门控。
        _before_execute: 可选已领取交付的前置决策。
        _raise_dead: 可选死信错误判定，返回 False 的错误不交给 wait_idle 与 close。
    """

    def __init__(
        self,
        queues: Mapping[str, TaskQueue],
        *,
        lease_seconds: float = 30,
        poll_interval: float = 0.05,
        before_claim: Callable[[TaskQueue], float | None] | None = None,
        before_execute: Callable[[TaskLease, Delivery], DeliveryResult | None]
        | None = None,
        raise_dead: Callable[[Work, BaseException], bool] | None = None,
    ) -> None:
        """装配明确的命名队列，不创建隐藏的默认队列。

        Args:
            queues: Runtime 通道名称到同名任务存储的映射。
            lease_seconds: 领取租约秒数，默认 30。
            poll_interval: 空轮询间隔秒数，默认 0.05。
            before_claim: 同步函数返回 None 放行，非负秒数推迟下一次领取。
            before_execute: 在本地 Slot 已分配后调用，None 进入 Graph，否则使用明确交付结果。
            raise_dead: 交付错误已写入死信后调用；返回 False 时该错误只保留在存储的死信记录中，
                不交给 wait_idle 与 close。租约丢失和存储错误不经此判定，None 全部传播。

        Raises:
            ValueError: 配置非法或通道名称不匹配。
        """
        self._queues = dict(queues)
        for name, queue in self._queues.items():
            if text(name, "queue name") != queue.name:
                raise ValueError("binding name must equal task queue name")
        self._ttl = seconds(lease_seconds)
        self._interval = seconds(poll_interval)
        self._bindings: list[_Binding] = []
        self._lock = RLock()
        self._closed = False
        self._close_lock = RLock()
        self._errors: deque[BaseException] = deque()
        self._local_cursor = 0
        for callback in (before_claim, before_execute, raise_dead):
            if callback is not None and (
                not callable(callback)
                or inspect.iscoroutinefunction(callback)
                or inspect.isasyncgenfunction(callback)
            ):
                raise TypeError("queue callbacks must be synchronous callables")
        self._before_claim = before_claim
        self._before_execute = before_execute
        self._raise_dead = raise_dead

    def _queue(self, name: str) -> TaskQueue:
        """在持锁入口校验生命周期与队列名。

        Args:
            name: 通道名称。

        Returns:
            借用的任务存储。

        Raises:
            RuntimeError: 后端已关闭。
            ValueError: 队列未装配。
        """
        if self._closed:
            raise RuntimeError("queue backend is closed")
        if name not in self._queues:
            raise ValueError(f"queue is not configured: {name!r}")
        return self._queues[name]

    def submit(self, queue: str, work: Work) -> None:
        """按 Work.id 原子去重并提交，正常返回表示存储已经接受。

        Args:
            queue: 已装配通道名称。
            work: JSON 可序列化任务。
        """
        with self._lock:
            store = self._queue(queue)
        store.put(work)

    def submit_local(self, queue: str, work: Work, lease: SlotLease) -> None:
        """持久化目标 Work 并在当前进程延续同一 Slot 链。

        优先选择仍在领取的绑定；注销或关闭排空期间，在途交付的延续交给仍在续租的绑定。

        Args:
            queue: 已绑定的本地消费通道。
            work: Interlace Router 创建的工作请求；本地提交信封可携带完整目标 Work。
            lease: Router 已 retain 并转交给本次投递的 Slot 引用。

        Raises:
            RuntimeError: 通道没有仍可执行的本地消费者。
            ValueError: 通道未装配。
            TypeError: Work 或 lease 不符合 Interlace 契约。
        """
        with self._lock:
            if queue not in self._queues:
                raise ValueError(f"queue is not configured: {queue!r}")
            accepting = [
                binding
                for binding in self._bindings
                if binding.queue.name == queue and binding.accepts_local
            ]
            bindings = [
                binding for binding in accepting if not binding.stop.is_set()
            ] or accepting
            if not bindings:
                raise RuntimeError(f"task queue {queue!r} has no local consumers")
            binding = bindings[self._local_cursor % len(bindings)]
            self._local_cursor += 1
            target = (
                work.inputs.work if isinstance(work.inputs, LocalSubmission) else work
            )
        binding.submit_local(target, lease)

    def bind(
        self,
        queue: str,
        handler: WorkHandler,
        *,
        concurrency: int,
        slots: SlotProvider | None = None,
    ) -> Callable[[], None]:
        """注册独立竞争消费者并返回幂等注销句柄。

        Args:
            queue: 已装配通道。
            handler: 完整执行处理器。
            concurrency: 本地执行及预取上限。
            slots: 本地 Slot 资源池，生命周期由调用方管理。

        Returns:
            停止领取并排空此绑定的注销函数。

        Raises:
            TypeError: 处理器、Slot 或并发类型非法。
            ValueError: 并发小于一或通道未装配。
        """
        with self._lock:
            store = self._queue(queue)
            if not callable(handler) or any(
                check(candidate)
                # 可调用实例的异步或生成器 __call__ 需单独检查。
                for candidate in (handler, getattr(handler, "__call__", None))  # noqa: B004
                for check in (
                    inspect.iscoroutinefunction,
                    inspect.isasyncgenfunction,
                    inspect.isgeneratorfunction,
                )
            ):
                raise TypeError(
                    "work handler must be a synchronous callable returning DeliveryResult"
                )
            binding = _Binding(
                store,
                handler,
                concurrency,
                slots,
                self._ttl,
                self._interval,
                self._before_claim,
                self._before_execute,
                self._raise_dead,
            )
            try:
                binding.thread.start()
            except BaseException:
                binding.close()
                raise
            self._bindings.append(binding)
        return partial(self._unbind, binding)

    def _unbind(self, binding: _Binding) -> None:
        """注销后释放后端持有的绑定引用，并保留尚未报告的错误。

        Args:
            binding: 此注销句柄对应的独立绑定。
        """
        try:
            binding.close()
        finally:
            with self._lock, binding.lock:
                if binding in self._bindings:
                    self._bindings.remove(binding)
                    self._errors.extend(binding.errors)
                    binding.errors.clear()

    @property
    def idle(self) -> bool:
        """检查配置队列均无未完成任务且本地 Slot 已释放。

        Returns:
            当前快照是否空闲；不表示外部生产者不会再提交。
        """
        with self._lock:
            bindings = tuple(self._bindings)
            closed = self._closed
        if closed:
            # 已停止领取的实例只负责本地清理，存储中剩余任务留给其他消费者。
            return all(
                binding.local.idle and not binding.thread.is_alive()
                for binding in bindings
            )
        for queue in self._queues.values():
            counts = queue.counts()
            if counts["pending"] or counts["leased"]:
                return False
        return all(binding.local.idle for binding in bindings)

    def _raise_failure(self) -> None:
        """向调用方传播尚未报告的消费或存储错误。

        Raises:
            BaseException: 原始处理器异常或后台存储错误。
        """
        failures: list[BaseException] = []
        with self._lock:
            failures.extend(self._errors)
            self._errors.clear()
            bindings = tuple(self._bindings)
        for binding in bindings:
            with binding.lock:
                failures.extend(binding.errors)
                binding.errors.clear()
        unique: list[BaseException] = []
        seen: set[int] = set()
        for failure in failures:
            if id(failure) not in seen:
                seen.add(id(failure))
                unique.append(failure)
        if unique:
            for earlier, later in pairwise(unique):
                # 原异常已有 cause 时保留真实根因，不以后续错误覆盖。
                if earlier.__cause__ is None:
                    earlier.__cause__ = later
            raise unique[0]

    def wait_idle(self, timeout: float | None = None) -> None:
        """运行时等待共享队列排空；关闭后只等待本地交付和线程清理。

        Args:
            timeout: 最大等待秒数；None 持续等待，零表示立即检查。

        Raises:
            TimeoutError: 未在指定时间内排空。
            BaseException: 处理器或存储错误，保留原异常。
        """
        deadline = (
            None
            if timeout is None
            else time.monotonic() + seconds(timeout, positive=False)
        )
        while True:
            self._raise_failure()
            if self.idle:
                self._raise_failure()
                return
            if deadline is not None and time.monotonic() >= deadline:
                raise TimeoutError("task queues did not become idle")
            time.sleep(
                self._interval
                if deadline is None
                else min(self._interval, max(0, deadline - time.monotonic()))
            )

    def close(self) -> None:
        """关闭本地绑定并保留未领取任务；借用存储和客户端继续可用。

        Raises:
            BaseException: 排空过程中尚未报告的失败。
        """
        with self._close_lock:
            with self._lock:
                if self._closed:
                    return
                self._closed = True
                bindings = tuple(self._bindings)
                for binding in bindings:
                    binding.stop.set()
                    binding._wake.set()
            failures: list[BaseException] = []
            for binding in bindings:
                try:
                    self._unbind(binding)
                except BaseException as exc:
                    failures.append(exc)
            with self._lock:
                self._errors.extend(failures)
            self._raise_failure()
