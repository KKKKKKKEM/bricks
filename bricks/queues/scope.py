"""通过队列实例共享的续租线程管理 with 内的任务占用。"""

from __future__ import annotations

import heapq
import time
from collections.abc import Callable
from functools import partial
from threading import Condition, RLock, Thread
from types import TracebackType
from typing import TYPE_CHECKING, Any, Literal

from interlace.spi import Work

from .models import LeaseLostError, TaskLease, seconds, text

if TYPE_CHECKING:
    from .base import TaskQueue


class _Renewals:
    """每个队列实例一个惰性续租调度器，不按任务创建线程。

    Attributes:
        _condition: 保护注册项及到期堆，并唤醒等待线程。
        _lifecycle: 串行化注册和最后一次注销，防止退出等待后来加入的任务。
        _scopes: token 对应的占用上下文，注销后释放引用。
        _due: 下一次续租的单调时间和 token 的最小堆。
        _thread: 当前共享线程；最后一个占用退出时等待停止并清空。
    """

    def __init__(self) -> None:
        """创建空调度器，首个占用进入时才启动线程。"""
        self._condition = Condition(RLock())
        self._lifecycle = RLock()
        self._scopes: dict[str, TaskScope] = {}
        self._due: list[tuple[float, str]] = []
        self._thread: Thread | None = None

    def add(self, token: str, scope: TaskScope) -> None:
        """登记租约并在必要时启动唯一的续租线程。

        Args:
            token: 当前租约 token，禁止重复登记。
            scope: 由队列创建的上下文。

        Raises:
            RuntimeError: 相同租约已有上下文管理。
            BaseException: 线程启动失败，登记会回滚。
        """
        with self._lifecycle, self._condition:
            if token in self._scopes:
                raise RuntimeError("lease already has an active task scope")
            self._scopes[token] = scope
            heapq.heappush(self._due, (time.monotonic() + scope._ttl / 3, token))
            if self._thread is None:
                self._thread = Thread(
                    target=self._run, name="bricks-queue-renewals", daemon=True
                )
                try:
                    self._thread.start()
                except BaseException:
                    self._scopes.pop(token)
                    self._due.clear()
                    self._thread = None
                    raise
            self._condition.notify_all()

    def remove(self, token: str) -> None:
        """注销当前占用，最后一个注销等待线程退出。

        Args:
            token: 要注销的租约身份；重复注销无副作用。
        """
        with self._lifecycle:
            with self._condition:
                self._scopes.pop(token, None)
                thread = self._thread if not self._scopes else None
                if len(self._due) > 2 * len(self._scopes) + 64:
                    self._due = [item for item in self._due if item[1] in self._scopes]
                    heapq.heapify(self._due)
                self._condition.notify_all()
            if thread is not None:
                thread.join()
                with self._condition:
                    self._thread = None
                    self._due.clear()

    def _run(self) -> None:
        """按最早到期项续租，单项失败不会中断其他占用的续租。"""
        while True:
            with self._condition:
                if not self._scopes:
                    return
                while self._due and self._due[0][1] not in self._scopes:
                    heapq.heappop(self._due)
                if not self._due:
                    self._condition.wait()
                    continue
                due, token = self._due[0]
                delay = due - time.monotonic()
                if delay > 0:
                    self._condition.wait(delay)
                    continue
                heapq.heappop(self._due)
                scope = self._scopes[token]
            renewed = scope._heartbeat()
            with self._condition:
                if renewed and self._scopes.get(token) is scope:
                    heapq.heappush(
                        self._due, (time.monotonic() + scope._ttl / 3, token)
                    )


class TaskScope:
    """单次使用的任务占用上下文，由 queue.reserve 或 queue.hold 创建。

    显式决定只在正常退出时生效；未确认或异常退出请求有界重投，原异常继续传播。
    续租失败通过 check 或退出传播，不强制中断业务函数。

    Attributes:
        _queue: 借用的任务存储，不负责关闭。
        _renewals: 所属队列实例共享的续租调度器。
        _owner: 进入时领取任务的身份，接管已有凭据时为 None。
        _lease: 当前凭据，尚未领取或空队列时为 None。
        _work: 本次占用的固定 Work 对象，后台更新凭据不替换业务对象。
        _ttl: 领取和续租的有效秒数，间隔为其三分之一。
        _lock: 保护当前凭据、状态、决定及后台错误。
        _exit_lock: 串行化退出操作。
        _state: new、entering、entered、closing 或 closed，禁止复用。
        _failure: 第一次后台续租失败的原异常；暂时存储错误在本地租约到期前不登记。
        _decision: 在正常退出时对最新凭据执行的操作，None 表示默认请求重投。
        _expires: 当前凭据的本地到期时刻，单位为 time.monotonic 秒；在发出领取或续租前取时，
            不与存储侧 deadline 比较。
    """

    def __init__(
        self,
        queue: TaskQueue,
        _renewals: _Renewals,
        *,
        owner: str | None = None,
        lease: TaskLease | None = None,
        lease_seconds: float = 30,
    ) -> None:
        """保存队列提供的共享调度器及占用配置，不立即领取任务。

        Args:
            queue: 当前任务存储。
            _renewals: 队列内部拥有的共享调度器。
            owner: 进入时领取任务的身份，与 lease 二选一。
            lease: 已领取凭据，与 owner 二选一。
            lease_seconds: 有效秒数，默认 30。

        Raises:
            ValueError: 身份、时长或二选一参数非法。
            TypeError: 凭据不是 TaskLease。
            LeaseLostError: 凭据属于其他队列。
        """
        if (owner is None) == (lease is None):
            raise ValueError("exactly one of owner or lease is required")
        if lease is not None:
            if not isinstance(lease, TaskLease):
                raise TypeError("lease must be TaskLease")
            if lease.queue != queue.name:
                raise LeaseLostError("lease belongs to another queue")
        self._queue = queue
        self._renewals = _renewals
        self._owner = None if owner is None else text(owner, "owner")
        self._lease = lease
        self._work: Work | None = None
        self._ttl = seconds(lease_seconds)
        self._lock = RLock()
        self._exit_lock = RLock()
        self._state = "new"
        self._failure: BaseException | None = None
        self._decision: Callable[[TaskLease], Any] | None = None
        self._expires = 0.0

    def __enter__(self) -> TaskScope | None:
        """领取或续租已有凭据，然后加入队列的共享续租调度。

        Returns:
            当前占用上下文；没有可领取任务时返回 None。

        Raises:
            RuntimeError: 上下文已使用，或已有凭据已由其他上下文管理。
            BaseException: 存储或调度器启动失败；新领取任务会尝试释放。
        """
        with self._lock:
            if self._state != "new":
                raise RuntimeError("task scope can only be entered once")
            self._state = "entering"
        registered = False
        try:
            started = time.monotonic()
            if self._owner is not None:
                self._lease = self._queue.claim(self._owner, lease_seconds=self._ttl)
            if self._lease is not None:
                self._work = self._lease.work
                self._renewals.add(self._lease.token, self)
                registered = True
                if self._owner is None:
                    started = time.monotonic()
                    self._lease = self._queue.renew(
                        self._lease, lease_seconds=self._ttl
                    )
                self._expires = started + self._ttl
            with self._lock:
                self._state = "entered"
            return None if self._lease is None else self
        except BaseException as exc:
            with self._lock:
                self._state = "closed"
            cleanup: BaseException | None = None
            try:
                if self._lease is not None:
                    if registered or self._owner is not None:
                        self._queue.retry(self._lease, error="task scope entry failed")
            except BaseException as error:
                cleanup = error
            finally:
                if registered:
                    assert self._lease is not None
                    self._renewals.remove(self._lease.token)
            if cleanup is not None and cleanup is not exc and exc.__cause__ is None:
                raise exc from cleanup
            raise

    def _heartbeat(self) -> bool:
        """续租当前占用并保留失败，不向共享线程抛出业务存储异常。

        租约丢失立即登记；其他存储错误在本地租约到期前于后续心跳重试。

        Returns:
            是否继续为此上下文安排心跳。
        """
        with self._lock:
            if self._state == "entering":
                return True
            if self._state != "entered" or self._failure is not None:
                return False
            assert self._lease is not None
            lease = self._lease
        started = time.monotonic()
        try:
            renewed = self._queue.renew(lease, lease_seconds=self._ttl)
        except BaseException as exc:
            with self._lock:
                current = (
                    self._state == "entered"
                    and self._lease is not None
                    and self._lease.token == lease.token
                )
                if not current:
                    return False
                if not isinstance(exc, LeaseLostError) and (
                    time.monotonic() < self._expires
                ):
                    return True
                self._failure = exc
            return False
        with self._lock:
            if (
                self._state == "entered"
                and self._lease is not None
                and self._lease.token == lease.token
            ):
                self._lease = renewed
                self._expires = started + self._ttl
                return True
            return False

    def check(self) -> None:
        """检查仍持有任务并传播已观察到的续租失败。

        Raises:
            RuntimeError: 上下文未进入、已退出或没有任务。
            BaseException: 后台续租失败；该检查不发起新的服务端续租。
        """
        with self._lock:
            if self._state != "entered" or self._lease is None:
                raise RuntimeError("task scope does not hold an active task")
            if self._failure is not None:
                raise self._failure

    @property
    def work(self) -> Work:
        """读取当前占用的独立 Work。

        Returns:
            业务可以读取的 Work，修改不回写队列。

        Raises:
            RuntimeError: 没有活跃占用。
            BaseException: 已观察到的续租失败。
        """
        with self._lock:
            self.check()
            assert self._work is not None
            return self._work

    def _decide(self, action: Callable[[TaskLease], Any]) -> None:
        """登记唯一退出决定，块内保持占用。

        Args:
            action: 退出时对最新凭据执行的操作。

        Raises:
            RuntimeError: 已有决定或当前没有活跃占用。
            BaseException: 已观察到的续租失败。
        """
        with self._lock:
            self.check()
            if self._decision is not None:
                raise RuntimeError("task scope already has an exit decision")
            self._decision = action

    def ack(self, *, result: str | None = None) -> None:
        """登记正常退出时确认交付，块内后续异常会取消确认。

        Args:
            result: 显式 success/failure 业务结果，默认 None。

        Raises:
            ValueError: 业务结果非法。
            RuntimeError: 上下文无效或重复决定。
        """
        if result not in (None, "success", "failure"):
            raise ValueError("result must be success, failure or None")
        self._decide(partial(self._queue.ack, result=result))

    def retry(self, *, delay: float = 0, error: str = "") -> None:
        """登记正常退出时的重投延迟和原因。

        Args:
            delay: 再次可领取之前的秒数。
            error: 重投原因。

        Raises:
            TypeError: 原因不是字符串。
            ValueError: 延迟非法。
            RuntimeError: 上下文无效或重复决定。
        """
        if not isinstance(error, str):
            raise TypeError("error must be a string")
        self._decide(
            partial(
                self._queue.retry, delay=seconds(delay, positive=False), error=error
            )
        )

    def reject(self, *, error: str = "") -> None:
        """登记正常退出时结束交付并保留死信。

        Args:
            error: 拒绝原因。

        Raises:
            TypeError: 原因不是字符串。
            RuntimeError: 上下文无效或重复决定。
        """
        if not isinstance(error, str):
            raise TypeError("error must be a string")
        self._decide(partial(self._queue.reject, error=error))

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> Literal[False]:
        """停止此占用的续租，按退出结果结束交付，并保留原业务异常。

        Args:
            exc_type: 块内异常类型。
            exc: 块内原始异常。
            traceback: 原始异常栈。

        Returns:
            False，不吞掉块内异常。

        Raises:
            RuntimeError: 上下文未进入。
            BaseException: 续租或写回失败；块内异常优先，清理错误通过 cause 保留。
        """
        del exc_type
        with self._exit_lock:
            with self._lock:
                if self._state == "closed":
                    return False
                if self._state != "entered":
                    raise RuntimeError("task scope has not been entered")
                self._state = "closing"
            cleanup: BaseException | None = None
            failure = self._failure
            try:
                if self._lease is not None:
                    if exc is None and failure is None and self._decision is not None:
                        self._decision(self._lease)
                    else:
                        self._queue.retry(
                            self._lease,
                            error=(
                                "task scope released without acknowledgement"
                                if exc is None and failure is None
                                else str(exc if exc is not None else failure)
                            ),
                        )
            except BaseException as error:
                cleanup = error
            finally:
                with self._lock:
                    self._state = "closed"
                # 写回结束前保留登记，防止其他上下文在确认窗口重新接管同一 token。
                if self._lease is not None:
                    self._renewals.remove(self._lease.token)
            if exc is not None:
                secondary = (
                    failure if failure is not None and failure is not exc else cleanup
                )
                if (
                    secondary is not None
                    and secondary is not exc
                    and exc.__cause__ is None
                ):
                    raise exc.with_traceback(traceback) from secondary
                return False
            if failure is not None:
                if (
                    cleanup is not None
                    and cleanup is not failure
                    and failure.__cause__ is None
                ):
                    raise failure from cleanup
                raise failure
            if cleanup is not None:
                raise cleanup
            return False
