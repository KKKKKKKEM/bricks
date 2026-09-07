"""在同步 Engine 边界内执行同步和异步调用。"""

from __future__ import annotations

import asyncio
import concurrent.futures
import inspect
from collections.abc import Callable
from threading import Event as ThreadEvent
from threading import RLock, Thread, current_thread
from typing import Any


class LocalRunner:
    """同步调用普通函数，并在常驻后台 event loop 上解析 awaitable。

    Attributes:
        _loop: 用于运行异步调用的后台事件循环。
        _ready: 通知后台事件循环已经就绪的同步事件。
        _lock: 保护当前组件共享状态的进程内互斥锁。
        _closed: 当前组件是否已停止接受新工作。
        _thread: 承载后台事件循环的专用线程。
    """

    def __init__(self) -> None:
        """创建后台事件循环线程并等待其准备完成。"""

        self._loop = asyncio.new_event_loop()
        self._ready = ThreadEvent()
        self._lock = RLock()
        self._closed = False
        self._thread = Thread(
            target=self._run_loop,
            name="bricks-async-runner",
            daemon=True,
        )
        self._thread.start()
        self._ready.wait()

    def resolve(
        self,
        value: Any,
        *,
        checkpoint: Callable[[], None] | None = None,
        wait_timeout: Callable[[], float | None] | None = None,
    ) -> Any:
        """原样返回同步值，或同步等待后台 loop 上的 awaitable。

        Args:
            value: 同步结果或需要在后台循环等待的 awaitable。
            checkpoint: 协作式取消和超时检查函数。
            wait_timeout: 计算当前剩余等待秒数的函数。

        Returns:
            同步调用或异步等待完成后的处理结果。

        Raises:
            RuntimeError: 当前生命周期状态或操作顺序不允许此操作。
        """

        if not inspect.isawaitable(value):
            return value
        with self._lock:
            if self._closed:
                if inspect.iscoroutine(value):
                    value.close()
                raise RuntimeError("runner is closed")
            if current_thread() is self._thread:
                if inspect.iscoroutine(value):
                    value.close()
                raise RuntimeError(
                    "cannot synchronously resolve an awaitable on the runner thread"
                )
            future: concurrent.futures.Future[Any] = concurrent.futures.Future()
            scheduled: concurrent.futures.Future[asyncio.Future[Any]] = (
                concurrent.futures.Future()
            )

            def completed(task: asyncio.Future[Any]) -> None:
                """将后台异步任务的结果或异常转交给同步等待方。

                Args:
                    task: 已经调度的异步任务。
                """

                try:
                    future.set_result(task.result())
                except BaseException as exc:
                    future.set_exception(exc)

            def schedule() -> None:
                """将待等待对象提交到后台事件循环。"""

                try:
                    task = asyncio.ensure_future(value, loop=self._loop)
                    task.add_done_callback(completed)
                    scheduled.set_result(task)
                except BaseException as exc:
                    scheduled.set_exception(exc)
                    future.set_exception(exc)

            self._loop.call_soon_threadsafe(schedule)
        if checkpoint is None:
            return future.result()
        try:
            while True:
                checkpoint()
                remaining = None if wait_timeout is None else wait_timeout()
                poll = 0.05 if remaining is None else min(0.05, remaining)
                try:
                    return future.result(timeout=poll)
                except concurrent.futures.TimeoutError:
                    if future.done():
                        return future.result()
                    checkpoint()
        except BaseException:
            # 代理 Future 可能在协程清理前就标记为已取消。
            # 必须等待实际任务退出，才能释放该执行持有的资源。
            if not future.done():
                task = scheduled.result()
                self._loop.call_soon_threadsafe(task.cancel)
                try:
                    future.result()
                except BaseException:
                    pass
            raise

    def close(self) -> None:
        """停止后台 loop，并等待专用线程退出。"""

        with self._lock:
            if self._closed:
                return
            self._closed = True
            self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join()

    def _run_loop(self) -> None:
        """运行后台事件循环，退出时取消残留任务并关闭循环。"""

        asyncio.set_event_loop(self._loop)
        self._ready.set()
        try:
            self._loop.run_forever()
        finally:
            pending = asyncio.all_tasks(self._loop)
            for task in pending:
                task.cancel()
            if pending:
                self._loop.run_until_complete(
                    asyncio.gather(*pending, return_exceptions=True)
                )
            self._loop.close()
