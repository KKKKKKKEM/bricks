# -*- coding: utf-8 -*-
# @Time    : 2023-11-13 17:46
# @Author  : Kem
# @Desc    : task distribution
__all__ = ("Dispatcher", "Task")

import asyncio
import ctypes
import queue
import sys
import threading
import time
from asyncio import ensure_future, futures
from concurrent.futures import Future
from typing import Any, Callable, Dict, List, Optional, Union

from bricks.core import context, events, signals
from bricks.state import const

# 常量定义
_NO_TIMEOUT = -1  # 表示不使用超时限制


class Exit(signals.Signal):
    def __init__(self, target: "Worker"):
        self.target = target


class _TaskQueue(queue.Queue):
    def get(self, block=True, timeout=None, worker: Optional["Worker"] = None) -> "Task":
        """
        获取任务，优先获取属于指定worker的任务，其次获取通用任务

        :param block: 是否阻塞等待
        :param timeout: 超时时间
        :param worker: 指定的worker，如果为None则获取任何任务
        :return: 匹配的任务
        """
        with self.not_empty:
            self._wait_for_task(block, timeout)

            item = self._find_and_remove_matching_task(worker)
            if item is None:
                raise queue.Empty

            self.not_full.notify()
            return item

    def _wait_for_task(self, block: bool, timeout: Optional[float]) -> None:
        """等待队列中有任务可用"""
        if not block:
            if not self._qsize():
                raise queue.Empty
        elif timeout is None:
            while not self._qsize():
                self.not_empty.wait()
        elif timeout < 0:
            raise ValueError("'timeout' must be a non-negative number")
        else:
            endtime = time.time() + timeout
            while not self._qsize():
                remaining = endtime - time.time()
                if remaining <= 0.0:
                    raise queue.Empty
                self.not_empty.wait(remaining)

    def _find_and_remove_matching_task(self, worker: Optional["Worker"]) -> Optional["Task"]:
        """
        在队列中查找并移除匹配的任务
        优先级：1. 专属任务(task.worker == worker) 2. 通用任务(task.worker is None)

        :param worker: 目标worker
        :return: 匹配的任务或None
        """
        if not self.queue:
            return None

        # 使用生成器表达式优雅地查找任务
        # 优先查找专属任务，然后查找通用任务
        task = (
            next((t for t in self.queue if t.worker == worker),
                 None) if worker else None
        ) or next((t for t in self.queue if t.worker is None), None)

        # 如果都没找到且worker为None，则取第一个任务
        if task is None and worker is None:
            return self.queue.popleft() if self.queue else None

        if task:
            self.queue.remove(task)

        return task


class Task(Future):
    """
    A future that is used to store task information

    """

    def __init__(
            self,
            func: Callable,
            args: Optional[Union[List, tuple]] = None,
            kwargs: Optional[Dict] = None,
            callback: Optional[Callable] = None,
            worker: Optional["Worker"] = None
    ) -> None:
        self.func = func
        self.args = args or []
        self.kwargs = kwargs or {}
        self.callback = callback
        self.dispatcher: Optional["Dispatcher"] = None
        self.worker: Optional["Worker"] = worker
        self.future: Optional["asyncio.Future"] = None
        if callback:
            self.add_done_callback(callback)
        super().__init__()

    @property
    def is_async(self) -> bool:
        return asyncio.iscoroutinefunction(self.func)

    def cancel(self) -> bool:
        if self.dispatcher:
            self.dispatcher.cancel_task(self)
        if self.future:
            self.future.cancel()
        return super().cancel()


class Worker(threading.Thread):
    """
    worker can execute task with threading

    """

    def __init__(
            self,
            dispatcher: "Dispatcher",
            name: str,
            timeout: Optional[float] = None,
            daemon: bool = True,
            trace: bool = False,
            **kwargs: Any,
    ) -> None:
        self.dispatcher = dispatcher
        self._shutdown = threading.Event()
        self.timeout = timeout
        self.trace = trace
        self._awaken = threading.Event()
        self._awaken.set()
        super().__init__(daemon=daemon, name=name, **kwargs)

    def run(self) -> None:
        events.EventManager.invoke(
            context.Context(const.BEFORE_WORKER_START, target=...), errors="output"
        )
        if self.trace:
            sys.settrace(self._trace)

        while self.dispatcher.is_running() and not self._shutdown.is_set():
            if not self.trace:
                self._awaken.wait()

            try:
                task: Task = self.dispatcher.doing.get(
                    timeout=self.timeout, worker=self
                )
                task.worker = self

            except queue.Empty:
                self.clean()
                self.dispatcher.stop_worker(self.name)
                return

            try:
                ret = task.func(*task.args, **task.kwargs)
                task.set_result(ret)

            except Exit:
                self.clean()
                return

            except (KeyboardInterrupt, SystemExit):
                raise

            except Exception as e:
                task.set_exception(e)
                events.EventManager.invoke(context.Error(error=e))

            finally:
                self.dispatcher.doing.task_done()

    def stop(self) -> None:
        if not self.is_alive() or self._shutdown.is_set():
            return

        def main(obj):
            raise Exit(obj)

        self.dispatcher.doing.put(Task(func=main, args=[self], worker=self))

    def shutdown(self):
        if not self.is_alive():
            return
        exc = ctypes.py_object(SystemExit)
        tid = ctypes.c_long(self.ident) # type: ignore

        res = ctypes.pythonapi.PyThreadState_SetAsyncExc(tid, exc)
        if res == 0:
            raise ValueError("nonexistent thread id")
        elif res > 1:
            ctypes.pythonapi.PyThreadState_SetAsyncExc(tid, None)
            raise SystemError("PyThreadState_SetAsyncExc failed")

    def clean(self):
        events.EventManager.invoke(
            context.Context(const.BEFORE_WORKER_CLOSE, target=...), errors="output"
        )
        self._shutdown.set()

    def wait(self, timeout=None):
        self._shutdown.wait(timeout=timeout)

    def is_shutdown(self):
        return self._shutdown.is_set()

    def pause(self) -> None:
        self._awaken.clear()

    def awake(self) -> None:
        self._awaken.set()

    def _trace(self, frame, event, arg):  # noqa
        if event == "call":
            return self._localtrace
        else:
            return None

    def _localtrace(self, frame, event, arg):  # noqa
        self._awaken.wait()
        if self._shutdown.is_set() and event == "line":
            raise SystemExit()
        return self._localtrace


class Dispatcher:
    """
    Dispatcher: Responsible for task scheduling, assignment and execution

    Outstanding features of this dispatcher:

    1. Supports setting a maximum number of concurrent tasks, and can automatically add or close workers based on the number of currently submitted tasks.
    2. Workers support manual pause and shutdown.
    3. The submitted tasks, whether asynchronous or synchronous, are all efficiently managed.
    4. After submission, a future is returned. To wait for the result, you simply need to call `future.result()`. You can also cancel the task, which simplifies asynchronous programming.


    """
    loop: asyncio.AbstractEventLoop
    doing: _TaskQueue  # _TaskQueue 在后面定义或导入
    workers: dict[str, Worker]
    _remain_workers: queue.Queue
    _running_tasks: threading.Semaphore
    _active_tasks: threading.Semaphore
    _shutdown: Optional[asyncio.Event]
    _running: threading.Event
    thread: threading.Thread
    _lock: threading.Lock

    def __init__(self, max_workers: int = 1, options: Optional[Dict] = None) -> None:
        self.max_workers = max_workers
        self.options = options or {}
        self._set_env()

    def _set_env(self):
        self.loop = asyncio.new_event_loop()
        self.doing = _TaskQueue()
        self.workers: Dict[str, Worker] = {}
        self._remain_workers = queue.Queue()
        for i in range(self.max_workers):
            self._remain_workers.put(f"Worker-{i}")

        self._running_tasks = threading.Semaphore(self.max_workers)
        self._active_tasks = threading.Semaphore(self.max_workers)
        # Created inside the dispatcher thread after setting the event loop.
        # This avoids "There is no current event loop in thread ..." when a Dispatcher
        # is instantiated from non-main threads.
        self._shutdown = None
        self._running = threading.Event()
        self.thread = threading.Thread(
            target=self.run, name="DisPatch", daemon=True)
        self._lock = threading.Lock()

    def create_worker(self, size: int = 1):
        """
        create workers

        :param size:
        :return:
        """
        options = self.options or {}

        for _ in range(size):
            ident = self._remain_workers.get()
            options.setdefault("trace", False)
            worker = Worker(self, name=ident, **options)
            self.workers[worker.name] = worker
            worker.start()

    def stop_worker(self, *idents: str) -> None:
        """
        stop workers

        :param idents: worker identifiers to stop
        :return: None
        """
        waiters = []
        for ident in idents:
            worker = self.workers.pop(ident, None)
            if worker:
                self._remain_workers.put(worker.name)
                worker.stop()
                waiters.append(worker)

        for waiter in waiters:
            waiter.wait()

    def pause_worker(self, *idents: str) -> None:
        """
        pause workers

        :param idents: worker identifiers to pause
        :return: None
        """
        self._control_workers(idents, 'pause')

    def awake_worker(self, *idents: str) -> None:
        """
        awake workers

        :param idents: worker identifiers to awake
        :return: None
        """
        self._control_workers(idents, 'awake')

    def _control_workers(self, idents: tuple, action: str) -> None:
        """内部方法：控制worker状态"""
        for ident in idents:
            worker = self.workers.get(ident)
            if worker:
                getattr(worker, action)()

    def submit_task(self, task: Task, timeout: Optional[int] = None) -> Task:
        """
        submit a task to the workers pool to run

        :param task: task to submit
        :param timeout: timeout in seconds, -1 means no limit
        :return: the submitted task
        """
        assert self.is_running(), "dispatcher is not running"

        def submit():
            task.dispatcher = self
            if task.is_async:
                self.active_task(task)
            else:
                self.doing.put(task)

        if timeout != _NO_TIMEOUT:
            self._running_tasks.acquire(timeout=timeout)
            task.add_done_callback(lambda x: self._running_tasks.release())

        submit()

        self.adjust_workers()
        return task

    def active_task(self, task: Task, timeout: Optional[int] = None) -> Task:
        """
        activate a task to start running

        :param task: task to activate
        :param timeout: timeout in seconds, -1 means no limit
        :return: the activated task
        """
        def _handle_exception(exc: BaseException) -> None:
            """统一的异常处理逻辑"""
            if task.set_running_or_notify_cancel():
                task.set_exception(exc)
            raise

        def run_async_task():
            def callback():
                try:
                    futures._chain_future(  # noqa
                        ensure_future(
                            task.func(*task.args, **task.kwargs), loop=self.loop
                        ),
                        task,
                    )
                except (SystemExit, KeyboardInterrupt):
                    raise
                except BaseException as exc:
                    _handle_exception(exc)

            self.loop.call_soon_threadsafe(callback)

        def run_sync_task():
            def callback():
                try:
                    ret = task.func(*task.args, **task.kwargs)
                    task.set_result(ret)
                except (SystemExit, KeyboardInterrupt):
                    raise
                except BaseException as exc:
                    _handle_exception(exc)

            threading.Thread(target=callback, daemon=True).start()

        def active():
            if task.is_async:
                run_async_task()
            else:
                run_sync_task()

        assert self.is_running(), "dispatcher is not running"

        if timeout != _NO_TIMEOUT:
            self._active_tasks.acquire(timeout=timeout)
            task.add_done_callback(lambda x: self._active_tasks.release())

        active()
        return task

    def adjust_workers(self) -> None:
        """根据任务数量动态调整worker数量"""
        remain_workers = self._remain_workers.qsize()
        remain_tasks = self.doing.qsize()
        if remain_workers > 0 and remain_tasks > 0:
            self.create_worker(min(remain_workers, remain_tasks))

    def cancel_task(self, task: Task) -> None:
        """
        cancel task

        :param task: task to cancel
        :return: None
        """
        with self.doing.not_empty:
            # If the task is still in the task queue and has not been retrieved for use, it will be deleted from the task queue
            if task in self.doing.queue:
                self.doing.queue.remove(task)
                self.doing.task_done()
                self.doing.not_full.notify()
            else:
                # If there is a worker and the task is running, shut down the worker
                if task.worker and not task.done():
                    self.stop_worker(task.worker.name)

    @staticmethod
    def make_task(task: Union[dict, Task]) -> Task:
        """
        package a task

        :param task:
        :return:
        :rtype:
        """
        if isinstance(task, Task):
            return task

        func = task.get("func")

        # get positional parameters
        positional = task.get("args", [])
        positional = (
            [positional] if not isinstance(
                positional, (list, tuple)) else positional
        )

        # get keyword parameters
        keyword = task.get("kwargs", {})
        keyword = {} if not isinstance(keyword, dict) else keyword

        # get callback function
        callback = task.get("callback")

        return Task(
            func=func, # type: ignore
            args=positional,
            kwargs=keyword,
            callback=callback,
        )

    def is_running(self) -> bool:
        """检查dispatcher是否正在运行"""
        return (
            self._running.is_set()
            and self._shutdown is not None
            and not self._shutdown.is_set()
        )

    @property
    def running(self) -> int:
        """返回未完成的任务数量"""
        return self.doing.unfinished_tasks

    def run(self) -> None:
        """运行dispatcher的主事件循环"""
        asyncio.set_event_loop(self.loop)
        self._shutdown = asyncio.Event()
        self._running.set()
        try:
            self.loop.run_until_complete(self._shutdown.wait())
        finally:
            try:
                self.loop.stop()
            except Exception:
                pass
            try:
                self.loop.close()
            except Exception:
                pass

    def stop(self) -> None:
        """停止dispatcher及所有worker"""
        self.stop_worker(*self.workers.keys())
        shutdown = self._shutdown
        if shutdown is None:
            return
        try:
            self.loop.call_soon_threadsafe(shutdown.set)
        except RuntimeError:
            pass
        # note: It must be called this way, otherwise the thread is unsafe and the write over there cannot be closed

    def start(self) -> None:
        with self._lock:
            if self.is_running():
                self._running.wait()
                return

            # Dispatcher thread can't be started twice; re-init env for restart.
            if self.thread.is_alive():
                self._running.wait()
                return
            if self.thread.ident is not None:
                self._set_env()

            self.thread.start()
            self._running.wait()

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()
