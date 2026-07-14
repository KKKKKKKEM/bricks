# -*- coding: utf-8 -*-
# @Time    : 2023-11-13 17:46
# @Author  : Kem
# @Desc    : task distribution
__all__ = ("Dispatcher", "Task")

import asyncio
import ctypes
import itertools
import queue
import sys
import threading
import time
from asyncio import ensure_future, futures
from collections import deque
from concurrent.futures import Future
from typing import Any, Callable, Dict, List, Optional, Union

from bricks.core import context, events, signals
from bricks.state import const

# 常量定义
_NO_TIMEOUT = -1  # 表示不使用超时限制


class Exit(signals.Signal):
    def __init__(self, target: "Worker"):
        self.target = target


class _TaskQueue:
    """任务队列：每个 Worker 独立 deque + 通用 deque，get 为 O(1)。"""

    def __init__(self):
        self._general: deque = deque()
        self._specific: Dict[str, deque] = {}
        self._specific_conds: Dict[str, threading.Condition] = {}
        self._general_cond = threading.Condition(threading.Lock())
        self._unfinished = 0

    def _get_worker_cond(self, worker_name: str) -> threading.Condition:
        cond = self._specific_conds.get(worker_name)
        if cond is None:
            cond = threading.Condition(self._general_cond._lock)
            self._specific_conds[worker_name] = cond
        return cond

    def put(self, task: "Task") -> None:
        with self._general_cond:
            if task.worker is not None:
                self._specific.setdefault(task.worker.name, deque()).append(task)
            else:
                self._general.append(task)
            self._general_cond.notify_all()
            self._unfinished += 1

    def task_done(self) -> None:
        with self._general_cond:
            self._unfinished -= 1
            if self._unfinished == 0:
                self._general_cond.notify_all()

    def get(self, block=True, timeout=None, worker: Optional["Worker"] = None) -> "Task":
        with self._general_cond:
            if not block:
                task = self._get_task(worker)
                if task is None:
                    raise queue.Empty
                return task

            if timeout is None:
                while True:
                    task = self._get_task(worker)
                    if task is not None:
                        return task
                    self._general_cond.wait()

            elif timeout < 0:
                raise ValueError("'timeout' must be a non-negative number")

            else:
                endtime = time.time() + timeout
                while True:
                    task = self._get_task(worker)
                    if task is not None:
                        return task
                    remaining = endtime - time.time()
                    if remaining <= 0.0:
                        raise queue.Empty
                    self._general_cond.wait(remaining)

    def _get_task(self, worker: Optional["Worker"] = None) -> Optional["Task"]:
        if worker is not None:
            specific = self._specific.get(worker.name)
            if specific:
                return specific.popleft()
        if self._general:
            return self._general.popleft()
        return None

    def get_nowait(self, worker: Optional["Worker"] = None) -> Optional["Task"]:
        with self._general_cond:
            return self._get_task(worker)

    def qsize(self) -> int:
        with self._general_cond:
            size = len(self._general)
            for d in self._specific.values():
                size += len(d)
            return size

    def empty(self) -> bool:
        return self.qsize() == 0

    @property
    def unfinished_tasks(self) -> int:
        with self._general_cond:
            return self._unfinished

    def cancel_task(self, task: "Task") -> bool:
        with self._general_cond:
            if task.worker is not None:
                specific = self._specific.get(task.worker.name)
                if specific and task in specific:
                    specific.remove(task)
                    self._unfinished -= 1
                    if self._unfinished == 0:
                        self._general_cond.notify_all()
                    return True
            if task in self._general:
                self._general.remove(task)
                self._unfinished -= 1
                if self._unfinished == 0:
                    self._general_cond.notify_all()
                return True
            return False


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
        self.is_async: bool = asyncio.iscoroutinefunction(func)
        super().__init__()
        if callback:
            self.add_done_callback(callback)

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
                continue

            if not task.set_running_or_notify_cancel():
                self.dispatcher.doing.task_done()
                continue

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

        self.awake()
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

    Features:

    1. Fixed-size worker pool, created once at startup.
    2. O(1) task dispatch via per-worker + general deque.
    3. Workers support manual pause and shutdown.
    4. Async and sync tasks are both managed via Future.
    """
    loop: asyncio.AbstractEventLoop
    doing: _TaskQueue
    workers: dict[str, Worker]
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
        self._worker_counter = itertools.count()

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

    def create_worker(self, size: int = 1, name_prefix: str = "Worker"):
        """
        create workers

        :param size:
        :param name_prefix:
        :return:
        """
        options = self.options or {}
        options.setdefault("trace", False)
        for _ in range(size):
            worker = Worker(
                self,
                name=f"{name_prefix}-{next(self._worker_counter)}",
                **options,
            )
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

        task.dispatcher = self

        if timeout != _NO_TIMEOUT:
            self._running_tasks.acquire(timeout=timeout)
            task.add_done_callback(lambda x: self._running_tasks.release())

        if task.is_async:
            self.active_task(task)
        else:
            self.doing.put(task)

        return task

    def active_task(self, task: Task, timeout: Optional[int] = None) -> Task:
        """
        activate a task to start running

        :param task: task to activate
        :param timeout: timeout in seconds, -1 means no limit
        :return: the activated task
        """
        assert self.is_running(), "dispatcher is not running"
        task.dispatcher = self

        if timeout != _NO_TIMEOUT:
            self._active_tasks.acquire(timeout=timeout)
            task.add_done_callback(lambda x: self._active_tasks.release())

        if task.is_async:
            self._run_async(task)
        else:
            self._run_sync(task)
        return task

    def _run_async(self, task: Task) -> None:
        def _chain():
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
                if task.set_running_or_notify_cancel():
                    task.set_exception(exc)
                raise

        self.loop.call_soon_threadsafe(_chain)

    def _run_sync(self, task: Task) -> None:
        def _execute():
            if not task.set_running_or_notify_cancel():
                return
            try:
                ret = task.func(*task.args, **task.kwargs)
                task.set_result(ret)
            except (SystemExit, KeyboardInterrupt):
                raise
            except BaseException as exc:
                task.set_exception(exc)
                raise

        threading.Thread(target=_execute, daemon=True).start()

    def cancel_task(self, task: Task) -> bool:
        """
        Remove a pending task from the queue. Running synchronous tasks follow
        Future semantics and cannot be force-cancelled.

        :param task: task to cancel
        :return: whether the task was removed from the queue
        """
        return self.doing.cancel_task(task)

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
            self.create_worker(self.max_workers)

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()
