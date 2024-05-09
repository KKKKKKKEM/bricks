# -*- coding: utf-8 -*-
# @Time    : 2023-11-13 17:46
# @Author  : Kem
# @Desc    : task distribution
__all__ = (
    "Dispatcher",
    "Task"
)

import asyncio
import ctypes
import queue
import sys
import threading
import time
from asyncio import futures, ensure_future
from concurrent.futures import Future
from typing import Union, Dict, Optional

from bricks.core import events, context, signals
from bricks.state import const


class Exit(signals.Signal):
    def __init__(self, target: 'Worker'):
        self.target = target


class _TaskQueue(queue.Queue):

    def get(self, block=True, timeout=None, worker: 'Worker' = None) -> 'Task':
        with self.not_empty:
            while True:
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
                item: Task = self._get()
                if item.worker in [None, worker]:
                    self.not_full.notify()
                    return item
                else:
                    self._put(item)


class Task(Future):
    """
    A future that is used to store task information

    """

    def __init__(self, func, args=None, kwargs=None, callback=None, worker: "Worker" = None):
        self.func = func
        self.args = args or []
        self.kwargs = kwargs or {}
        self.callback = callback
        self.dispatcher: Optional["Dispatcher"] = None
        self.worker: Optional["Worker"] = worker
        self.future: Optional["asyncio.Future"] = None
        callback and self.add_done_callback(callback)
        super().__init__()

    @property
    def is_async(self):
        return asyncio.iscoroutinefunction(self.func)

    def cancel(self) -> bool:
        self.dispatcher and self.dispatcher.cancel_task(self)
        self.future and self.future.cancel()
        return super().cancel()


class Worker(threading.Thread):
    """
    worker can execute task with threading

    """

    def __init__(self, dispatcher: 'Dispatcher', name: str, timeout=60, daemon=True, trace=False, **kwargs):
        self.dispatcher = dispatcher
        self._shutdown = threading.Event()
        self.timeout = timeout
        self.trace = trace
        self._awaken = threading.Event()
        self._awaken.set()
        super().__init__(daemon=daemon, name=name, **kwargs)

    def run(self) -> None:
        events.EventManager.invoke(context.Context(const.BEFORE_WORKER_START, target=...), errors='output')
        self.trace and sys.settrace(self._trace)
        while self.dispatcher.is_running() and not self._shutdown.is_set():
            not self.trace and self._awaken.wait()
            try:
                task: Task = self.dispatcher.doing.get(timeout=self.timeout, worker=self)
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

            except (KeyboardInterrupt, SystemExit) as e:
                raise e

            except Exception as e:
                task.set_exception(e)
                events.EventManager.invoke(context.Error(error=e))

            finally:
                self.dispatcher.doing.task_done()

    def stop(self) -> None:
        if not self.is_alive() or self._shutdown.is_set():
            return

        def main(obj): raise Exit(obj)

        self.dispatcher.doing.put(Task(func=main, args=[self], worker=self))

    def shutdown(self):
        if not self.is_alive():
            return
        exc = ctypes.py_object(SystemExit)
        tid = ctypes.c_long(self.ident)

        res = ctypes.pythonapi.PyThreadState_SetAsyncExc(tid, exc)
        if res == 0:
            raise ValueError("nonexistent thread id")
        elif res > 1:
            ctypes.pythonapi.PyThreadState_SetAsyncExc(tid, None)
            raise SystemError("PyThreadState_SetAsyncExc failed")

    def clean(self):
        events.EventManager.invoke(
            context.Context(const.BEFORE_WORKER_CLOSE, target=...),
            errors='output'
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
        if event == 'call':
            return self._localtrace
        else:
            return None

    def _localtrace(self, frame, event, arg):  # noqa
        self._awaken.wait()
        if self._shutdown.is_set() and event == 'line':
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

    def __init__(self, max_workers=1, options: dict = None):
        self.max_workers = max_workers
        self.options = options or {}
        self.loop: asyncio.AbstractEventLoop
        self.doing: _TaskQueue
        self.workers: Dict[str, Worker]

        self._remain_workers: queue.Queue
        self._running_tasks: threading.Semaphore
        self._active_tasks: threading.Semaphore
        self._shutdown: asyncio.Event
        self._running: threading.Event
        self.thread: threading.Thread
        self._lock: threading.Lock
        self._set_env()

    def _set_env(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.doing = _TaskQueue()
        self.workers: Dict[str, Worker] = {}
        self._remain_workers = queue.Queue()
        for i in range(self.max_workers): self._remain_workers.put(f"Worker-{i}")
        self._running_tasks = threading.Semaphore(self.max_workers)
        self._active_tasks = threading.Semaphore(self.max_workers)
        self._shutdown = asyncio.Event()
        self._running = threading.Event()
        self.thread = threading.Thread(target=self.run, name="DisPatch", daemon=True)
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
            options.setdefault('trace', False)
            worker = Worker(self, name=ident, **options)
            self.workers[worker.name] = worker
            worker.start()

    def stop_worker(self, *idents: str):
        """
        stop workers

        :param idents:
        :return:
        """
        waiters = []
        for ident in idents:
            worker = self.workers.pop(ident, None)
            worker and self._remain_workers.put(worker.name)
            worker and worker.stop()
            worker and waiters.append(worker)

        for waiter in waiters: waiter.wait()

    def pause_worker(self, *idents: str):
        """
        pause workers

        :param idents:
        :return:
        """
        for ident in idents:
            worker = self.workers.get(ident)
            worker and worker.pause()

    def awake_worker(self, *idents: str):
        """
        awake workers

        :param idents:
        :return:
        """
        for ident in idents:
            worker = self.workers.get(ident)
            worker and worker.awake()

    def submit_task(self, task: Task, timeout: int = None) -> Task:
        """
        submit a task to the workers pool to run

        :param task:
        :param timeout:
        :return:
        """
        assert self.is_running(), "dispatcher is not running"

        def submit():
            task.dispatcher = self
            if task.is_async:
                self.active_task(task)
            else:
                self.doing.put(task)

        timeout != -1 and self._running_tasks.acquire(timeout=timeout)
        submit()
        timeout != -1 and task.add_done_callback(lambda x: self._running_tasks.release())

        self.adjust_workers()
        return task

    def active_task(self, task: Task, timeout: int = None) -> Task:
        """
        activate a task to start running

        :param timeout:
        :param task:
        :return:
        """

        def run_async_task():
            def callback():
                try:
                    futures._chain_future(  # noqa
                        ensure_future(task.func(*task.args, **task.kwargs), loop=self.loop), task
                    )
                except (SystemExit, KeyboardInterrupt):
                    raise
                except BaseException as exc:
                    if task.set_running_or_notify_cancel():
                        task.set_exception(exc)
                    raise

            self.loop.call_soon_threadsafe(callback)

        def run_sync_task():

            def callback():
                try:
                    ret = task.func(*task.args, **task.kwargs)
                    task.set_result(ret)
                except (SystemExit, KeyboardInterrupt):
                    raise
                except BaseException as exc:
                    if task.set_running_or_notify_cancel():
                        task.set_exception(exc)
                    raise

            threading.Thread(target=callback, daemon=True).start()

        def active():
            if task.is_async:
                run_async_task()
            else:
                run_sync_task()

        assert self.is_running(), "dispatcher is not running"

        timeout != -1 and self._active_tasks.acquire(timeout=timeout)
        active()
        timeout != -1 and task.add_done_callback(lambda x: self._active_tasks.release())
        return task

    def adjust_workers(self):
        remain_workers = self._remain_workers.qsize()
        remain_tasks = self.doing.qsize()
        if remain_workers > 0 and remain_tasks > 0:
            self.create_worker(min(remain_workers, remain_tasks))

    def cancel_task(self, task: Task):
        """
        cancel task

        :param task:
        :return:
        """
        with self.doing.not_empty:
            # If the task is still in the task queue and has not been retrieved for use, it will be deleted from the task queue
            if task in self.doing.queue:
                self.doing.queue.remove(task)
                self.doing.task_done()
                self.doing.not_full.notify()
            else:
                # If there is a worker and the task is running, shut down the worker
                task.worker and not task.done() and self.stop_worker(task.worker.name)

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

        func = task.get('func')

        # get positional parameters
        positional = task.get('args', [])
        positional = [positional] if not isinstance(positional, (list, tuple)) else positional

        # get keyword parameters
        keyword = task.get('kwargs', {})
        keyword = {} if not isinstance(keyword, dict) else keyword

        # get callback function
        callback = task.get("callback")

        return Task(
            func=func,
            args=positional,
            kwargs=keyword,
            callback=callback,
        )

    def is_running(self):
        return not self._shutdown.is_set() and self._running.is_set()

    @property
    def running(self):
        return self.doing.unfinished_tasks

    def run(self):
        async def main():
            self.loop = asyncio.get_event_loop()
            self._shutdown = asyncio.Event()
            self._running.set()
            await self._shutdown.wait()
            self._set_env()

        asyncio.run(main())

    def stop(self):
        self.stop_worker(*self.workers.keys())
        self.loop.call_soon_threadsafe(self._shutdown.set)
        # note: It must be called this way, otherwise the thread is unsafe and the write over there cannot be closed

    def start(self) -> None:
        with self._lock:
            if not self.is_running():
                self.thread.start()
            self._running.wait()

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()
