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
import itertools
import queue
import sys
import threading
import traceback
from concurrent.futures import Future
from typing import Union, Dict, Optional

from loguru import logger

from bricks._api._events import Event


class Task(Future):
    """
    A future that is used to store task information

    """

    def __init__(self, func, args=None, kwargs=None, callback=None):
        self.func = func
        self.args = args or []
        self.kwargs = kwargs or {}
        self.callback = callback
        self.dispatcher: Optional["Dispatcher"] = None
        self.worker: Optional["Worker"] = None
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

    def __init__(self, dispatcher: 'Dispatcher', name: str, daemon=True, trace=False, **kwargs):
        self.dispatcher = dispatcher
        self._shutdown = False
        self.trace = trace
        self._awaken = threading.Event()
        self._awaken.set()
        super().__init__(daemon=daemon, name=name, **kwargs)

    def run(self) -> None:
        self.trace and sys.settrace(self._trace)
        while self.dispatcher.is_running() and not self._shutdown:
            try:
                task: Task = self.dispatcher.tasks.get(timeout=5)
                task.worker = self

            except queue.Empty:
                self.dispatcher.stop_worker(self.name)
                return

            try:
                if task.is_async:
                    future = self.dispatcher.create_future(task)
                    task.future = future
                    future.result()
                else:
                    ret = task.func(*task.args, **task.kwargs)
                    task.set_result(ret)

            except (KeyboardInterrupt, SystemExit) as e:
                raise e

            except Exception as e:
                task.set_exception(e)
                Event.spark_events({
                    "type": Event.ErrorOccurred,
                    "error": e,
                    "stack": traceback.format_exc(),
                })

    def stop(self) -> None:

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

        self._shutdown = True

    def pause(self) -> None:
        assert self.trace, "The pause function must be turned on trace mode, which may cause a decrease in formation and is not recommended to turn on"
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
        if self._shutdown and event == 'line':
            raise SystemExit()
        return self._localtrace


class Dispatcher(threading.Thread):
    """
    Dispatcher: Responsible for task scheduling, assignment and execution

    Outstanding features of this dispatcher:

    1. Supports setting a maximum number of concurrent tasks, and can automatically add or close workers based on the number of currently submitted tasks.
    2. Workers support manual pause and shutdown.
    3. The submitted tasks, whether asynchronous or synchronous, are all efficiently managed.
    4. After submission, a future is returned. To wait for the result, you simply need to call `future.result()`. You can also cancel the task, which simplifies asynchronous programming.


    """

    def __init__(self, max_workers=1, trace=False):
        self.max_workers = max_workers
        self.trace = trace
        self.tasks = queue.Queue()
        self._workers: Dict[str, Worker] = {}
        self._current_workers = asyncio.Semaphore(self.max_workers)
        self._semaphore = asyncio.Semaphore(self.max_workers)
        self._awake = asyncio.Event()
        self._shutdown = asyncio.Event()
        self.loop = asyncio.get_event_loop()
        self._running = threading.Event()
        self._counter = itertools.count()

        super().__init__(daemon=False, name="ForkConsumer")

    def create_worker(self, size: int = 1):
        """
        create workers

        :param size:
        :return:
        """
        for _ in range(size):
            worker = Worker(self, name=f"worker-{next(self._counter)}", trace=self.trace)
            self._workers[worker.name] = worker
            worker.start()

    def stop_worker(self, *idents: str):
        """
        stop workers

        :param idents:
        :return:
        """
        for ident in idents:
            worker = self._workers.pop(ident, None)
            worker and self.loop.call_soon_threadsafe(self._current_workers.release)
            worker and worker.stop()

    def pause_worker(self, *idents: str):
        """
        pause workers

        :param idents:
        :return:
        """
        for ident in idents:
            worker = self._workers.get(ident)
            worker and worker.pause()

    def awake_worker(self, *idents: str):
        """
        awake workers

        :param idents:
        :return:
        """
        for ident in idents:
            worker = self._workers.get(ident)
            worker and worker.awake()

    async def run_task(self, task: Task, timeout=None):
        if timeout == -1:
            self.tasks.put(task)

        else:
            async with self._semaphore:
                self.tasks.put(task)

        self._awake.set()

    def submit_task(self, task: Task, timeout: int = None) -> Task:
        if not self._running.wait(timeout=5):
            raise RuntimeError("dispatcher is not running")

        task.dispatcher = self
        future = asyncio.run_coroutine_threadsafe(self.run_task(task, timeout=timeout), self.loop)
        future.result(timeout=timeout if timeout != -1 else None)
        return task

    def cancel_task(self, task: Task):
        """
        cancel task

        :param task:
        :return:
        """
        with self.tasks.not_empty:
            # If the task is still in the task queue and has not been retrieved for use, it will be deleted from the task queue
            if task in self.tasks.queue:
                self.tasks.queue.remove(task)
                self.tasks.not_full.notify()
            else:
                # If there is a worker and the task is running, shut down the worker
                task.worker and not task.done() and self.stop_worker(task.worker.name)
                # notify the scheduler to reassign the worker
                self.loop.call_soon_threadsafe(self._awake.set)

    def create_future(self, task: Task):
        """
        create a async task future object

        :param task:
        :return:
        """

        assert task.is_async, "task must be async function"
        return asyncio.run_coroutine_threadsafe(task.func(*task.args, **task.kwargs), self.loop)

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
        return not self._shutdown.is_set()

    @property
    def running(self):
        return self.max_workers - self._current_workers._value + self.tasks.qsize()  # noqa

    def run(self):
        async def main():
            logger.debug("dispatcher is running")
            self._running.set()
            # if self.max_workers != 1: self.create_worker(self.max_workers)
            while not self._shutdown.is_set():
                await self._awake.wait()

                try:
                    # The execution queue is not empty -> The task cannot be processed
                    # The current number of workers is less than the maximum number of workers -> and there is still worker capacity remaining
                    if not self.tasks.empty() and not self._current_workers.locked():
                        await self._current_workers.acquire()
                        self.create_worker()

                finally:
                    self._awake.clear()

        asyncio.set_event_loop(self.loop)
        self.loop.run_until_complete(main())
        try:
            self.loop.close()
        except:
            pass

    def stop(self):
        self.stop_worker(*self._workers.keys())
        # note: It must be called this way, otherwise the thread is unsafe and the write over there cannot be closed
        self.loop.call_soon_threadsafe(self._shutdown.set)
        self.loop.call_soon_threadsafe(self._awake.set)

        self._running.clear()
