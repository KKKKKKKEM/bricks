# -*- coding: utf-8 -*-
# @Time    : 2023-11-13 17:46
# @Author  : Kem
# @Desc    : task distribution
__all__ = (
    "Dispatcher",
    "Task"
)

import asyncio
import itertools
import queue
import sys
import threading
import traceback
from typing import Union, Dict

from loguru import logger

from bricks._api._events import Event


class Task(asyncio.Future):
    """
    任务类, 用于存放任务信息

    """

    def __init__(self, func, args=None, kwargs=None, callback=None):
        self.func = func
        self.args = args or []
        self.kwargs = kwargs or {}
        self.callback = callback
        callback and self.add_done_callback(callback)
        super().__init__()

    @property
    def is_async(self):
        return asyncio.iscoroutinefunction(self.func)


class Worker(threading.Thread):
    """
    worker can execute task with threading

    """

    def __init__(self, dispatcher: 'Dispatcher', name: str, daemon=True, trace=True, **kwargs):
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

            except queue.Empty:
                self.dispatcher.stop_worker(self.name)
                return

            try:
                if task.is_async:
                    future = self.dispatcher.create_future(task)
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
        self._shutdown = True

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
        if self._shutdown and event == 'line':
            raise SystemExit()
        return self._localtrace


class Dispatcher(threading.Thread):
    """
    Dispatcher: Responsible for task scheduling, assignment and execution

    """

    def __init__(self, max_workers=1):
        self.max_workers = max_workers
        self.pending = asyncio.Queue(maxsize=max_workers)
        self.tasks = queue.Queue()
        self._workers: Dict[str, Worker] = {}
        self._current_workers = asyncio.Semaphore(self.max_workers)
        self._semaphore = asyncio.Semaphore(self.max_workers)
        self._awake = asyncio.Event()
        self._shutdown = asyncio.Event()
        self.loop = asyncio.get_event_loop()
        self._running = threading.Event()
        self._counter = itertools.count()

        # atexit.register(self.shutdown)
        super().__init__(daemon=False, name="ForkConsumer")

    def create_worker(self, size: int = 1):
        """
        create workers

        :param size:
        :return:
        """
        for _ in range(size):
            worker = Worker(self, name=f"worker-{next(self._counter)}")
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

    def submit_task(self, task: Task, timeout: int = None) -> asyncio.Future:
        if not self._running.wait(timeout=5):
            raise RuntimeError("dispatcher is not running")

        future = asyncio.run_coroutine_threadsafe(self.run_task(task, timeout=timeout), self.loop)
        timeout != -1 and future.result(timeout=timeout)

        return task

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
        return self._current_workers._value  # noqa

    def run(self):
        async def main():
            logger.debug("dispatcher is running")
            self._running.set()
            # if self.max_workers != 1: self.create_worker(self.max_workers)
            while not self._shutdown.is_set():
                await self._awake.wait()

                # The execution queue is not empty -> The task cannot be processed
                # The current number of workers is less than the maximum number of workers -> and there is still worker capacity remaining
                if not self.tasks.empty() and not self._current_workers.locked():
                    await self._current_workers.acquire()
                    logger.debug('create worker')
                    self.create_worker()
                    self._awake.clear()

        asyncio.set_event_loop(self.loop)
        self.loop.run_until_complete(main())
        try:
            self.loop.close()
        except:
            pass

    def stop(self):
        for worker in self._workers.values():
            worker.stop()

        # note: It must be called this way, otherwise the thread is unsafe and the write over there cannot be closed
        self.loop.call_soon_threadsafe(self._shutdown.set)
        self.loop.call_soon_threadsafe(self._awake.set)

        self._running.clear()
