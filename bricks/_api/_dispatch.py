# -*- coding: utf-8 -*-
# @Time    : 2023-11-13 17:46
# @Author  : Kem
# @Desc    : task distribution
__all__ = (
    "Dispatcher",
    "Task"
)

import asyncio
import contextlib
import itertools
import queue
import sys
import threading
import traceback
from typing import Union, Dict

from loguru import logger

from bricks._api._events import Event

_counter = itertools.count()


class Task:
    """
    任务类, 用于存放任务信息

    """

    def __init__(self, func, args=None, kwargs=None, callback=None, errback=None):
        self.func = func
        self.args = args or []
        self.kwargs = kwargs or {}
        self.callback = callback
        self.errback = errback
        self._done = threading.Event()
        self._result = None
        self._done.clear()

    def result(self, timeout=None):
        self._done.wait(timeout=timeout)
        return self._result

    def set_result(self, v):
        self._result = v

    @property
    def done(self):
        return self._done.is_set()

    def exec(self):
        try:
            ret = self.func(*self.args, **self.kwargs)
            self.set_result(ret)
            self._run_callback()
            return ret

        except Exception as e:
            self.set_result(e)
            self._run_errback()
            raise e

        finally:
            self._done.set()

    async def async_exec(self):
        try:
            ret = await self.func(*self.args, **self.kwargs)
            self.set_result(ret)
            self._run_callback()
            return ret

        except Exception as e:
            self.set_result(e)
            self._run_errback()
            raise e
        finally:
            self._done.set()

    def _run_callback(self):
        try:
            self.callback and self.callback(self._result)
        except Exception as e:
            logger.exception(e)

    def _run_errback(self):
        try:
            self.errback and self.errback(self._result)
        except Exception as e:
            logger.exception(e)

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
                self.dispatcher.exec(task)

            except queue.Empty:
                pass

            except (KeyboardInterrupt, SystemExit) as e:
                raise e

            except Exception as e:
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

    def __init__(self, concurrents=1):
        self.concurrents = concurrents
        self.tasks = queue.Queue()
        self._workers: Dict[str, Worker] = {}

        self._shutdown = asyncio.Event()
        self.loop = asyncio.get_event_loop()
        self._running = threading.Event()
        self._semaphore = threading.Semaphore(self.concurrents)
        # atexit.register(self.shutdown)
        super().__init__(daemon=False, name="ForkConsumer")

    def create_future(self, task: Task):
        """
        create a async task future object

        :param task:
        :return:
        """

        assert task.is_async, "task must be async function"
        return asyncio.run_coroutine_threadsafe(task.async_exec(), self.loop)

    def create_worker(self, size: int):
        """
        create workers

        :param size:
        :return:
        """
        for _ in range(size):
            worker = Worker(self, name=f"worker-{next(_counter)}")
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
            # worker and terminate_thread(worker, SystemExit)
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

    def submit_task(self, task: Task, limit=True, timeout=None):
        if not self._running.wait(timeout=timeout):
            raise RuntimeError("dispatcher is not running")

        if self.concurrents == 1:
            self.do(task)

        elif task.is_async and limit is True:
            self._semaphore.acquire(timeout=timeout)
            try:
                future = self.create_future(task)
                future.add_done_callback(lambda _: self._semaphore.release())
            except Exception as e:
                self._semaphore.release()
                raise e

        elif task.is_async:
            self.create_future(task)

        elif limit is True:
            with self._semaphore:
                self.tasks.put(task)
        else:
            self.tasks.put(task)

        return task

    def exec(self, task: Task) -> Task:
        """
        Handles a task and executes the callback function while it exists
        When an exception occurs during task execution, a 'ErrorOccurred' signal is issued

        :return: `None`
        """

        if self.concurrents != 1:
            context = self._semaphore
        else:
            context = contextlib.nullcontext()

        with context:
            if task.is_async:
                future = self.create_future(task)
                # block it
                future.result()
            else:
                task.exec()
        return task

    def do(self, task: Task) -> Task:
        try:
            self.exec(task)
        except (KeyboardInterrupt, SystemExit) as e:
            raise e

        except Exception as e:
            Event.spark_events({
                "type": Event.ErrorOccurred,
                "error": e,
                "stack": traceback.format_exc(),
            })

        return task

    @staticmethod
    def wrap_task(task: Union[dict, Task]) -> Task:
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
        errback = task.get("errback")

        return Task(
            func=func,
            args=positional,
            kwargs=keyword,
            callback=callback,
            errback=errback,
        )

    def is_empty(self):
        return self.running == 0

    def is_running(self):
        return self._running.is_set() and not self._shutdown.is_set()

    @property
    def running(self):
        return self.concurrents + self.coroutines - self._semaphore._value - self._asemaphore._value + len(  # noqa
            self.tasks.queue)  # noqa

    def run(self):
        async def main():
            logger.debug("dispatcher is running")
            self._running.set()
            if self.concurrents != 1: self.create_worker(self.concurrents)
            while True:
                try:

                    shutdown = await asyncio.wait_for(self._shutdown.wait(), timeout=100)
                    if shutdown:
                        return
                except asyncio.TimeoutError:
                    pass

        asyncio.set_event_loop(self.loop)
        self.loop.run_until_complete(main())
        try:
            self.loop.close()
        except:
            pass

    def shutdown(self):
        for worker in self._workers.values():
            worker.stop()

        self._shutdown.set()

        # note: It must be called this way, otherwise the thread is unsafe and the write over there cannot be closed
        self.loop.call_soon_threadsafe(self._shutdown.set)

        self._running.clear()
