# -*- coding: utf-8 -*-
# @Time    : 2023-12-11 13:57
# @Author  : Kem
# @Desc    :
import atexit
import math
import queue
import sys
import threading
from typing import Callable

from bricks.utils import pandora


class Collector:
    """
    收集数据, 批量处理

    """

    def __init__(
            self,
            callback: Callable,
            max_count: int = 0,
            max_bytes: int = 256 * 1024,

    ) -> None:

        self.callback = callback
        self.max_count = max_count or math.inf
        self.max_bytes = max_bytes
        self._queue = queue.Queue(maxsize=max_count)
        self._flag = threading.Event()

        self.start_guard()

    def guard(self):
        while True:
            self._flag.wait()
            if not self.is_push:
                self._flag.clear()
                continue

            self.flush()

    def start_guard(self):
        t = threading.Thread(target=self.guard, daemon=True)
        t.start()
        atexit.register(self.flush)

    def put(self, item, block=True, timeout=None):
        self._queue.put(item, block=block, timeout=timeout)
        self._flag.set()
        return True

    def get(self, block=True, timeout=None):
        return self._queue.get(block=block, timeout=timeout)

    def is_emtpy(self):
        return self._queue.empty()

    @property
    def total_bytes(self):
        with self._queue.mutex:
            return sys.getsizeof(self._queue.queue)

    @property
    def total_size(self):
        return self._queue.qsize()

    @property
    def is_push(self):
        return self.total_bytes >= self.max_bytes or self.total_size >= self.max_count

    def flush(self):
        """
        触发回调函数, 处理队列内的 items

        :return:
        """

        with self._queue.not_empty:
            pending = []
            while not (sys.getsizeof(pending) >= self.max_bytes or len(pending) > self.max_count):
                if not self._queue.queue:
                    break

                item = self._queue.queue.popleft()
                self._queue.not_full.notify()
                pending.append(item)

            pandora.invoke(self.callback, args=pending)


if __name__ == '__main__':
    collect = Collector(lambda *a: print(a), max_count=10, max_bytes=1024)
    for i in range(100):
        collect.put({"id": i})
