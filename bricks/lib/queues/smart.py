# -*- coding: utf-8 -*-
# @Time    : 2023-12-11 13:16
# @Author  : Kem
# @Desc    :
import queue
import time
from collections import deque


class SmartQueue(queue.Queue):
    """
    基于 queue.Queue 封装的 Queue
    支持更多智能的特性
        - in 操作 (但是可能会线程不安全)
        - 双端队列投放的特性
        - 可选的不抛出异常
        - 可选的唯一去重

    """
    __slots__ = ['unique']

    def __init__(self, maxsize=0, unique=False):
        """
        实例化 一个 SmartQueue

        :param maxsize: 队列最大大小限制
        :param unique: 队列元素是否保证唯一去重
        """
        self.unique = unique
        super().__init__(maxsize=maxsize)
        self.queue: deque

    def put(self, *items, block=True, timeout=None, unique=None, limit=0, head=False) -> int:
        """
        将 `items` 投放至 队列中

        :param items: 需要投放的 items
        :param block: 是否阻塞
        :param timeout: 超时时间
        :param unique: 是否唯一
        :param limit: 动态限制队列大小
        :param head: 是否投放至头部
        :return:

        .. code:: python

            from lego.libs.models import SmartQueue
            q = SmartQueue()

            q.put(
                *[{'name':'kem'}, {'name':'kem2'}, {'name':'kem3'}, {'name':'kem4'}],
            )

        """

        before = self.unfinished_tasks
        unique = self.unique if unique is None else unique
        for item in items:

            with self.not_full:
                if unique:
                    if item in self.queue:
                        continue

                maxsize = limit if limit > 0 else self.maxsize
                if maxsize > 0:
                    if not block:
                        if self._qsize() >= maxsize:
                            raise queue.Full
                    elif timeout is None:
                        while self._qsize() >= maxsize:
                            self.not_full.wait()
                    elif timeout < 0:
                        raise ValueError("'timeout' must be a non-negative number")
                    else:
                        endtime = time.time() + timeout
                        while self._qsize() >= maxsize:
                            remaining = endtime - time.time()
                            if remaining <= 0.0:
                                raise queue.Full
                            self.not_full.wait(remaining)

                self._put(item) if head is False else self._put_head(item)
                self.unfinished_tasks += 1
                self.not_empty.notify()

        return self.unfinished_tasks - before

    def _put_head(self, item):
        """
        将 item 放置队列头部

        :param item:
        :return:
        """
        self.queue.appendleft(item)

    def get(self, block=True, timeout=None, count=1, tail=False):
        """
        从队列中获取值

        :param block: 是否阻塞
        :param timeout: 等待时长
        :param count: 一次拿多少个
        :param tail: 是否从尾巴处拿(拿出最后的)
        :return:

        .. code:: python

            from lego.libs.models import SmartQueue
            q = SmartQueue()

            # 从尾部拿出一个
            q.get(tail=True)

            # 从头部拿出一个
            q.get()


        """
        with self.not_empty:
            if not block:
                if not self._qsize():
                    return None
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
                        return None
                    self.not_empty.wait(remaining)
            items = []
            try:
                for _ in range(count):
                    items.append(self._get() if tail is False else self._get_tail())
            except IndexError:
                pass

            self.not_full.notify()
            return items[0] if len(items) == 1 else items

    def remove(self, *values):
        """
        从多列中删除 `values`

        :param values: 需要删除的元素
        :return:

        .. code:: python

            from lego.libs.models import SmartQueue
            q = SmartQueue()

            # 删除一个元素
            q.remove({"name":"kem"})

        """
        count = 0
        with self.mutex:
            try:
                for value in values:
                    self.queue.remove(value)
            except:  # noqa
                pass
            else:
                count += 1

        return count

    def _get_tail(self):
        """
        从尾部获取一个值

        :return:
        """
        return self.queue.pop()

    def __contains__(self, item):
        """
        使之支持 in 操作

        :param item:
        :return:
        """
        return item in self.queue

    def clear(self):
        self.queue.clear()
