# -*- coding: utf-8 -*-
# @Time    : 2023-11-15 16:17
# @Author  : Kem
# @Desc    :
import copy
import functools
import json
import os
import queue
import threading
import time
from collections import deque, defaultdict
from typing import Optional, Union

from loguru import logger

from bricks.core import genesis
from bricks.utils import pandora


class Item(dict):

    def __init__(self, __dict: Optional[Union[dict, str]] = ..., **kwargs) -> None:
        if isinstance(__dict, self.__class__):
            self.fingerprint = __dict.fingerprint

        elif isinstance(__dict, str):
            self.fingerprint = __dict
            __dict = pandora.json_or_eval(__dict)

        elif isinstance(__dict, dict):
            self.fingerprint = copy.deepcopy(__dict)
            __dict = copy.deepcopy(__dict)

        else:
            self.fingerprint = None
        try:
            super().__init__(__dict, **kwargs)
        except:
            self.fingerprint = __dict

    fingerprint = property(
        fget=lambda self: getattr(self, "_fingerprint", None) or self,
        fset=lambda self, v: setattr(self, "_fingerprint", v),
        fdel=lambda self: setattr(self, "_fingerprint", None)
    )

    def rebuild_fingerprint(self):
        self.fingerprint = json.dumps(self, default=str, sort_keys=True)

    def __str__(self):
        if self:
            return repr(self)
        else:
            return repr(self.fingerprint)


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
            except:
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


class TaskQueue(metaclass=genesis.MetaClass):
    reversible = property(
        fget=lambda self: getattr(self, "_reversible", True),
        fset=lambda self, value: setattr(self, "_reversible", value),
        fdel=lambda self: setattr(self, "_reversible", True),
    )

    def continue_(self, name: str, maxsize=None, interval=1, **kwargs):
        """
        判断 name 是否可与继续投放

        :param interval: 休眠间隔
        :param maxsize: 队列最大大小
        :param name:
        :return:
        """
        if maxsize is None:
            return
        else:
            while self.size(name, **kwargs) >= maxsize:
                logger.debug(f'队列内种子数量已经超过 {maxsize}, 暂停投放')
                time.sleep(interval)

    @staticmethod
    def name2key(name: str, _type: str) -> str:
        """
        将 name 转换为 key

        :param name: 队列名
        :param _type: 队列类型
        :return:
        """
        if not _type:
            return name

        if name.endswith(f":{_type}"):
            return name
        else:
            return f'{name}:{_type}'

    def is_empty(self, name: str, **kwargs) -> bool:
        """
        判断队列是否为空

        :param name: 队列名
        :param kwargs: 传入 size 的其他参数
        :return:
        """
        threshold = kwargs.get('threshold', 0)
        return self.size(name, **kwargs) <= threshold

    def size(self, *names: str, qtypes: tuple = ('current', 'temp', 'failure'), **kwargs) -> int:
        """
        获取队列大小

        :param qtypes:
        :param names:
        :param kwargs:
        :return:
        """
        raise NotImplementedError

    def reverse(self, name: str, **kwargs) -> bool:
        """
        强制翻转队列

        :param name:
        :param kwargs:
        :return:
        """
        raise NotImplementedError

    def _when_reverse(self, func):  # noqa
        @functools.wraps(func)
        def inner(*args, **kwargs):
            reversible = self.reversible
            if reversible:
                return func(*args, **kwargs)
            else:
                logger.debug("[翻转失败] reversible 属性为 False")
                return False

        return inner

    def smart_reverse(self, name: str, status=0) -> bool:
        """
        智能翻转队列

        :return:
        """
        raise NotImplementedError

    _when_smart_reverse = _when_reverse

    def merge(self, dest: str, *queues: str, **kwargs):
        raise NotImplementedError

    def replace(self, name: str, old, new, **kwargs):
        """
        替换

        :return:
        """
        raise NotImplementedError

    def _when_replace(self, func):  # noqa
        def inner(name, old, new, **kwargs):
            if isinstance(old, Item): old = old.fingerprint
            if isinstance(new, Item): new = new.fingerprint

            return func(name, old, new, **kwargs)

        return inner

    def clear(self, *names, qtypes=('current', 'temp', "failure", "lock", "record"), **kwargs):
        """
        清空任务队列

        :param qtypes:
        :param kwargs:
        :return:
        """
        raise NotImplementedError

    def get(self, name: str, count: int = 1, **kwargs) -> Item:
        """
        获取一个任务

        :param count:
        :param name:
        :param kwargs:
        :return:
        """
        raise NotImplementedError

    def _when_get(self, func):  # noqa
        def inner(*args, **kwargs):
            ret = func(*args, **kwargs)
            if ret in [None, []]:
                return None

            elif isinstance(ret, list) and len(ret) != 1:
                return [Item(i) for i in ret]

            else:
                if isinstance(ret, list):
                    ret = ret[0]
                return Item(ret)

        return inner

    def put(self, name: str, *values, **kwargs):
        """
        放入一个任务

        :param name:
        :param values:
        :param kwargs:
        :return:
        """
        raise NotImplementedError

    def _when_put(self, func):  # noqa
        def inner(name, *values, **kwargs):
            return func(name, *[i.fingerprint if isinstance(i, Item) else i for i in values], **kwargs)

        return inner

    def remove(self, name: str, *values, **kwargs):
        """
        删除一个任务

        :param name:
        :param values:
        :param kwargs:
        :return:
        """
        raise NotImplementedError

    def _when_remove(self, func):  # noqa
        def inner(name, *values, **kwargs):
            return func(name, *[i.fingerprint if isinstance(i, Item) else i for i in values], **kwargs)

        return inner

    def command(self, name: str, order: dict):
        raise NotImplementedError


class LocalQueue(TaskQueue):

    def __init__(self) -> None:
        self._box = dict()
        self._container = getattr(self, "_container", None) or defaultdict(SmartQueue)
        self._locks = defaultdict(threading.Lock)
        self._status = defaultdict(threading.Event)

    def size(self, *names: str, qtypes: tuple = ('current', 'temp', 'failure'), **kwargs) -> int:
        if not names:
            return 0
        else:
            names = [self.name2key(name, _) for name in names for _ in qtypes]

        count = 0

        for name in names:
            count += self._container[name].qsize()

        return count

    def reverse(self, name: str, **kwargs) -> bool:
        qtypes = kwargs.pop('qtypes', None) or ["temp", "failure"]

        dest = self.name2key(name, 'current')
        for qtype in qtypes:
            _queue = self.name2key(name, qtype)
            with self._container[_queue].mutex:
                self._container[dest].put(*self._container[_queue].queue)
                self._container[_queue].queue.clear()
        return True

    def smart_reverse(self, name: str, status=0) -> bool:
        tc = self.size(name, qtypes=("temp",))
        cc = self.size(name, qtypes=("current",))
        fc = self.size(name, qtypes=("failure",))

        if cc == 0 and fc != 0:
            qtypes = ['failure']
            need_reverse = True

        elif cc == 0 and fc == 0 and tc != 0 and status == 0:
            qtypes = ['temp']
            need_reverse = True

        else:
            need_reverse = False
            qtypes = []

        if need_reverse:
            self.reverse(name, qtypes=qtypes)
            return True
        else:
            return False

    def merge(self, dest: str, *queues: str, **kwargs):
        with self._container[queues].mutex:
            for _queue in queues:
                self._container[dest].put(*self._container[_queue].queue)
                self._container[_queue].queue.clear()
        return True

    def replace(self, name: str, old, new, **kwargs):
        kwargs.setdefault('qtypes', 'current')
        self.remove(name, *pandora.iterable(old), **kwargs)
        return self.put(name, *pandora.iterable(new), **kwargs)

    def remove(self, name: str, *values, **kwargs):
        backup = kwargs.pop('backup', None)
        backup and self.put(name, *values, qtypes=backup)
        name = self.name2key(name, kwargs.get('qtypes', 'temp'))
        return self._container[name].remove(*values)

    def put(self, name: str, *values, **kwargs):
        name = self.name2key(name, kwargs.pop('qtypes', "current"))
        unique = kwargs.pop('unique', None)
        priority = kwargs.pop('priority', None)
        timeout = kwargs.pop('timeout', None)
        limit = kwargs.pop('limit', 0)
        head = bool(priority)
        return self._container[name].put(*values, block=True, timeout=timeout, unique=unique, limit=limit, head=head)

    def get(self, name: str, count: int = 1, **kwargs) -> Item:
        pop_key = self.name2key(name, 'current')
        add_key = self.name2key(name, 'temp')
        tail = kwargs.pop('tail', False)
        items = self._container[pop_key].get(block=False, timeout=None, count=count, tail=tail)
        items is not None and self._container[add_key].put(*pandora.iterable(items))
        return items

    def clear(self, *names, qtypes=('current', 'temp', "failure", "lock", "record"), **kwargs):
        for name in names:
            for qtype in qtypes:
                self._container.pop(self.name2key(name, qtype), None)

    def command(self, name: str, order: dict):
        def set_init_record():
            os.environ[f'{name}-init-record'] = json.dumps(order['record'], default=str)

        def reset_init_record():
            os.environ.pop(f'{name}-init-record', None)
            self.clear(name)

        def backup_init_record():
            record = order['record']
            os.environ[f'{name}-init-record-backup'] = json.dumps(record, default=str)

        actions = {
            "get-permission": lambda: True,

            "get-init-record": lambda: json.loads(os.environ.get(f'{name}-init-record') or "{}"),
            "reset-init-record": reset_init_record,
            "continue-init-record": lambda: self.reverse(name),
            "set-init-record": set_init_record,
            "backup-init-record": backup_init_record,

            "wait-for-init-start": lambda: self._status[f'{name}-init-start'].wait(),
            "set-init-start": lambda: self._status[f'{name}-init-start'].set(),
            "release-init-start": lambda: self._status.pop(f'{name}-init-start', None),

            "set-init-status": lambda: self._status[f'{name}-init-status'].set(),
            "is-init-status": lambda: self._status[f'{name}-init-status'].is_set(),
            "release-init-status": lambda: self._status[f'{name}-init-status'].clear(),
        }
        action = order['action']
        if action in actions:
            return actions[action]()
        else:
            raise ValueError(f"Action {action} not found")
