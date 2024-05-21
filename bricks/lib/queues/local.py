# -*- coding: utf-8 -*-
# @Time    : 2023-12-11 13:14
# @Author  : Kem
# @Desc    :
import json
import os
import threading
import time
from collections import defaultdict

from loguru import logger

from bricks.lib.queues import TaskQueue, Item
from bricks.lib.queues.smart import SmartQueue
from bricks.utils import pandora


class LocalQueue(TaskQueue):

    def __init__(self) -> None:
        self._box = dict()
        self._container = getattr(self, "_container", None) or defaultdict(SmartQueue)
        self._locks = defaultdict(threading.Lock)
        self._status = defaultdict(threading.Event)

    def __str__(self):
        return f'<LocalQueue>'

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

    def replace(self, name: str, *values, **kwargs):
        qtypes = kwargs.pop('qtypes', ["current", "temp", "failure"])
        count = 0
        for (old, new) in values:
            for qtype in pandora.iterable(qtypes):
                if self.remove(name, old, qtypes=qtype):
                    count += self.put(name, new, qtypes=qtype)

        return count

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
        def set_record():
            key = self.name2key(name, "record")
            os.environ[key] = json.dumps(order['record'], default=str)

        def reset_init_record():
            key = self.name2key(name, "record")
            os.environ.pop(key, None)
            self.clear(name)

        def wait_for_init_start():
            key = self.name2key(name, "record")

            while not self._status[key].is_set() and self.is_empty(name):
                time.sleep(1)
                logger.debug('等待初始化开始')

        def release_init():
            key = self.name2key(name, 'record')
            os.environ.pop(key, None)
            self._status[key].clear()

        def get_record():
            key = self.name2key(name, 'record')
            record = json.loads(os.environ.get(key) or "{}")
            if record.get('status') == 0:
                os.environ.pop(key, None)
                return {}
            else:
                return record

        actions = {
            self.COMMANDS.GET_PERMISSION: lambda: {"state": True, "msg": "success"},

            self.COMMANDS.GET_RECORD: get_record,
            self.COMMANDS.CONTINUE_RECORD: lambda: self.reverse(name),

            self.COMMANDS.SET_RECORD: set_record,

            self.COMMANDS.RESET_INIT: reset_init_record,

            self.COMMANDS.WAIT_INIT: wait_for_init_start,
            self.COMMANDS.SET_INIT: lambda: self._status[self.name2key(name, "record")].set(),
            self.COMMANDS.IS_INIT: lambda: self._status[self.name2key(name, "record")].is_set(),
            self.COMMANDS.RELEASE_INIT: release_init,
        }
        action = order['action']
        if action in actions:
            return actions[action]()
