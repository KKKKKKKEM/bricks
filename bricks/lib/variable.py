# -*- coding: utf-8 -*-
# @Time    : 2023-12-05 15:54
# @Author  : Kem
# @Desc    :
import dataclasses
import itertools
import threading
import time
from typing import Dict


@dataclasses.dataclass
class Variable:
    """
    变量
    """
    value: any
    init_time: float = time.time()
    use_count: itertools.count = itertools.count()
    expired: bool = False
    max_count: int = None
    until: int = None

    def update_status(self):
        if self.max_count and next(self.use_count) >= self.max_count:
            self.expired = True

        elif self.until and self.until + self.init_time >= time.time():
            self.expired = True

        return self.expired


class VariableG:

    def __init__(self):
        self._map: Dict[str, Variable] = {}
        self._lock = threading.RLock()

    def __getattr__(self, item):
        return None

    def __setitem__(self, key, value):
        self._map[key] = Variable(value=value, init_time=time.time(), use_count=itertools.count(1))

    def __delitem__(self, key):
        return self._map.pop(key, None)

    def __getitem__(self, item):
        obj: Variable = self._map.get(item)
        if obj is None:
            return None

        if obj.expired:
            del self._map[item]
            return None

        if obj.update_status():
            del self._map[item]
        return obj.value

    def __enter__(self):
        self._lock.acquire()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._lock.release()

    def __repr__(self):
        return f'<VariableG [map:{self._map}]>'

    def expire(
            self,
            key,
            count: int = None,
            until: int = None,
    ):

        if key in self._map:
            self._map[key].max_count = count
            self._map[key].until = until

    def set_default(self, key, value):
        if key not in self._map:
            self[key] = value

        return self[key]

    def set(self, key, value, count: int = None, until: int = None):
        self[key] = value
        (count or until) and self.expire(key, count, until)

    def get(self, key, default=None):
        if key not in self._map:
            return default

        return self[key]

    def delete(self, key):
        del self[key]


class VariableT(threading.local, VariableG):
    """线程专用变量[各线程独立]"""

    def __repr__(self):
        return f'<VariableT [map:{self._map}]>'
