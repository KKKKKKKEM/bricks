import itertools
import threading


class FastWriteCounter:
    """快写计数器, 利用GIL实现累加不加锁, 仅Cpython下有效"""

    __slots__ = (
        "_de_counter",
        "_in_counter",
        "_disable",
        "_lock",
    )

    def __init__(self, init=0, step=1):
        self._de_counter = itertools.count(init, step)
        self._in_counter = itertools.count(init, step)
        self._disable = False
        self._lock = threading.Lock()

    def increment(self, step: int = 1):
        if self._disable:
            return
        for _ in range(step):
            next(self._in_counter)

    def decrement(self, step: int = 1):
        if self._disable:
            return
        for _ in range(step):
            next(self._de_counter)

    @property
    def value(self):
        with self._lock:
            return next(self._in_counter) - next(self._de_counter)

    def disable(self):
        self._disable = True


class FastReadCounter:
    """
    快读计数器


    """

    def __init__(self, init=0, step=1):
        self._value = init
        self._step = step
        self._lock = threading.Lock()

    def increment(self):
        with self._lock:
            self._value += self._step

    def decrement(self):
        with self._lock:
            self._value -= self._step

    @property
    def value(self):
        return self._value
