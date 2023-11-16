# -*- coding: utf-8 -*-
# @Time    : 2023-11-15 12:19
# @Author  : Kem
# @Desc    :
from collections import deque
from typing import Callable, Optional


class Context:

    def __init__(self, form: str, target=None, **kwargs) -> None:
        self.form = form
        self.target = target
        for k, v in kwargs.items():
            setattr(self, k, v)


class Node:

    def __init__(
            self,
            root: Callable = None,
            prev: Optional['Node'] = None,
    ):
        self._prev = prev
        self.root = root

    @property
    def prev(self):
        if not isinstance(self._prev, Node):
            return Node(self._prev)
        return self._prev

    def __bool__(self):
        return bool(self.root)

    def __str__(self):
        return f'Node({self.root})'

    def __call__(self, *args, **kwargs):
        return self.root(*args, **kwargs)


class Flow(Context):
    _next: Node

    def __init__(self, form: str, target=None, **kwargs) -> None:
        super().__init__(form, target, **kwargs)
        self.doing: deque = deque([self])
        self.pending: deque = deque([])
        self.signpost = 0

    def _set_next(self, value):
        self._next = Node(value, self.next)

    next: Node = property(
        fget=lambda self: getattr(self, "_next", Node()),
        fset=_set_next,
        fdel=lambda self: setattr(self, "_next", Node())
    )
    args: list = property(
        fget=lambda self: getattr(self, "_args", []),
        fset=lambda self, value: setattr(self, "_args", value),
        fdel=lambda self: setattr(self, "_args", [])
    )
    kwargs: dict = property(
        fget=lambda self: getattr(self, "_kwargs", {}),
        fset=lambda self, value: setattr(self, "_kwargs", value),
        fdel=lambda self: setattr(self, "_kwargs", {})
    )
    flows: dict = property(
        fget=lambda self: getattr(self, "_flow", {}),
        fset=lambda self, value: setattr(self, "_flow", value),
        fdel=lambda self: setattr(self, "_flow", {})
    )

    def is_continue(self) -> bool:
        return bool(self.doing or self.pending)

    def produce(self) -> 'Flow':
        queue = self.doing or self.pending
        if queue:
            return queue.popleft()

    def flow(self, attrs=None):
        # 更新属性
        attrs = attrs or {}
        for k, v in attrs.items():
            setattr(self, k, v)

        if "next" not in attrs and self.next.root in self.flows:
            self.next = self.flows[self.next.root]

    def rollback(self):
        self._next = self.next.prev
        return self


class Error(Context):
    def __init__(self, form: str, error: Exception, target=None) -> None:
        super().__init__(form, target)
        self.error = error
