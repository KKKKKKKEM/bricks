# -*- coding: utf-8 -*-
# @Time    : 2023-11-15 12:19
# @Author  : Kem
# @Desc    :
from collections import deque
from typing import Callable, Optional

from bricks.core import signals


class Context:

    def __init__(self, form: str, target=None, **kwargs) -> None:
        self.form = form
        self.target = target
        for k, v in kwargs.items():
            setattr(self, k, v)

    def obtain(self, name, default=None):
        return getattr(self, name, default)

    def install(self, name, value, nx=False):
        if nx:
            if hasattr(self, name):
                return getattr(self, name)
            else:
                setattr(self, name, value)
                return value
        else:
            setattr(self, name, value)
            return value


class Node:

    def __init__(
            self,
            root: Callable = None,
            prev: Optional['Node'] = None,
            callback: Optional[Callable] = None
    ):
        self._prev = prev
        self.root = root
        self.callback = callback

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
        self.callback = None
        super().__init__(form, target, **kwargs)
        self.doing: deque = deque([self])
        self.pending: deque = deque([])

    def _set_next(self, value):
        if isinstance(value, Node):
            value = value.root
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

        if "next" not in attrs:

            if self.next.root in self.flows:
                node = self.flows[self.next.root]

            else:
                node = Node()

            self.next = node

    def rollback(self, recursion=True):
        """
        回滚节点

        :param recursion: 为真的话, 会回滚至节点存在于 flows, 如果一直不存在, 就会置为 Node(None), 否则只返回上一级
        :return:
        :rtype:
        """
        node = self.next.prev
        while recursion and node and node.root not in self.flows:
            node = node.prev
        self._next = node
        return self

    def branch(self, attrs: dict = None, rollback=False, submit=True):
        """
        当前线程分支, 执行完当前 flow 之后就会执行这个分支

        :param attrs:
        :param rollback:
        :param submit:
        :return:
        """
        attrs = attrs or {}
        context = self.copy()
        for k, v in attrs.items(): setattr(context, k, v)
        rollback and context.rollback()
        submit and self.pending.append(context)
        return context

    def background(self, attrs: dict = None, rollback=False, action="active"):
        """
        后台分支

        submit 会抢占 worker / active 不会, 但是主任务不会等待他执行完毕, 类似后台守护任务
        这个 context 会被提交至调度器, 可能被其他线程获取到

        :param action: submit / active
        :param attrs:
        :param rollback:
        :return:
        """
        from bricks.core import dispatch, genesis
        assert self.target and isinstance(self.target, genesis.Pangu), \
            "The 'target' should be an instance of bricks.core.genesis.Pangu or an instance of its subclasses."
        context = self.branch(attrs, rollback, False)
        if action == "submit":
            fun = self.target.submit
        else:
            fun = self.target.active

        future = fun(dispatch.Task(context.next, [context]), timeout=-1)
        context.future = future
        return context

    def switch(self, attrs: dict = None):
        self.flow(attrs)
        raise signals.Switch

    def retry(self):
        raise NotImplementedError

    def success(self, shutdown=False):
        raise NotImplementedError

    def failure(self, shutdown=False):
        raise NotImplementedError

    def __copy__(self):
        return self.__class__(**self.__dict__)

    copy = __copy__


class Error(Context):
    def __init__(self, form: str, error: Exception, target=None, **kwargs) -> None:
        super().__init__(form, target, **kwargs)
        self.error = error
