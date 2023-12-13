# -*- coding: utf-8 -*-
# @Time    : 2023-11-15 12:19
# @Author  : Kem
# @Desc    :
import threading
from collections import deque

from bricks.core import signals
from bricks.lib.nodes import LinkNode


class LocalStack(threading.local):
    def __init__(self):
        self._stack = deque()

    def push(self, item):
        self._stack.append(item)

    def pop(self):
        return self._stack.pop()

    def top(self):
        try:
            return self._stack[-1]
        except IndexError:
            return None

    def __len__(self):
        return len(self._stack)


class Context:
    _stack = LocalStack()

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

    @classmethod
    def get_context(cls) -> "Context":
        """
        获取当前正在 consume 的 Context
        仅支持正在 consume 的
        没有被 on consume 使用的话是获取不到的

        :return:
        """
        return cls._stack.top()

    def set_context(self):
        self._stack.push(self)

    def clear_context(self):
        return self._stack.pop()

    def __enter__(self):
        self.set_context()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.clear_context()


class Flow(Context):

    def __init__(
            self,
            form: str,
            target=None,
            next: LinkNode = None,  # noqa
            flows: dict = None,
            args: list = None,
            kwargs: dict = None,
            **options
    ) -> None:
        self.next: LinkNode = next
        self.callback = None
        self.args = args or []
        self.kwargs = kwargs or {}
        self.flows = flows or {}
        super().__init__(form, target, **options)
        self.doing: deque = deque([self])
        self.pending: deque = deque([])

    def __setattr__(self, key, value):
        if key == "next":
            if isinstance(value, LinkNode):
                value = value.root
            value = LinkNode(value, getattr(self, "next", LinkNode()))

        super().__setattr__(key, value)

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
                node = LinkNode()

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
        self.next = node
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

        future = fun(dispatch.Task(context.next.root, [context]), timeout=-1)
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

    def error(self, shutdown=False):
        raise NotImplementedError

    def __copy__(self):
        return self.__class__(**self.__dict__)

    copy = __copy__


class Error(Context):
    def __init__(self, form: str, error: Exception, target=None, **kwargs) -> None:
        super().__init__(form, target, **kwargs)
        self.error = error
