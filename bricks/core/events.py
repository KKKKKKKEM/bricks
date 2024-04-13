# -*- coding: utf-8 -*-
# @Time    : 2023-11-13 19:41
# @Author  : Kem
# @Desc    :
import functools
import itertools
import threading
from collections import defaultdict
from dataclasses import dataclass
from typing import Optional, Union, Callable, Any, List, Literal, Dict

from loguru import logger

from bricks.core.context import Context
from bricks.utils import pandora


class RegisteredEvents:
    def __init__(self):
        # 持久事件
        self.permanent = defaultdict(functools.partial(defaultdict, list))
        # 一次性事件
        self.disposable = defaultdict(functools.partial(defaultdict, list))

        self.registed: Dict[str, List[Register]] = defaultdict(list)

        self._lock = threading.Lock()

    def __enter__(self):
        self._lock.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._lock.release()


@dataclass
class Task:
    func: Callable
    args: Optional[list] = None
    kwargs: Optional[dict] = None
    match: Union[Callable, str] = None
    index: Optional[int] = None
    disposable: Optional[bool] = False


@dataclass
class Register:
    task: Task
    container: list
    form: str
    target: Optional[Any] = None

    def unregister(self):
        """
        取消注册该事件

        :return:
        """
        self.container.remove(self.task)

    def reindex(self, index: int):
        """
        重构当前事件的 index, 并进行排序

        :param index:
        :return:
        """
        self.task.index = index
        self.container.sort(key=lambda x: x.index)

    def move2top(self):
        """
        将事件移动到最前面

        :return:
        """
        index = min([x.index for x in self.container])
        self.reindex(index - 1)

    def move2tail(self):
        """
        将事件移动到最后面

        :return:
        """
        index = max([x.index for x in self.container])
        self.reindex(index + 1)


class EventManager:
    counter = defaultdict(itertools.count)

    @classmethod
    def trigger(cls, context: Context, errors: Literal['raise', 'ignore', 'output'] = "raise"):
        """
        trigger events: interact with external functions

        """

        for event in cls.acquire(context):
            yield cls._call(event, context, errors=errors)

    @classmethod
    def invoke(cls, context: Context, errors: Literal['raise', 'ignore', 'output'] = "raise"):
        """
        invoke events: invoke all events

        :param errors:
        :param context:
        :return:
        """
        for _ in cls.trigger(context, errors=errors):
            pass

    @classmethod
    def acquire(cls, context: Context):
        targets = [None, context.target]
        for target in targets:
            disposable = []
            for event in REGISTERED_EVENTS.disposable[target][context.form]:
                match = event.match
                if match is None:
                    disposable.append(event)
                    yield event
                elif callable(match) and match(context):
                    disposable.append(event)
                    yield event
                elif isinstance(match, str) and eval(match, globals(), {"context": context}):
                    disposable.append(event)
                    yield event

            for event in disposable:
                REGISTERED_EVENTS.disposable[target][context.form].remove(event)

            for event in REGISTERED_EVENTS.permanent[target][context.form]:
                match = event.match
                if match is None:
                    yield event
                elif callable(match) and match(context):
                    yield event
                elif isinstance(match, str) and eval(match, globals(), {"context": context}):
                    yield event

    @classmethod
    def _call(cls, event: Task, context: Context, errors: Literal['raise', 'ignore', 'output'] = "raise"):
        try:
            return pandora.invoke(
                event.func,
                args=event.args,
                kwargs=event.kwargs,
                annotations={type(context): context},
                namespace={"context": context}
            )
        except Exception as e:
            if errors == "raise":
                raise
            elif errors == "ignore":
                pass
            else:
                logger.exception(e)

    @classmethod
    def register(cls, context: Context, *events: Task) -> List[Register]:
        ret = []
        for event in events:
            if isinstance(event, dict):
                event = Task(**event)

            disposable = event.disposable

            if disposable:
                container = REGISTERED_EVENTS.disposable
                counter = cls.counter[f'{context.target}.{context.form}.disposable']
            else:
                container = REGISTERED_EVENTS.permanent
                counter = cls.counter[f'{context.target}.{context.form}.permanent']

            event.index = next(counter) if event.index is None else event.index

            container[context.target][context.form].append(event)

            ret.append(Register(

                task=event,
                container=container[context.target][context.form],
                form=context.form,
                target=context.target
            ))

        REGISTERED_EVENTS.disposable[context.target][context.form].sort(key=lambda x: x.index)
        REGISTERED_EVENTS.permanent[context.target][context.form].sort(key=lambda x: x.index)
        REGISTERED_EVENTS.registed[context.target].extend(ret)
        return ret


def on(form: str, index: int = None, disposable: Optional[bool] = False, binding: Any = ...):
    """
    使用装饰器的方式注册事件
    如果有 staticmethod 之类的装饰器, 则需要紧贴着你的函数


    :param binding: 事件作用对象, 默认为自动判断
        如果是类相关的方法(静态方法, 类方法, 实例方法), 则会作用于该类的实例
        如果是普通函数(则会作用于全局)
        如果传入了对象, 则仅作用于该对象

    :param form: 事件类型, 可以传入 const 属性
    :param index: 事件排序, 默认会在最后注册执行, 按照代码顺序索引一次递增, 索引越大, 执行速度越靠后
    :param disposable: 是否为可弃用事件(仅运行一次)
    :return:
    """

    def inner(func):
        if binding is ...:
            if "." in func.__qualname__:
                setattr(func, "$event", (form, Task(func=func.__name__, index=index, disposable=disposable)))
            else:
                EventManager.register(Context(form=form), Task(func=func, index=index, disposable=disposable))
        else:
            EventManager.register(Context(form=form, target=binding),
                                  Task(func=func, index=index, disposable=disposable))
        return func

    return inner


# 已注册事件
REGISTERED_EVENTS = RegisteredEvents()
