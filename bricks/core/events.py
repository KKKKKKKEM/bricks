# -*- coding: utf-8 -*-
# @Time    : 2023-11-13 19:41
# @Author  : Kem
# @Desc    :
import functools
import itertools
import threading
from collections import defaultdict
from dataclasses import dataclass
from typing import Optional, Union, Callable, Any, List, Literal

from loguru import logger

from bricks.core.context import Context
from bricks.utils import pandora


class RegisteredEvents:
    def __init__(self):
        # 持久事件
        self.permanent = defaultdict(functools.partial(defaultdict, list))
        # 一次性事件
        self.disposable = defaultdict(functools.partial(defaultdict, list))

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
        disposable = []
        for event in REGISTERED_EVENTS.disposable[context.target][context.form]:
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
            REGISTERED_EVENTS.disposable[context.target][context.form].remove(event)

        for event in REGISTERED_EVENTS.permanent[context.target][context.form]:
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
        return ret


# 已注册事件
REGISTERED_EVENTS = RegisteredEvents()
