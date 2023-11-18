# -*- coding: utf-8 -*-
# @Time    : 2023-11-13 19:41
# @Author  : Kem
# @Desc    :
import collections
import functools
import itertools
import threading
from collections import defaultdict
from dataclasses import dataclass
from typing import Optional, Union, Callable

from loguru import logger

from bricks import const
from bricks.lib.context import Context, Error
from bricks.utils import pandora


class RegisteredEvents:
    def __init__(self):
        def output_exception(context: Error):
            logger.exception(context.error)

        # 持久事件
        self.permanent = defaultdict(functools.partial(defaultdict, list))
        self.permanent[None][const.ERROR_OCCURRED].append({"func": output_exception})

        # 一次性事件
        self.disposable = defaultdict(functools.partial(defaultdict, list))
        self.lock = threading.Lock()

    def __enter__(self):
        self.lock.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.lock.release()


@dataclass
class Task:
    func: callable
    args: Optional[list] = None
    kwargs: Optional[dict] = None
    match: Union[Callable, str] = None
    index: Optional[int] = None
    disposable: Optional[bool] = False


class EventManger:
    counter = collections.defaultdict(itertools.count)

    @classmethod
    def trigger(cls, context: Context):
        """
        trigger events: interact with external functions

        """

        for event in cls.acquire(context):
            yield cls._call(event, context)

    @classmethod
    def invoke(cls, context: Context):
        """
        invoke events: invoke all events

        :param context:
        :return:
        """
        for _ in cls.trigger(context):
            pass

    @classmethod
    def acquire(cls, context: Context):
        disposable = []
        for event in REGISTERED_EVENTS.disposable[context.target][context.form]:
            match = event.match
            if callable(match) and match(context):
                disposable.append(event)
                yield event
            elif isinstance(match, str) and eval(match, globals(), {"context": context}):
                disposable.append(event)
                yield event
            else:
                disposable.append(event)
                yield event

        for event in disposable:
            REGISTERED_EVENTS.disposable[context.target][context.form].remove(event)

        for event in REGISTERED_EVENTS.permanent[context.target][context.form]:
            match = event.match
            if callable(match) and match(context):
                yield event
            elif isinstance(match, str) and eval(match, globals(), {"context": context}):
                yield event
            else:
                yield event

    @classmethod
    def _call(cls, event: Task, context: Context):

        func = event.func
        args = event.args or []
        kwargs = event.kwargs or {}
        return pandora.invoke(
            func,
            args=args,
            kwargs=kwargs,
            annotations={type(context): context},
            namespace={"context": context}
        )

    @classmethod
    def register(cls, context: Context, *events: Task):

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

        REGISTERED_EVENTS.disposable[context.target][context.form].sort(key=lambda x: x.index)
        REGISTERED_EVENTS.permanent[context.target][context.form].sort(key=lambda x: x.index)


# 已注册事件
REGISTERED_EVENTS = RegisteredEvents()
