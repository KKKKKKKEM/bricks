# -*- coding: utf-8 -*-
# @Time    : 2023-11-13 19:41
# @Author  : Kem
# @Desc    :
import functools
import threading
from collections import defaultdict

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


class Event:

    @classmethod
    def trigger(cls, context: Context):
        """
        trigger events: interact with external functions

        """

        for event in cls.aquire(context):
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
    def aquire(cls, context: Context):
        disposable = []
        for event in REGISTERED_EVENTS.disposable[context.target][context.form]:
            match = event.get("match", None)
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
            match = event.get("match", None)
            if callable(match) and match(context):
                yield event
            elif isinstance(match, str) and eval(match, globals(), {"context": context}):
                yield event
            else:
                yield event

    @classmethod
    def _call(cls, event, context: Context):

        func = event['func']
        args = event.get('args') or []
        kwargs = event.get('kwargs') or {}
        return pandora.invoke(
            func,
            args=args,
            kwargs=kwargs,
            annotations={type(context): context},
            namespace={"context": context}
        )

    @classmethod
    def register(cls, context: Context, *events: dict):

        for event in events:

            disposable = event.get("disposable", False)
            index = event.get("index", None)

            if disposable:
                container = REGISTERED_EVENTS.disposable
            else:
                container = REGISTERED_EVENTS.permanent
            if index:
                container[context.target][context.form].insert(index, event)
            else:
                container[context.target][context.form].append(event)


# 已注册事件
REGISTERED_EVENTS = RegisteredEvents()
