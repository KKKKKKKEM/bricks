# -*- coding: utf-8 -*-
# @Time    : 2023-11-13 19:41
# @Author  : Kem
# @Desc    :
import functools
import threading
from collections import defaultdict

from loguru import logger

from bricks.lib.context import Context, ErrorContext
from bricks.utils import universal


class RegisteredEvents:
    def __init__(self):
        def output_exception(context: ErrorContext):
            logger.exception(context.error)

        # 持久事件
        self.permanent = defaultdict(functools.partial(defaultdict, list))
        self.permanent[None][EventEnum.ErrorOccurred].append(
            {
                "func": output_exception,
                "type": EventEnum.ErrorOccurred
            }
        )

        # 一次性事件
        self.disposable = defaultdict(functools.partial(defaultdict, list))
        self.lock = threading.Lock()

    def __enter__(self):
        self.lock.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.lock.release()


class EventEnum:
    ErrorOccurred = 'ErrorOccurred'
    BeforeStart = 'BeforeStart'
    BeforeClose = 'BeforeClose'


class Event:

    @classmethod
    def trigger(cls, context: Context):
        """
        trigger events: interact with external functions

        """

        for event in REGISTERED_EVENTS.disposable[context.target][context.form]:
            yield cls._call(event, context)

        REGISTERED_EVENTS.disposable[context.target].clear()

        for event in REGISTERED_EVENTS.permanent[context.target][context.form]:
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
    def _call(cls, event, context: Context):

        func = event['func']
        args = event.get('args') or []
        kwargs = event.get('kwargs') or {}
        return universal.invoke(func, args=args, kwargs=kwargs, annotations={type(context): context})

    @classmethod
    def register(cls, *events: dict):

        for event in events:

            disposable = event.get("disposable", False)
            event_type = event.get("type", None)

            if disposable:
                REGISTERED_EVENTS.disposable[event_type].append(event)
            else:
                REGISTERED_EVENTS.permanent[event_type].append(event)


# 已注册事件
REGISTERED_EVENTS = RegisteredEvents()
