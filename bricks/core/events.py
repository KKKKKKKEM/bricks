# -*- coding: utf-8 -*-
# @Time    : 2023-11-13 19:41
# @Author  : Kem
# @Desc    :
import functools
import itertools
import threading
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Literal, Optional, Union

from loguru import logger

from bricks.core.context import Context, Flow
from bricks.utils import pandora


class RegisteredEvents:
    def __init__(self):
        # 持久事件
        self.permanent = defaultdict(functools.partial(defaultdict, list))
        # 一次性事件
        self.disposable = defaultdict(functools.partial(defaultdict, list))

        self.registered: Dict[str, List[Register]] = defaultdict(list)

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
    match: Optional[Union[Callable, str]] = None
    index: Optional[int] = None
    disposable: Optional[bool] = False
    box: list = ...


@dataclass
class Register:
    task: Task
    form: str
    target: Optional[Any] = None

    def unregister(self):
        """
        取消注册该事件

        :return:
        """
        self.task.box.remove(self.task)

    def reindex(self, index: int):
        """
        重构当前事件的 index, 并进行排序

        :param index:
        :return:
        """
        self.task.index = index
        self.task.box.sort(key=lambda x: x.index)

    def move2top(self):
        """
        将事件移动到最前面

        :return:
        """
        index = min([x.index for x in self.task.box])
        self.reindex(index - 1)

    def move2tail(self):
        """
        将事件移动到最后面

        :return:
        """
        index = max([x.index for x in self.task.box])
        self.reindex(index + 1)


class EventManager:
    counter = defaultdict(itertools.count)

    @classmethod
    def trigger(
            cls,
            context: Context,
            errors: Literal["raise", "ignore", "output"] = "raise",
            annotations: dict = None,  # type: ignore
            namespace: dict = None,  # type: ignore
    ):
        """
        trigger events: interact with external functions

        """

        for event in cls.acquire(context):
            yield cls._call(
                event,
                context,
                errors=errors,
                annotations=annotations,
                namespace=namespace,
            )

    @classmethod
    def invoke(
            cls,
            context: Context,
            errors: Literal["raise", "ignore", "output"] = "raise",
            annotations: dict = None,  # type: ignore
            namespace: dict = None,  # type: ignore
    ):
        """
        invoke events: invoke all events

        :param namespace:
        :param annotations:
        :param errors:
        :param context:
        :return:
        """
        for _ in cls.trigger(
                context, errors=errors, annotations=annotations, namespace=namespace
        ):
            pass

    @classmethod
    def next(
            cls,
            ctx: Flow,
            form: str,
            annotations: dict = None,  # type: ignore
            namespace: dict = None,  # type: ignore
            callback: Callable = None,  # type: ignore
    ):
        def main(context: Flow):
            EventManager.invoke(context, annotations=annotations, namespace=namespace)
            callback and callback(context)  # type: ignore

        ctx.form = form
        ctx.flow({"next": main})
        return main

    @classmethod
    def acquire(cls, context: Context):
        if context.target is None:
            targets = [None]

        elif context.target is ...:
            targets = [...]
        else:
            targets = [None, context.target]

        events_group = [
            REGISTERED_EVENTS.disposable[context.form],
            REGISTERED_EVENTS.permanent[context.form],
        ]
        for i, group in enumerate(events_group):
            group: dict

            for target in targets:
                if target is ...:
                    events = [e for es in group.values() for e in es]
                else:
                    events = list(group[target])

                for event in events:
                    match = event.match
                    if (
                            (match is None)
                            or (callable(match) and match(context))
                            or (
                            isinstance(match, str)
                            and eval(match, globals(), {"context": context})
                    )
                    ):
                        event.disposable and event.box.remove(event)  # type: ignore
                        yield event

    @classmethod
    def _call(
            cls,
            event: Task,
            context: Context,
            errors: Literal["raise", "ignore", "output"] = "raise",
            annotations: dict = None,  # type: ignore
            namespace: dict = None,  # type: ignore
    ):
        try:
            annotations = annotations or {}
            namespace = namespace or {}
            return pandora.invoke(
                event.func,
                args=event.args,
                kwargs=event.kwargs,  # type: ignore
                annotations={type(context): context, **annotations},
                namespace={"context": context, **namespace},
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
                counter = cls.counter[f"{context.target}.{context.form}.disposable"]
            else:
                container = REGISTERED_EVENTS.permanent
                counter = cls.counter[f"{context.target}.{context.form}.permanent"]

            event.index = next(counter) if event.index is None else event.index
            event.box = container[context.form][context.target]
            event.box.append(event)

            ret.append(Register(task=event, form=context.form, target=context.target))

        REGISTERED_EVENTS.disposable[context.form][context.target].sort(
            key=lambda x: x.index
        )
        REGISTERED_EVENTS.permanent[context.form][context.target].sort(
            key=lambda x: x.index
        )
        REGISTERED_EVENTS.registered[context.target].extend(ret)  # type: ignore
        return ret


def on(
        form: str,
        index: Optional[int] = None,
        disposable: Optional[bool] = False,
        args: Optional[list] = None,
        kwargs: Optional[dict] = None,
        match: Optional[Union[Callable, str]] = None,
):
    """
    使用装饰器的方式注册事件
    如果有 staticmethod 之类的装饰器, 则需要紧贴着你的函数


    :param is_global: 是否为全局事件, 如果非对象方法 / 类方法, 需要标识为 True 才会起作用
    :param match:
    :param kwargs:
    :param args:
    :param form: 事件类型, 可以传入 const 属性
    :param index: 事件排序, 默认会在最后注册执行, 按照代码顺序索引一次递增, 索引越大, 执行速度越靠后
    :param disposable: 是否为可弃用事件(仅运行一次)
    :return:
    """

    def inner(func):
        e = Task(
            func=func,
            index=index,
            disposable=disposable,
            args=args,
            kwargs=kwargs,
            match=match,
        )

        if "." in func.__qualname__:
            EventManager.register(Context(form=form, target=None), e)
        else:
            setattr(func, "__event__", (form, e))

        return func

    return inner


# 已注册事件
REGISTERED_EVENTS = RegisteredEvents()
