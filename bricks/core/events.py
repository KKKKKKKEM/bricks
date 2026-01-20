# -*- coding: utf-8 -*-
# @Time    : 2023-11-13 19:41
# @Author  : Kem
# @Desc    :
import functools
import itertools
import threading
from collections import defaultdict
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Dict, List, Literal, Optional, Tuple, Union

from loguru import logger

from bricks.core.context import Context, Flow
from bricks.utils import pandora


class ErrorMode(str, Enum):
    """错误处理模式"""
    RAISE = "raise"  # 抛出异常
    IGNORE = "ignore"  # 忽略异常
    OUTPUT = "output"  # 输出日志


class RegisteredEvents:
    """事件注册容器，管理持久事件和一次性事件

    Attributes:
        permanent: 持久事件存储，按form和target组织
        disposable: 一次性事件存储，按form和target组织
        registered: 已注册事件的索引，按target组织
    """

    def __init__(self):
        # 持久事件
        self.permanent = defaultdict(functools.partial(defaultdict, list))
        # 一次性事件
        self.disposable = defaultdict(functools.partial(defaultdict, list))

        self.registered: Dict[Any, List[Register]] = defaultdict(list)

        self._lock = threading.RLock()  # 使用可重入锁，避免死锁
        self._sorted_cache: Dict[Tuple[str, Any], bool] = {}  # 缓存排序状态

    def __enter__(self):
        self._lock.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._lock.release()

    def mark_unsorted(self, form: str, target: Any):
        """标记指定容器需要重新排序"""
        self._sorted_cache[(form, target)] = False


@dataclass
class Task:
    """事件任务数据结构

    Attributes:
        func: 要执行的函数
        args: 位置参数
        kwargs: 关键字参数
        match: 匹配条件（函数或表达式字符串）
        index: 执行顺序索引
        disposable: 是否为一次性事件
        box: 任务所属的容器列表
    """
    func: Callable
    args: Optional[list] = None
    kwargs: Optional[dict] = None
    match: Optional[Union[Callable, str]] = None
    index: Optional[int] = None
    disposable: bool = False
    box: Optional[list] = None  # 改为Optional避免使用...
    match_code: Optional[object] = None  # lazily-compiled eval code object for str matches


@dataclass
class Register:
    """事件注册信息

    Attributes:
        task: 注册的任务
        form: 事件类型
        target: 事件目标
    """
    task: Task
    form: str
    target: Optional[Any] = None

    def unregister(self):
        """
        取消注册该事件，同时清理所有引用

        :return:
        """
        with REGISTERED_EVENTS:
            if self.task.box and self.task in self.task.box:
                self.task.box.remove(self.task)

            # 清理registered索引，避免内存泄漏
            if self.target in REGISTERED_EVENTS.registered:
                registered_list = REGISTERED_EVENTS.registered[self.target]
                if self in registered_list:
                    registered_list.remove(self)

    def reindex(self, index: int):
        """
        重构当前事件的 index, 并标记需要重新排序

        :param index: 新的索引值
        :return:
        """
        with REGISTERED_EVENTS:
            self.task.index = index
            # 标记需要重新排序，延迟到acquire时执行
            if self.task.box:
                REGISTERED_EVENTS.mark_unsorted(self.form, self.target)

    def move2top(self):
        """
        将事件移动到最前面

        :return:
        """
        with REGISTERED_EVENTS:
            if self.task.box:
                index = min([x.index for x in self.task.box])
                self.reindex(index - 1)

    def move2tail(self):
        """
        将事件移动到最后面

        :return:
        """
        with REGISTERED_EVENTS:
            if self.task.box:
                index = max([x.index for x in self.task.box])
                self.reindex(index + 1)


class EventManager:
    """事件管理器，负责事件的注册、触发和调用

    提供事件的生命周期管理，支持持久事件和一次性事件
    """
    counter = defaultdict(itertools.count)  # 事件索引计数器

    @classmethod
    def trigger(
            cls,
            context: Context,
            errors: Literal["raise", "ignore", "output"] = "raise",
            annotations: Optional[dict] = None,
            namespace: Optional[dict] = None,
    ):
        """
        触发事件：逐个执行匹配的事件并返回结果

        :param context: 上下文对象
        :param errors: 错误处理模式 (raise/ignore/output)
        :param annotations: 额外的注解参数
        :param namespace: 额外的命名空间
        :yield: 每个事件的执行结果
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
            annotations: Optional[dict] = None,
            namespace: Optional[dict] = None,
    ):
        """
        调用所有匹配的事件（不返回结果）

        :param context: 上下文对象
        :param errors: 错误处理模式 (raise/ignore/output)
        :param annotations: 额外的注解参数
        :param namespace: 额外的命名空间
        :return: None
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
            annotations: Optional[dict] = None,
            namespace: Optional[dict] = None,
            callback: Optional[Callable] = None,
    ):
        """
        创建下一个流程步骤

        :param ctx: 流程上下文
        :param form: 事件类型
        :param annotations: 额外的注解参数
        :param namespace: 额外的命名空间
        :param callback: 完成后的回调函数
        :return: 流程处理函数
        """
        def flow_handler(context: Flow):
            EventManager.invoke(
                context, annotations=annotations, namespace=namespace)
            if callback:
                callback(context)

        ctx.form = form
        ctx.flow({"next": flow_handler})
        return flow_handler

    @classmethod
    def acquire(cls, context: Context):
        """获取匹配的事件列表

        :param context: 上下文对象
        :yield: 匹配的事件任务
        """
        # 确定目标列表
        if context.target is None:
            targets = [None]
        elif context.target is ...:
            targets = [...]
        else:
            targets = [None, context.target]

        with REGISTERED_EVENTS:
            events_group = [
                # (group, is_disposable)
                (REGISTERED_EVENTS.disposable[context.form], True),
                (REGISTERED_EVENTS.permanent[context.form], False),
            ]

            # 收集需要移除的disposable事件
            to_remove = []

            for group, is_disposable in events_group:
                for target in targets:
                    # 获取事件列表
                    if target is ...:
                        events = [e for es in group.values() for e in es]
                    else:
                        events = list(group[target])

                    # 延迟排序：仅在需要时排序
                    if target is ...:
                        events.sort(key=lambda x: x.index if x.index is not None else 0)
                    else:
                        cache_key = (context.form, target)
                        if not REGISTERED_EVENTS._sorted_cache.get(cache_key, False):
                            events.sort(key=lambda x: x.index if x.index is not None else 0)
                            group[target] = events
                            REGISTERED_EVENTS._sorted_cache[cache_key] = True

                    # 匹配并yield事件
                    for event in events:
                        if cls._match_event(event, context):
                            # 记录需要移除的disposable事件
                            if is_disposable and event.box:
                                to_remove.append((event, event.box))
                            yield event

            # 移除disposable事件及其registered引用
            for event, box in to_remove:
                if event in box:
                    box.remove(event)
                # 清理registered索引
                cls._cleanup_registered(event)

    @classmethod
    def _match_event(cls, event: Task, context: Context) -> bool:
        """检查事件是否匹配上下文

        :param event: 事件任务
        :param context: 上下文对象
        :return: 是否匹配
        """
        match = event.match
        if match is None:
            return True
        if callable(match):
            return match(context)
        if isinstance(match, str):
            if event.match_code is None:
                event.match_code = compile(match, "<bricks.core.events.match>", "eval")
            return eval(event.match_code, globals(), {"context": context})
        return False

    @classmethod
    def _cleanup_registered(cls, event: Task):
        """清理registered索引中的事件引用

        :param event: 要清理的事件任务
        """
        for target, registers in list(REGISTERED_EVENTS.registered.items()):
            REGISTERED_EVENTS.registered[target] = [
                reg for reg in registers if reg.task is not event
            ]

    @classmethod
    def _call(
            cls,
            event: Task,
            context: Context,
            errors: Literal["raise", "ignore", "output"] = "raise",
            annotations: Optional[dict] = None,
            namespace: Optional[dict] = None,
    ):
        """调用事件函数

        :param event: 事件任务
        :param context: 上下文对象
        :param errors: 错误处理模式
        :param annotations: 额外的注解参数
        :param namespace: 额外的命名空间
        :return: 事件函数的返回值
        """
        try:
            # 简化空值处理，避免重复判断
            merged_annotations = {**context.annotations, **(annotations or {})}
            merged_namespace = {**context.namespace, **(namespace or {})}

            return pandora.invoke(
                event.func,
                args=event.args,
                kwargs=event.kwargs or {},
                annotations=merged_annotations,
                namespace=merged_namespace
            )
        except Exception as e:
            if errors == ErrorMode.RAISE.value or errors == "raise":
                raise
            elif errors == ErrorMode.IGNORE.value or errors == "ignore":
                pass
            elif errors == ErrorMode.OUTPUT.value or errors == "output":
                logger.exception(e)
                return None  # 返回None而不是什么都不返回

    @classmethod
    def register(cls, context: Context, *events: Task) -> List[Register]:
        """注册事件到事件管理器

        :param context: 上下文对象，包含form和target信息
        :param events: 要注册的事件任务
        :return: 注册信息列表
        """
        ret = []

        with REGISTERED_EVENTS:
            for event in events:
                # 支持字典形式的事件配置
                if isinstance(event, dict):
                    event = Task(**event)
                else:
                    # Avoid mutating/cross-sharing Task templates (e.g., tasks coming from decorators).
                    event = Task(
                        func=event.func,
                        args=list(event.args) if event.args is not None else None,
                        kwargs=dict(event.kwargs) if event.kwargs is not None else None,
                        match=event.match,
                        index=event.index,
                        disposable=event.disposable,
                    )

                disposable = event.disposable

                # 选择容器和计数器（提取公共逻辑）
                event_type = "disposable" if disposable else "permanent"
                container = REGISTERED_EVENTS.disposable if disposable else REGISTERED_EVENTS.permanent
                counter_key = f"{context.target}.{context.form}.{event_type}"
                counter = cls.counter[counter_key]

                # 设置索引和容器
                if event.index is None:
                    event.index = next(counter)
                event.box = container[context.form][context.target]
                if event.box is not None:
                    event.box.append(event)

                ret.append(
                    Register(task=event, form=context.form, target=context.target))

            # 标记需要重新排序，延迟到acquire时执行
            REGISTERED_EVENTS.mark_unsorted(context.form, context.target)

            # 添加到registered索引
            REGISTERED_EVENTS.registered[context.target].extend(ret)

        return ret


def on(
        form: str,
        index: Optional[int] = None,
        disposable: bool = False,
        args: Optional[list] = None,
        kwargs: Optional[dict] = None,
        match: Optional[Union[Callable, str]] = None,
):
    """
    使用装饰器的方式注册事件

    注意：如果使用 @staticmethod 等装饰器，@on 应该放在最内层（紧贴函数定义）

    :param form: 事件类型，可以传入常量属性
    :param index: 事件排序索引，默认按代码顺序递增，索引越大越靠后执行
    :param disposable: 是否为一次性事件（仅运行一次），默认False
    :param args: 传递给事件函数的位置参数
    :param kwargs: 传递给事件函数的关键字参数
    :param match: 匹配条件，可以是函数或表达式字符串
    :return: 装饰器函数

    Example:
        @on(form="before_request", index=10)
        def my_handler(context):
            print("Request starting")
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

        if "." not in func.__qualname__ or "<locals>" in func.__qualname__:
            EventManager.register(Context(form=form, target=None), e)
        else:
            setattr(func, "__event__", (form, e))

        return func

    return inner


# 已注册事件
REGISTERED_EVENTS = RegisteredEvents()
