# -*- coding: utf-8 -*-
# @Time    : 2023-12-11 13:14
# @Author  : Kem
# @Desc    :
# -*- coding: utf-8 -*-
# @Time    : 2023-12-11 13:15
# @Author  : Kem
# @Desc    :
import copy
import functools
import json
import time
from typing import Optional, Union

from loguru import logger

from bricks.core import genesis
from bricks.utils import pandora


class Item(dict):

    def __init__(self, __dict: Optional[Union[dict, str]] = ..., **kwargs) -> None:
        if isinstance(__dict, self.__class__):
            self.fingerprint = __dict.fingerprint

        elif isinstance(__dict, str):
            self.fingerprint = __dict
            __dict = pandora.json_or_eval(__dict)

        elif isinstance(__dict, dict):
            self.fingerprint = copy.deepcopy(__dict)
            __dict = copy.deepcopy(__dict)

        else:
            self.fingerprint = None
        try:
            super().__init__(__dict, **kwargs)
        except:  # noqa
            self.fingerprint = __dict

    fingerprint = property(
        fget=lambda self: getattr(self, "_fingerprint", None) or self,
        fset=lambda self, v: setattr(self, "_fingerprint", v),
        fdel=lambda self: setattr(self, "_fingerprint", None)
    )

    def __str__(self):
        if self:
            return repr(self)
        else:
            return repr(self.fingerprint)


class TaskQueue(metaclass=genesis.MetaClass):
    subscribe = False

    class COMMANDS:
        GET_PERMISSION = "GET_PERMISSION"

        GET_RECORD = "GET_RECORD"
        CONTINUE_RECORD = "CONTINUE_RECORD"
        SET_RECORD = "SET_RECORD"

        WAIT_INIT = "WAIT_INIT"
        RESET_INIT = "RESET_INIT"
        RELEASE_INIT = "RELEASE_INIT"
        IS_INIT = "IS_INIT"
        SET_INIT = "SET_INIT"

        RUN_SUBSCRIBE = "RUN_SUBSCRIBE"

    reversible = property(
        fget=lambda self: getattr(self, "_reversible", True),
        fset=lambda self, value: setattr(self, "_reversible", value),
        fdel=lambda self: setattr(self, "_reversible", True),
    )

    @staticmethod
    def _to_str(*args):
        return [
            json.dumps(value, default=str, sort_keys=True, ensure_ascii=False)
            if not isinstance(value, (bytes, str, int, float)) else value
            for value in args
        ]

    def continue_(self, name: str, maxsize=None, interval=1, **kwargs):
        """
        判断 name 是否可与继续投放

        :param interval: 休眠间隔
        :param maxsize: 队列最大大小
        :param name:
        :return:
        """
        if maxsize is None:
            return
        else:
            while self.size(name, **kwargs) >= maxsize:
                logger.debug(f'队列内种子数量已经超过 {maxsize}, 暂停投放')
                time.sleep(interval)

    @staticmethod
    def name2key(name: str, _type: str) -> str:
        """
        将 name 转换为 key

        :param name: 队列名
        :param _type: 队列类型
        :return:
        """
        if not _type:
            return name

        if name.endswith(f":{_type}"):
            return name
        else:
            return f'{name}:{_type}'

    def is_empty(self, name: str, **kwargs) -> bool:
        """
        判断队列是否为空

        :param name: 队列名
        :param kwargs: 传入 size 的其他参数
        :return:
        """
        threshold = kwargs.get('threshold', 0)
        return self.size(name, **kwargs) <= threshold

    def size(self, *names: str, qtypes: tuple = ('current', 'temp', 'failure'), **kwargs) -> int:
        """
        获取队列大小

        :param qtypes:
        :param names:
        :param kwargs:
        :return:
        """
        raise NotImplementedError

    def reverse(self, name: str, **kwargs) -> bool:
        """
        强制翻转队列

        :param name:
        :param kwargs:
        :return:
        """
        raise NotImplementedError

    def _when_reverse(self, func):  # noqa
        @functools.wraps(func)
        def inner(*args, **kwargs):
            reversible = self.reversible
            if reversible:
                return func(*args, **kwargs)
            else:
                logger.debug("[翻转失败] reversible 属性为 False")
                return False

        return inner

    def smart_reverse(self, name: str, status=0) -> bool:
        """
        智能翻转队列

        :return:
        """
        raise NotImplementedError

    _when_smart_reverse = _when_reverse

    def merge(self, dest: str, *queues: str, **kwargs):
        raise NotImplementedError

    def replace(self, name: str, old, new, **kwargs):
        """
        替换

        :return:
        """
        raise NotImplementedError

    def _when_replace(self, func):  # noqa
        def inner(name, old, new, **kwargs):
            if isinstance(old, Item): old = old.fingerprint
            if isinstance(new, Item): new = new.fingerprint

            return func(name, old, new, **kwargs)

        return inner

    def clear(self, *names, qtypes=('current', 'temp', "failure", "lock", "record"), **kwargs):
        """
        清空任务队列

        :param qtypes:
        :param kwargs:
        :return:
        """
        raise NotImplementedError

    def get(self, name: str, count: int = 1, **kwargs) -> Item:
        """
        获取一个任务

        :param count:
        :param name:
        :param kwargs:
        :return:
        """
        raise NotImplementedError

    def _when_get(self, func):  # noqa
        def inner(*args, **kwargs):
            ret = func(*args, **kwargs)
            if ret in [None, []]:
                return None

            elif isinstance(ret, list) and len(ret) != 1:
                return [Item(i) for i in ret]

            else:
                if isinstance(ret, list):
                    ret = ret[0]
                return Item(ret)

        return inner

    def put(self, name: str, *values, **kwargs):
        """
        放入一个任务

        :param name:
        :param values:
        :param kwargs:
        :return:
        """
        raise NotImplementedError

    def _when_put(self, func):  # noqa
        def inner(name, *values, **kwargs):
            return func(name, *[i.fingerprint if isinstance(i, Item) else i for i in values], **kwargs)

        return inner

    def remove(self, name: str, *values, **kwargs):
        """
        删除一个任务

        :param name:
        :param values:
        :param kwargs:
        :return:
        """
        raise NotImplementedError

    def _when_remove(self, func):  # noqa
        def inner(name, *values, **kwargs):
            return func(name, *[i.fingerprint if isinstance(i, Item) else i for i in values], **kwargs)

        return inner

    def command(self, name: str, order: dict):
        raise NotImplementedError


# 必须在最后导入, 不然会出现循环导入
from bricks.lib.queues.smart import SmartQueue  # noqa: E402
from bricks.lib.queues.redis_ import RedisQueue  # noqa: E402
from bricks.lib.queues.local import LocalQueue  # noqa: E402
