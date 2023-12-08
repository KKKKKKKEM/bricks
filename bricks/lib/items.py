# -*- coding: utf-8 -*-
# @Time    : 2023-11-15 21:53
# @Author  : Kem
# @Desc    :
import csv
import itertools
import os
from collections import UserList
from types import GeneratorType
from typing import Optional, Union, Any, Callable


class Items(UserList):
    def __init__(self, data=None, callback: Callable = None):
        self.callback = callback
        if isinstance(data, dict):
            data = [data]

        super().__init__(initlist=data)

    def update(self, *args, **kwargs) -> None:
        for item in self.data:
            item.update(*args, **kwargs)

    def setdefault(self, __key, __default):
        for item in self.data:
            item.setdefault(__key, __default)

    def drop(self, columns):
        for item in self.data:
            for column in columns:
                try:
                    item.__delitem__(column)
                except:  # noqa
                    pass

    @property
    def columns(self):
        return {j for i in self.data for j in i.keys()}

    def to_csv(
            self,
            path: str,
            header: Union[str, list, None] = "inner",
            mode: str = "w",
            delimiter: Any = ",",
            doublequote: bool = True,
            escapechar: Optional[bool] = None,
            lineterminator: str = '\r\n',
            quotechar: str = '"',
            quoting: int = csv.QUOTE_MINIMAL,
            skipinitialspace: bool = False,
            strict: bool = False,
            writer=None,
            **kwargs
    ):
        """
        将 items 写入 csv 文件

        :param writer:
        :param strict: 如果为 True，则在输入错误的 CSV 时抛出 Error 异常。默认值为 False。
        :param skipinitialspace: 如果为 True，则忽略 定界符 之后的空格。默认值为 False。
        :param quoting: 控制 writer 何时生成引号，以及 reader 何时识别引号。该属性可以等于任何 QUOTE_* 常量（参见 模块内容 段落），默认为 QUOTE_MINIMAL。
        :param quotechar: 一个单字符，用于包住含有特殊字符的字段，特殊字符如 定界符 或 引号字符 或换行符。默认为 '"'。
        :param lineterminator: 放在 writer 产生的行的结尾，默认为 '\r\n'
        :param escapechar: 一个用于 writer 的单字符，用来在 quoting 设置为 QUOTE_NONE 的情况下转义 定界符
        :param doublequote: 控制出现在字段中的 引号字符 本身应如何被引出。当该属性为 True 时，双写引号字符。如果该属性为 False，则在 引号字符 的前面放置 转义符。默认值为 True。
        :param delimiter: 分隔符, 默认为 ,
        :param path: 文件路径
        :param header: 表头, 默认为自动读取, None 为不写表头, 也可以自己传一个列表
        :param mode: 写入模式
        :param kwargs: 打开文件的其他参数
        :return:
        """
        kwargs.setdefault('encoding', "utf-8")

        if header == "inner":
            header = list(sorted(self.columns))
            writer_header = True
        elif not header:
            header = list(sorted(self.columns))
            writer_header = False
        else:
            writer_header = True

        dirname = os.path.dirname(path)
        if dirname and not os.path.exists(dirname):
            os.makedirs(dirname)

        if not writer:
            with open(path, mode, **kwargs) as file:
                writer = csv.writer(
                    file,
                    delimiter=delimiter,
                    doublequote=doublequote,
                    escapechar=escapechar,
                    lineterminator=lineterminator,
                    quotechar=quotechar,
                    quoting=quoting,
                    skipinitialspace=skipinitialspace,
                    strict=strict,
                )
                writer_header and not os.path.getsize(path) and writer.writerow(header)
                writer.writerows(self._dict2list(header))

        else:
            writer_header and not os.path.getsize(path) and writer.writerow(header)
            writer.writerows(self._dict2list(header))

    def _dict2list(self, orders: list):
        return [[i.get(k, "") for k in orders] for i in self.data]

    def __delitem__(self, key):
        for item in self.data:
            item.__delitem__(key)

    def __setitem__(self, key, value):
        if not isinstance(value, GeneratorType):
            value = itertools.repeat(value)

        for item, v in zip(self.data, value):
            item.__setitem__(key, v)

    def __getitem__(self, item):
        if isinstance(item, int):
            return list.__getitem__(self.data, item)
        return self.__class__([{item: i.__getitem__(item)} for i in self.data])

    def __repr__(self):
        return repr(self.data)
