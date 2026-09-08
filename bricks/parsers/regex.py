"""基于标准库 re 的文本提取，捕获组输出由调用方显式选择。"""

from __future__ import annotations

import re
from typing import Any

from .base import BaseParser, check_expression


class RegexParser(BaseParser):
    """仅处理文本，不隐式序列化字典、HTML 元素或字节。"""

    def prepare(self, source: Any) -> str:
        """校验输入是已解码文本。

        Args:
            source: 已解码文本。

        Returns:
            原文本，不复制或缓存。

        Raises:
            TypeError: 输入不是字符串。
        """
        if not isinstance(source, str):
            raise TypeError("regex source must be a string")
        return source

    def extract(self, source: Any, expression: str, **options: Any) -> list[Any]:
        """以 finditer 收集匹配，不根据捕获组数量隐式改变返回形状。

        Args:
            source: 已解码文本。
            expression: 标准库 re 表达式。
            **options: flags 默认 0；group 默认 0 为完整匹配，整数或名称选择
                单个捕获组，None 返回命名捕获组字典。

        Returns:
            匹配值列表，未参与匹配的可选组保留 None。

        Raises:
            TypeError: 输入、捕获组或选项类型错误。
            IndexError: 捕获组不存在，即使没有匹配也会校验。
            re.error: 正则表达式不合法。
        """
        check_expression(expression)
        return self._select(self.prepare(source), expression, **options)

    @staticmethod
    def _select(
        source: str,
        expression: str,
        *,
        flags: int = 0,
        group: int | str | None = 0,
    ) -> list[Any]:
        """编译表达式、校验捕获组并按相同规则收集全部命中。

        Args:
            source: 待查询文本。
            expression: 正则表达式。
            flags: re 编译标志，默认 0。
            group: 捕获组索引或名称，None 表示命名组字典。

        Returns:
            匹配值或命名组字典列表。

        Raises:
            TypeError: group 类型不受支持。
            IndexError: 捕获组不存在。
        """
        query = re.compile(expression, flags)
        if group is not None:
            if type(group) is int:
                if not 0 <= group <= query.groups:
                    raise IndexError(f"no such group: {group}")
            elif isinstance(group, str):
                if group not in query.groupindex:
                    raise IndexError(f"no such group: {group}")
            else:
                raise TypeError("group must be an integer, a name, or None")
        return [
            matched.groupdict() if group is None else matched.group(group)
            for matched in query.finditer(source)
        ]
