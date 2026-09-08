"""直接查询已解码 JSON 数据，不隐式解析字符串或回退 Python 字面量。"""

from __future__ import annotations

import json
from typing import Any

import jmespath
import jsonpath

from .base import BaseParser, check_expression


class JmesPathParser(BaseParser):
    """使用 JMESPath 查询和投影，保留原生 null 与数组语义。"""

    def extract(self, source: Any, expression: str, **options: Any) -> Any:
        """查询已解码对象；源字符串是 JSON 字符串值而不是 JSON 文档文本。

        Args:
            source: 已解码 JSON 值，文本应由调用方先用 json.loads 解码。
            expression: 标准 JMESPath 表达式。
            **options: options 为原生 jmespath.Options，其余选项报错。

        Returns:
            原生查询值；语言将缺失和显式 null 均表示为 None。
        """
        check_expression(expression)
        return jmespath.search(expression, source, **options)


class JsonPathParser(BaseParser):
    """使用 python-jsonpath 的严格语法查询，结果始终是命中值列表。"""

    def prepare(self, source: Any) -> Any:
        """校验已解码 JSON 根值类型，不读取文件或隐式解码字节。

        Args:
            source: dict、list、str、int、float、bool 或 None。

        Returns:
            原始根值，不复制或递归校验容器内容。

        Raises:
            TypeError: 根值类型不支持或提供文件读取接口。
        """
        if (
            source is not None
            and not isinstance(source, (dict, list, str, int, float, bool))
        ) or hasattr(source, "read"):
            raise TypeError("JSONPath source must be a decoded JSON value")
        return source

    def extract(self, source: Any, expression: str, **options: Any) -> list[Any]:
        """查询已解码 JSON，缺失返回 []，命中的 null 保留为 [None]。

        Args:
            source: 已解码 JSON 值；字符串值保持字符串语义。
            expression: RFC 9535 JSONPath 表达式，必须包含根选择符。
            **options: 当前不接受附加选项，未知选项明确报错。

        Returns:
            按查询顺序返回命中值列表，单次命中的数组仍作为列表中的一项。

        Raises:
            TypeError: 输入根值类型或选项不支持。
        """
        check_expression(expression)
        if options:
            raise TypeError(f"unsupported JSONPath options: {', '.join(options)}")
        source = self.prepare(source)
        # 原生库自动解码字符串；显式编码一次，确保根字符串值不被误读。
        data = json.dumps(source) if isinstance(source, str) else source
        return jsonpath.findall(expression, data, strict=True)
