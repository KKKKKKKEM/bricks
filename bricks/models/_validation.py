"""爬虫模型共享的字段校验与规范化函数。"""

from __future__ import annotations

import math
import re
from collections.abc import Iterable, Iterator, Mapping
from types import MappingProxyType
from typing import Any
from urllib.parse import urlsplit

_TOKEN = re.compile(r"^[!#$%&'*+.^_`|~0-9A-Za-z-]+$")


def entries(
    values: Mapping[str, Any] | Iterable[tuple[str, Any]], label: str
) -> Iterator[tuple[Any, Any]]:
    """统一遍历映射或有序键值对，校验条目结构。

    Args:
        values: 字段映射或有序键值对迭代器。
        label: 校验失败时用于指明字段的说明名称。

    Yields:
        按输入顺序排列的键值对，不合并同名字段。

    Raises:
        TypeError: 参数类型或接口实现不符合当前契约。
    """

    if isinstance(values, Mapping):
        yield from values.items()
        return
    if isinstance(values, (str, bytes)):
        raise TypeError(f"{label} must be a mapping or an iterable of pairs")
    for pair in values:
        if not isinstance(pair, (tuple, list)) or len(pair) != 2:
            raise TypeError(f"{label} entries must be key/value pairs")
        yield pair[0], pair[1]


def token(value: str, label: str) -> str:
    """校验字段值符合 HTTP token 字符规则。

    Args:
        value: 待校验的 HTTP 字段名称或方法字符串。
        label: 校验失败时用于指明字段的说明名称。

    Returns:
        通过 HTTP token 校验的原始字符串。

    Raises:
        TypeError: 参数类型或接口实现不符合当前契约。
        ValueError: 参数值或字段组合不合法。
    """

    if not isinstance(value, str):
        raise TypeError(f"{label} must be a string")
    if not _TOKEN.fullmatch(value):
        raise ValueError(f"{label} must be an HTTP token")
    return value


def http_url(value: str) -> str:
    """校验绝对 HTTP 或 HTTPS URL，拒绝空白和控制字符。

    Args:
        value: 待校验的绝对 HTTP 或 HTTPS URL。

    Returns:
        通过校验的原始 URL 字符串。

    Raises:
        TypeError: 参数类型或接口实现不符合当前契约。
        ValueError: 参数值或字段组合不合法。
    """

    if not isinstance(value, str):
        raise TypeError("url must be a string")
    if any(char.isspace() or ord(char) < 32 or ord(char) == 127 for char in value):
        raise ValueError("url must not contain whitespace or control characters")
    parsed = urlsplit(value)
    if parsed.scheme.lower() not in ("http", "https") or not parsed.hostname:
        raise ValueError("url must be an absolute HTTP or HTTPS URL")
    parsed.port
    return value


def duration(
    value: float | None, label: str, *, allow_zero: bool = False
) -> float | None:
    """校验并规范化秒数，按参数决定是否允许零值。

    Args:
        value: 待校验的秒数，None 表示未指定时限。
        label: 校验失败时用于指明字段的说明名称。
        allow_zero: 是否允许零秒作为合法时长。

    Returns:
        规范化后的浮点秒数，或表示未设置的 None。

    Raises:
        TypeError: 参数类型或接口实现不符合当前契约。
        ValueError: 参数值或字段组合不合法。
    """

    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{label} must be a number or None")
    if not math.isfinite(value) or value < 0 or (value == 0 and not allow_zero):
        raise ValueError(
            f"{label} must be finite and {'non-negative' if allow_zero else 'positive'}"
        )
    return float(value)


def cookies(values: Mapping[str, str] | None) -> Mapping[str, str]:
    """校验 Cookie 名称和值，并返回独立的只读映射。

    Args:
        values: Cookie 名称和值的映射，None 表示空集合。

    Returns:
        独立、只读的 Cookie 名称到值映射。

    Raises:
        TypeError: 参数类型或接口实现不符合当前契约。
        ValueError: 参数值或字段组合不合法。
    """

    if values is None:
        return MappingProxyType({})
    if not isinstance(values, Mapping):
        raise TypeError("cookies must be a mapping")
    result: dict[str, str] = {}
    for name, value in values.items():
        token(name, "cookie name")
        if not isinstance(value, str):
            raise TypeError("cookie values must be strings")
        if any(char in value for char in "\r\n\x00"):
            raise ValueError("cookie values must not contain CR, LF or NUL")
        result[name] = value
    return MappingProxyType(result)


def request_cookies(values: Mapping[str, str] | None) -> Mapping[str, str]:
    """校验显式请求 Cookie，拒绝会改变请求头结构的字符。

    Args:
        values: 显式请求 Cookie 映射，不自动转义字段值。

    Returns:
        可安全序列化为请求 Cookie 的只读映射。

    Raises:
        ValueError: 参数值或字段组合不合法。
    """

    result = cookies(values)
    for value in result.values():
        if any(
            ord(char) < 0x21 or ord(char) > 0x7E or char in '",;\\' for char in value
        ):
            raise ValueError(
                "request cookie values must contain only HTTP cookie octets"
            )
    return result
