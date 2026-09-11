"""领域插件共用的显式校验与同步回调调用。"""

from __future__ import annotations

import inspect
import json
from collections.abc import Callable, Iterable, Mapping
from typing import Any

from bricks.queues.models import encode


def integer(value: int, name: str, *, minimum: int = 0) -> int:
    """校验整数范围，不转换字符串或布尔值。

    Args:
        value: 待校验的整数。
        name: 报错字段名。
        minimum: 最小允许值，默认零。

    Returns:
        原整数。

    Raises:
        ValueError: 类型或范围不合法。
    """
    if type(value) is not int or value < minimum:
        raise ValueError(f"{name} must be an integer >= {minimum}")
    return value


def sync(function: Callable[..., Any], *args: Any) -> Any:
    """执行同步回调，拒绝异步结果并释放未等待的协程。

    Args:
        function: 明确传入的回调。
        args: 位置参数。

    Returns:
        回调的普通结果。

    Raises:
        TypeError: 回调返回异步对象。
    """
    value = function(*args)
    if inspect.isawaitable(value) or inspect.isasyncgen(value):
        if inspect.iscoroutine(value):
            value.close()
        raise TypeError("plugin callback must be synchronous")
    return value


def condition(function: Callable[[Any], bool], context: Any) -> bool:
    """执行只接受布尔结果的条件。

    Args:
        function: 同步条件函数。
        context: 显式领域上下文。

    Returns:
        条件结果。

    Raises:
        TypeError: 返回值不是布尔值。
    """
    result = sync(function, context)
    if type(result) is not bool:
        raise TypeError("plugin condition must return bool")
    return result


def records(values: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """复制记录的外层映射，防止存储适配修改业务字段。

    Args:
        values: 明确的记录集合。

    Returns:
        独立外层字典列表，内部字段值归调用方管理。

    Raises:
        TypeError: 成员不是字符串键映射。
    """
    result = []
    for value in values:
        if not isinstance(value, Mapping) or any(type(key) is not str for key in value):
            raise TypeError("records must be mappings with string keys")
        result.append(dict(value))
    return result


def seed(value: Any) -> dict[str, Any]:
    """建立符合队列契约的独立 JSON 种子。

    Args:
        value: 待投放的对象。

    Returns:
        深复制的 JSON 对象。

    Raises:
        TypeError: 内容不是严格 JSON 对象。
    """
    if type(value) is not dict:
        raise TypeError("seed must be a JSON object")
    return json.loads(encode(value))


def identifier(value: str) -> str:
    """引用 SQLite 标识符，避免把表名和字段名拼成 SQL 语句。

    Args:
        value: 原标识符。

    Returns:
        双引号引用的标识符。

    Raises:
        ValueError: 标识符为空或包含零字符。
    """
    if not isinstance(value, str) or not value or "\0" in value:
        raise ValueError("SQL identifier must be a non-empty string without NUL")
    return '"' + value.replace('"', '""') + '"'
