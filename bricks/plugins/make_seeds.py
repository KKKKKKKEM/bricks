"""CSV、SQLite、MongoDB、Redis 与整数范围种子源；不隐式取得 Context。"""

from __future__ import annotations

import csv
import glob
import hashlib
import itertools
import json
import sqlite3
from collections.abc import Callable, Generator, Iterable, Iterator, Mapping, Sequence
from pathlib import Path
from typing import Any

from bricks.queues.models import encode

from ._utils import identifier, integer, seed, sync


def _skip(value: Any) -> int | None:
    """把 dev 的省略值和 auto 恢复写法映射到当前断点语义。

    Args:
        value: 非负整数、None、省略值或字符串 auto。

    Returns:
        None 表示从 record 恢复，否则为显式跳过条数。

    Raises:
        ValueError: 跳过位置非法。
    """
    if value is ... or value is None or value == "auto":
        return None
    return integer(value, "skip")


def _batch(
    source: Iterable[Any],
    *,
    batch_size: int,
    skip: int | None,
    record: dict[str, Any] | None,
    checkpoint: str,
    transform: Callable[[Any], dict[str, Any]] | None = None,
) -> Iterator[list[dict[str, Any]]]:
    """按已投放位置恢复，校验整批后在产出前更新调用方记录。

    Args:
        source: 调用方提供的源迭代器。
        batch_size: 每批最大条数。
        skip: 显式跳过条数；None 使用恢复记录。
        record: Air 初始化持有的可变 JSON 记录；None 不记录。
        checkpoint: 当前源独立的记录键。
        transform: 可选同步记录转换，用于显式处理 ObjectId 等类型。

    Yields:
        严格 JSON 种子批次。Air 在整批入队后提交该位置。

    Raises:
        ValueError: 批量、跳过位置或记录键非法。
        TypeError: 记录或源内容不符合契约。
    """
    integer(batch_size, "batch_size", minimum=1)
    if record is not None and type(record) is not dict:
        raise TypeError("record must be a dict or None")
    if not isinstance(checkpoint, str) or not checkpoint:
        raise ValueError("checkpoint must be a non-empty string")
    position = integer(
        (0 if record is None else record.get(checkpoint, 0)) if skip is None else skip,
        "skip",
    )
    iterator = iter(source)
    for _ in itertools.islice(iterator, position):
        pass
    while True:
        values = list(itertools.islice(iterator, batch_size))
        if not values:
            return
        rows = [
            seed(value if transform is None else sync(transform, value))
            for value in values
        ]
        position += len(rows)
        if record is not None:
            record[checkpoint] = position
        yield rows


def _key(kind: str, value: Any) -> str:
    """根据稳定源配置生成默认恢复键。

    Args:
        kind: 源类别。
        value: JSON 源配置。

    Returns:
        类别和内容摘要。
    """
    return kind + ":" + hashlib.sha256(encode(value).encode()).hexdigest()


def by_range(
    start: int = 0,
    stop: int | None = None,
    step: int = 1,
    batch_size: int = 10000,
    skip: Any = ...,
    record: dict[str, Any] | None = None,
    key: str = "id",
    reverse: bool = False,
    *,
    checkpoint: str | None = None,
) -> Iterator[list[dict[str, Any]]]:
    """生成整数种子，有限范围严格使用 Python range 的方向与边界。

    Args:
        start: 包含的起始整数，默认零。
        stop: 不包含的终点；None 持续生成。
        step: 非零整数步长，默认一。
        key: 种子中的整数键名。
        reverse: 对有限范围反向遍历，默认 False。
        batch_size: 每批最大条数。
        skip: 显式跳过条数；省略值、auto 或 None 从 record 恢复。
        record: 调用方持有的恢复记录。
        checkpoint: 自定义记录键；缺省根据范围生成。

    Yields:
        整数种子批次。

    Raises:
        ValueError: 参数类型、步长或反向无界范围非法。
    """
    if type(start) is not int or (stop is not None and type(stop) is not int):
        raise ValueError("range boundaries must be integers")
    if type(step) is not int or step == 0 or type(reverse) is not bool:
        raise ValueError("step must be a nonzero integer and reverse must be bool")
    if not isinstance(key, str) or not key or (reverse and stop is None):
        raise ValueError("key must be non-empty; reverse requires stop")
    values: Iterable[int] = (
        itertools.count(start, step) if stop is None else range(start, stop, step)
    )
    if reverse:
        values = reversed(range(start, stop, step))  # type: ignore[arg-type]
    yield from _batch(
        ({key: value} for value in values),
        batch_size=batch_size,
        skip=_skip(skip),
        record=record,
        checkpoint=checkpoint or _key("range", [start, stop, step, key, reverse]),
    )


def by_csv(
    path: str | Path,
    query: str | None = None,
    batch_size: int = 10000,
    skip: Any = ...,
    reader_options: Mapping[str, Any] | None = None,
    record: dict[str, Any] | None = None,
    *,
    encoding: str = "utf-8-sig",
    parameters: Sequence[Any] = (),
    checkpoint: str | None = None,
    transform: Callable[[dict[str, Any]], dict[str, Any]] | None = None,
) -> Iterator[list[dict[str, Any]]]:
    """按排序后的文件路径读取有表头 CSV，可显式查询临时 SQLite 的 data 表。

    Args:
        path: 文件路径或 glob 表达式。
        encoding: 文本编码，默认兼容 UTF-8 BOM。
        reader_options: csv.DictReader 参数。
        query: 可选 SQL；有查询时将每个文件装入临时 data 表，字段为 TEXT。
        parameters: SQL 值参数。
        batch_size: 每批最大条数。
        skip: 显式跳过结果条数；省略值、auto 或 None 恢复。
        record: 调用方恢复记录；源文件和顺序必须稳定。
        checkpoint: 独立记录键。
        transform: 可选同步字段转换。

    Yields:
        已校验的 JSON 记录批次。

    Raises:
        FileNotFoundError: 没有匹配文件。
        ValueError: CSV 表头或行宽不合法。
    """
    files = sorted(glob.glob(str(path)))
    if not files:
        raise FileNotFoundError(path)
    options = dict(reader_options or {})
    selected_query = None if query is None else query.replace("<TABLE>", "data")

    def rows() -> Generator[dict[str, Any], None, None]:
        """逐文件读取并在退出或生成器关闭时释放文件与临时数据库。

        Yields:
            查询结果或原始 CSV 记录。
        """
        for filename in files:
            with open(filename, encoding=encoding, newline="") as stream:
                reader = csv.DictReader(stream, **options)
                fields = reader.fieldnames
                if (
                    not fields
                    or len(set(fields)) != len(fields)
                    or any(not name for name in fields)
                ):
                    raise ValueError("CSV requires unique non-empty headers")
                if selected_query is None:
                    for row in reader:
                        if None in row or any(value is None for value in row.values()):
                            raise ValueError("CSV row width differs from header")
                        yield dict(row)
                else:
                    # 生成器由串行图节点推进，但不同 firing 不保证同一操作系统线程。
                    connection = sqlite3.connect(":memory:", check_same_thread=False)
                    try:
                        connection.execute(
                            "CREATE TABLE data ("
                            + ",".join(identifier(name) + " TEXT" for name in fields)
                            + ")"
                        )
                        statement = (
                            "INSERT INTO data VALUES ("
                            + ",".join("?" for _ in fields)
                            + ")"
                        )
                        for row in reader:
                            if None in row or any(
                                value is None for value in row.values()
                            ):
                                raise ValueError("CSV row width differs from header")
                            connection.execute(
                                statement, tuple(row[name] for name in fields)
                            )
                        yield from _sql_rows(connection, selected_query, parameters)
                    finally:
                        connection.close()

    source = rows()
    try:
        yield from _batch(
            source,
            batch_size=batch_size,
            skip=_skip(skip),
            record=record,
            checkpoint=checkpoint
            or _key("csv", [files, selected_query, list(parameters)]),
            transform=transform,
        )
    finally:
        source.close()


def _sql_rows(
    conn: sqlite3.Connection, query: str, parameters: Sequence[Any]
) -> Generator[dict[str, Any], None, None]:
    """执行原始查询并逐行映射，不改写已有 LIMIT/OFFSET。

    Args:
        conn: 调用方连接，不关闭、不提交。
        query: 完整 SQL 查询。
        parameters: 绑定参数。

    Yields:
        独立的字段字典。

    Raises:
        ValueError: 查询没有结果或包含重复列名。
    """
    cursor = conn.execute(query, parameters)
    try:
        if cursor.description is None:
            raise ValueError("seed query must return rows")
        names = [field[0] for field in cursor.description]
        if len(set(names)) != len(names):
            raise ValueError("query columns must have unique names")
        for values in cursor:
            yield dict(zip(names, values))
    finally:
        cursor.close()


def by_sqlite(
    path: str,
    conn: sqlite3.Connection,
    batch_size: int = 10000,
    skip: Any = ...,
    record: dict[str, Any] | None = None,
    *,
    parameters: Sequence[Any] = (),
    checkpoint: str | None = None,
    transform: Callable[[dict[str, Any]], dict[str, Any]] | None = None,
) -> Iterator[list[dict[str, Any]]]:
    """从原生 SQLite 连接读取种子；查询排序和源快照由调用方保证。

    Args:
        path: 完整 SQL 查询。
        conn: 当前线程可用的原生连接，归调用方管理。
        parameters: 查询参数。
        batch_size: 每批最大条数。
        skip: 显式跳过条数；省略值、auto 或 None 恢复。
        record: 调用方恢复记录。
        checkpoint: 记录键；不同数据库使用同一查询时应显式区分。
        transform: 可选同步字段转换。

    Yields:
        严格 JSON 种子批次。
    """
    source = _sql_rows(conn, path, parameters)
    try:
        yield from _batch(
            source,
            batch_size=batch_size,
            skip=_skip(skip),
            record=record,
            checkpoint=checkpoint or _key("sqlite", [path, list(parameters)]),
            transform=transform,
        )
    finally:
        source.close()


def by_mongo(
    path: str,
    conn: Any,
    query: Mapping[str, Any] | None = None,
    projection: Mapping[str, Any] | None = None,
    database: str | None = None,
    batch_size: int = 10000,
    skip: Any = ...,
    record: dict[str, Any] | None = None,
    sort: Sequence[tuple[str, int]] | None = None,
    *,
    checkpoint: str | None = None,
    transform: Callable[[dict[str, Any]], dict[str, Any]] | None = None,
) -> Iterator[list[dict[str, Any]]]:
    """从显式 Mongo Collection 读取种子，不导入或创建 Mongo 客户端。

    Args:
        path: Collection 名称。
        conn: 同步 Collection、Database 或 MongoClient；资源归调用方管理。
        query: 原生查询条件。
        projection: 字段投影；默认排除 _id，其他非 JSON 字段需 transform。
        database: conn 为 MongoClient 时选择的数据库。
        sort: 恢复时应提供包含唯一键的稳定排序。
        batch_size: 每批最大条数。
        skip: 显式跳过条数；省略值、auto 或 None 恢复。
        record: 调用方恢复记录。
        checkpoint: 当前查询独立的记录键；缺省根据源配置生成。
        transform: 显式转换 ObjectId、日期等类型。

    Yields:
        严格 JSON 种子批次。
    """
    if hasattr(conn, "find"):
        collection = conn
        if getattr(collection, "name", path) != path:
            raise ValueError("path must match the injected Mongo Collection name")
    elif database is None:
        collection = conn[path]
    else:
        collection = conn[database][path]
    cursor = collection.find(
        dict(query or {}), {"_id": 0} if projection is None else dict(projection)
    )
    try:
        if sort:
            cursor = cursor.sort(list(sort))
        cursor = cursor.batch_size(integer(batch_size, "batch_size", minimum=1))
        yield from _batch(
            cursor,
            batch_size=batch_size,
            skip=_skip(skip),
            record=record,
            checkpoint=checkpoint
            or _key(
                "mongo",
                [
                    path,
                    query,
                    projection,
                    database,
                    [list(item) for item in sort or ()],
                ],
            ),
            transform=transform,
        )
    finally:
        cursor.close()


def by_redis(
    path: str,
    conn: Any,
    batch_size: int = 10000,
    key_type: str = "set",
) -> Iterator[list[dict[str, Any]]]:
    """无损读取 Redis 集合、列表或匹配的字符串键，显式 JSON 解码。

    Args:
        path: 集合或列表键；string 模式为 SCAN 匹配表达式。
        conn: 同步 redis-py 客户端，归调用方管理。
        key_type: set、list 或 string，默认 set。
        batch_size: 每批最大条数及扫描提示数量。

    Yields:
        JSON 种子批次。SCAN 可能重复，交给稳定任务 ID 去重；不承诺断点恢复。

    Raises:
        ValueError: 模式非法或读取内容不是 JSON。
        TypeError: Redis 键类型或种子类型不符。
    """
    integer(batch_size, "batch_size", minimum=1)
    if key_type not in {"set", "list", "string"}:
        raise ValueError("key_type must be set, list or string")
    if key_type != "string":
        actual = conn.type(path)
        if isinstance(actual, bytes):
            actual = actual.decode("ascii")
        if actual not in {"none", key_type}:
            raise TypeError("Redis key type does not match seed source")

    def values() -> Iterator[Any]:
        """有界读取 Redis 数据，不删除源值。

        Yields:
            已解码的 JSON 值。
        """
        if key_type == "set":
            raw = conn.sscan_iter(path, count=batch_size)
            for value in raw:
                yield json.loads(value)
        elif key_type == "string":
            for name in conn.scan_iter(match=path, count=batch_size, _type="string"):
                value = conn.get(name)
                if value is not None:
                    yield json.loads(value)
        else:
            position = 0
            while True:
                raw = conn.lrange(path, position, position + batch_size - 1)
                if not raw:
                    return
                for value in raw:
                    yield json.loads(value)
                position += len(raw)

    yield from _batch(
        values(), batch_size=batch_size, skip=0, record=None, checkpoint="redis"
    )
