"""参数贴近 dev、使用当前 Context 与原生客户端的存储插件。"""

from __future__ import annotations

import csv
import hashlib
import sqlite3
from collections.abc import Iterable, Mapping, Sequence
from contextlib import nullcontext
from pathlib import Path
from typing import Any, ContextManager

from bricks import hooks
from bricks.queues.models import encode
from bricks.spider.context import Context

from ._utils import identifier, integer, records


def _rows(items: Iterable[Mapping[str, Any]] | None) -> list[dict[str, Any]]:
    """取得显式 Items，缺省时读取当前 Context。

    Args:
        items: 显式记录；None 使用 Context.get_context().items。

    Returns:
        不修改来源对象的独立外层字典列表。
    """
    source = Context.get_context().items if items is None else items
    return records(source)


def _keys(rows: list[dict[str, Any]], names: Sequence[str] | None) -> tuple[str, ...]:
    """校验业务身份字段。

    Args:
        rows: 当前写入记录。
        names: 可选字段序列。

    Returns:
        已校验的字段元组。

    Raises:
        ValueError: 字段重复、缺失、为空或名称非法。
    """
    values: Sequence[str] = () if names is None else names
    if isinstance(values, str) or any(
        not isinstance(name, str) or not name for name in values
    ):
        raise ValueError("row_keys must be a sequence of non-empty field names")
    keys = tuple(values)
    if len(set(keys)) != len(keys) or any(
        row.get(key) is None for row in rows for key in keys
    ):
        raise ValueError("row_keys must be unique, present and non-null")
    return keys


def _sqlite_write(
    connection: sqlite3.Connection,
    table: str,
    rows: list[dict[str, Any]],
    keys: tuple[str, ...],
    *,
    with_lock: bool,
) -> None:
    """在一个原生 SQLite 事务中写入当前批次。

    Args:
        connection: 当前调用拥有或借用的连接。
        table: 数据表名称。
        rows: 字段集合一致的记录。
        keys: 可选业务主键。
        with_lock: True 使用 BEGIN IMMEDIATE 提前取得写锁。

    Raises:
        ValueError: 行字段不一致。
        TypeError: 字段不是 SQLite 原生标量。
        sqlite3.Error: 事务失败。
    """
    if not rows:
        return
    names = tuple(rows[0])
    if not names or any(set(row) != set(names) for row in rows):
        raise ValueError("SQLite rows must have the same non-empty field set")
    if any(
        value is not None and not isinstance(value, (str, int, float, bytes))
        for row in rows
        for value in row.values()
    ):
        raise TypeError(
            "SQLite fields must be scalar values; encode nested data explicitly"
        )
    target = identifier(table)
    columns = ",".join(identifier(name) for name in names)
    definitions = columns
    if keys:
        definitions += ", PRIMARY KEY (" + ",".join(map(identifier, keys)) + ")"
    statement = (
        f"INSERT INTO {target} ({columns}) VALUES ({','.join('?' for _ in names)})"
    )
    if keys:
        updates = [name for name in names if name not in keys]
        statement += " ON CONFLICT (" + ",".join(map(identifier, keys)) + ") DO "
        statement += (
            "UPDATE SET "
            + ",".join(
                f"{identifier(name)}=excluded.{identifier(name)}" for name in updates
            )
            if updates
            else "NOTHING"
        )
    with connection:
        connection.execute("BEGIN IMMEDIATE" if with_lock else "BEGIN")
        connection.execute(f"CREATE TABLE IF NOT EXISTS {target} ({definitions})")
        connection.executemany(
            statement, [tuple(row[name] for name in names) for row in rows]
        )


@hooks.current
def to_sqlite(
    path: str,
    conn: str | Path | sqlite3.Connection,
    items: Iterable[Mapping[str, Any]] | None = None,
    row_keys: Sequence[str] | None = None,
    with_lock: bool = True,
    *,
    timeout: float = 30,
) -> None:
    """写入 SQLite，沿用 dev 的 path/conn/items/row_keys 参数角色。

    Args:
        path: 数据表名称。
        conn: 数据库文件路径或调用方管理的原生 Connection。
        items: 显式记录；作为插件使用时省略并读取当前 Context。
        row_keys: 可选业务主键，用于 upsert。
        with_lock: 是否在写入前取得 SQLite 写锁，默认 True。
        timeout: 本函数创建连接时的锁等待秒数。

    Raises:
        ValueError: 表、数据库目标、字段或主键不合法。
        sqlite3.Error: 写入失败并回滚当前批次。
    """
    if type(with_lock) is not bool:
        raise TypeError("with_lock must be boolean")
    rows = _rows(items)
    keys = _keys(rows, row_keys)
    if isinstance(conn, sqlite3.Connection):
        _sqlite_write(conn, path, rows, keys, with_lock=with_lock)
        return
    if str(conn) == ":memory:":
        raise ValueError("to_sqlite requires a persistent path or Connection")
    connection = sqlite3.connect(conn, timeout=timeout)
    try:
        _sqlite_write(connection, path, rows, keys, with_lock=with_lock)
    finally:
        connection.close()


@hooks.current
def to_mongo(
    path: str,
    conn: Any,
    items: Iterable[Mapping[str, Any]] | None = None,
    row_keys: Sequence[str] | None = None,
    database: str | None = None,
    **kwargs: Any,
) -> None:
    """写入 MongoDB，支持 Collection、Database 或 Client 装配。

    Args:
        path: Collection 名称；直接传 Collection 时用于校验目标。
        conn: 原生同步 Collection、Database 或 MongoClient。
        items: 显式记录；None 读取当前 Context。
        row_keys: 可选业务身份字段；Mongo 仅接受不含点号且不以 `$` 开头的字段名。
        database: conn 为 MongoClient 时选择的数据库。
        kwargs: insert_many 或 update_one 的原生参数。

    Raises:
        ValueError: Collection、数据库、写关注或身份字段不合法。
        Exception: 原生驱动错误原样传播。
    """
    rows = _rows(items)
    keys = _keys(rows, row_keys)
    if any("." in key or key.startswith("$") for key in keys):
        raise ValueError(
            "Mongo row_keys must be plain field names without dots or dollar prefixes"
        )
    if not rows:
        return
    if hasattr(conn, "insert_many") and hasattr(conn, "update_one"):
        collection = conn
        if getattr(collection, "name", path) != path:
            raise ValueError("path must match the injected Mongo Collection name")
    elif database is None:
        collection = conn[path]
    else:
        collection = conn[database][path]
    if not collection.write_concern.acknowledged:
        raise ValueError("Mongo storage requires acknowledged writes")
    if not keys:
        collection.insert_many(rows, **kwargs)
    else:
        kwargs.pop("ordered", None)
        for row in rows:
            identity_filter = {key: {"$eq": row[key]} for key in keys}
            collection.update_one(identity_filter, {"$set": row}, upsert=True, **kwargs)


@hooks.current
def to_csv(
    path: str | Path,
    items: Iterable[Mapping[str, Any]] | None = None,
    conn: csv.DictWriter | None = None,
    encoding: str = "utf-8-sig",
    *,
    header: Sequence[str] | None = None,
    lock: ContextManager[Any] | None = None,
    **kwargs: Any,
) -> None:
    """追加 CSV，沿用 dev 的 path/items/conn/encoding 参数顺序。

    Args:
        path: CSV 文件路径；传入 conn 时仅作为业务目标说明。
        items: 显式记录；None 读取当前 Context。
        conn: 可选调用方管理的 csv.DictWriter。
        encoding: 本函数打开文件时使用的编码。
        header: 稳定列顺序；None 使用第一条记录字段。
        lock: 多调用方写同一文件时共享的上下文锁。
        kwargs: csv.DictWriter 格式参数。

    Raises:
        ValueError: 字段、表头或 writer 参数不合法。
        OSError: 文件写入失败。
    """
    rows = _rows(items)
    if not rows:
        return
    names = list(rows[0] if header is None else header)
    if (
        not names
        or len(set(names)) != len(names)
        or any(type(name) is not str or not name for name in names)
        or any(set(row) != set(names) for row in rows)
    ):
        raise ValueError("CSV rows must match unique non-empty header fields")
    if conn is not None:
        conn.writerows(rows)
        return
    if "extrasaction" in kwargs:
        raise ValueError("CSV does not silently discard extra fields")
    with lock if lock is not None else nullcontext():
        with open(path, "a+", encoding=encoding, newline="") as stream:
            stream.seek(0)
            reader_options = {
                key: value
                for key, value in kwargs.items()
                if key not in {"restval", "lineterminator"}
            }
            existing = next(csv.reader(stream, **reader_options), None)
            if existing is not None and existing != names:
                raise ValueError("existing CSV header differs from header")
            stream.seek(0, 2)
            writer = csv.DictWriter(stream, names, **kwargs)
            if existing is None:
                writer.writeheader()
            writer.writerows(rows)


@hooks.current
def to_redis(
    path: str,
    conn: Any,
    items: Iterable[Mapping[str, Any]] | None = None,
    key_type: str = "set",
    row_keys: Sequence[str] | None = None,
    splice: str = "|",
    ttl: int = 0,
) -> None:
    """写入 Redis，沿用 dev 参数顺序且不修改业务记录。

    Args:
        path: set/list 键或 string 模式的键前缀。
        conn: 调用方管理的同步 redis-py 客户端。
        items: 显式记录；None 读取当前 Context。
        key_type: set、list 或 string，默认 set。
        row_keys: string 模式必填的业务身份字段。
        splice: string 键前缀与安全摘要之间的分隔符。
        ttl: 非负过期秒数；零表示不过期。

    Raises:
        ValueError: 类型、键、分隔符、主键或 TTL 非法。
        Exception: JSON 或 Redis 写入失败。
    """
    if key_type not in {"set", "list", "string"} or not path:
        raise ValueError("path and key_type must be valid")
    if not isinstance(splice, str):
        raise ValueError("splice must be a string")
    integer(ttl, "ttl")
    rows = _rows(items)
    keys = _keys(rows, row_keys)
    if key_type == "string" and not keys:
        raise ValueError("Redis string storage requires row_keys")
    if not rows:
        return
    encoded = [encode(row) for row in rows]
    with conn.pipeline(transaction=True) as pipeline:
        if key_type == "string":
            for row, value in zip(rows, encoded):
                identity = encode([row[key] for key in keys]).encode()
                name = path + splice + hashlib.sha256(identity).hexdigest()
                pipeline.set(name, value, ex=ttl or None)
        else:
            if key_type == "set":
                pipeline.sadd(path, *encoded)
            else:
                pipeline.rpush(path, *encoded)
            if ttl:
                pipeline.expire(path, ttl)
        pipeline.execute()


__all__ = ["to_csv", "to_mongo", "to_redis", "to_sqlite"]
