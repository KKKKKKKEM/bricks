"""显式接收 Context 的存储插件，写入成功后返回，不提前确认队列。"""

from __future__ import annotations

import csv
import hashlib
import sqlite3
from collections.abc import Sequence
from contextlib import nullcontext
from pathlib import Path
from typing import TYPE_CHECKING, Any, ContextManager

from bricks.queues.models import encode

from ._utils import identifier, integer, records

if TYPE_CHECKING:
    from bricks.spider.context import Context


def _keys(rows: list[dict[str, Any]], names: Sequence[str]) -> tuple[str, ...]:
    """校验幂等字段，拒绝缺失与空值造成意外覆盖。

    Args:
        rows: 独立外层记录。
        names: 作为业务身份的字段序列。

    Returns:
        已校验的字段元组。

    Raises:
        ValueError: 字段重复、缺失、为空或名称非法。
    """
    if isinstance(names, str) or any(
        not isinstance(name, str) or not name for name in names
    ):
        raise ValueError("row_keys must be a sequence of non-empty field names")
    keys = tuple(names)
    if len(set(keys)) != len(keys) or any(
        row.get(key) is None for row in rows for key in keys
    ):
        raise ValueError("row_keys must be unique, present and non-null")
    return keys


def to_sqlite(
    context: Context,
    *,
    path: str | Path,
    table: str,
    row_keys: Sequence[str] = (),
    timeout: float = 30,
) -> None:
    """每批创建独立 SQLite 连接并事务提交，适合 Air 工作线程调用。

    Args:
        context: 显式领域上下文，读取 items 而不修改。
        path: SQLite 文件路径；不接受跨调用丢失内容的 :memory:。
        table: 数据表名称；不存在时以本批字段创建，无隐式字段迁移。
        row_keys: 可选主键字段，提供后按唯一约束 upsert。
        timeout: SQLite 锁等待秒数，默认 30。

    Raises:
        ValueError: 路径、行字段或主键不合法。
        TypeError: SQLite 字段不是原生标量；嵌套结构应由业务显式编码。
        sqlite3.Error: 写入失败，整批事务回滚。
    """
    rows = records(context.items)
    keys = _keys(rows, row_keys)
    target = identifier(table)
    if str(path) == ":memory:":
        raise ValueError("to_sqlite requires a persistent file path")
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
    columns = ",".join(identifier(name) for name in names)
    definitions = columns
    if keys:
        definitions += (
            ", PRIMARY KEY (" + ",".join(identifier(key) for key in keys) + ")"
        )
    statement = (
        f"INSERT INTO {target} ({columns}) VALUES ({','.join('?' for _ in names)})"
    )
    if keys:
        updates = [name for name in names if name not in keys]
        statement += (
            " ON CONFLICT (" + ",".join(identifier(key) for key in keys) + ") DO "
        )
        statement += (
            "UPDATE SET "
            + ",".join(
                f"{identifier(name)}=excluded.{identifier(name)}" for name in updates
            )
            if updates
            else "NOTHING"
        )
    connection = sqlite3.connect(path, timeout=timeout)
    try:
        with connection:
            connection.execute("BEGIN")
            connection.execute(f"CREATE TABLE IF NOT EXISTS {target} ({definitions})")
            connection.executemany(
                statement, [tuple(row[name] for name in names) for row in rows]
            )
    finally:
        connection.close()


def to_csv(
    context: Context,
    *,
    path: str | Path,
    fieldnames: Sequence[str] | None = None,
    encoding: str = "utf-8-sig",
    lock: ContextManager[Any] | None = None,
    **writer_options: Any,
) -> None:
    """向 CSV 追加记录并检查已有表头，不建立隐藏的全局 Writer 缓存。

    Args:
        context: 显式领域上下文，读取 items。
        path: 文件路径；父目录由调用方准备。
        fieldnames: 稳定列顺序；None 使用第一行字段。
        encoding: 文本编码，默认兼容 Excel 的 UTF-8 BOM。
        lock: 所有写入同一文件的调用方共同使用的锁；None 仅适合单写入者。
        writer_options: csv.DictWriter 格式参数。

    Raises:
        ValueError: 字段或已有表头不匹配。
        OSError: 文件写入失败；CSV 不提供回滚或重投去重。
    """
    rows = records(context.items)
    if not rows:
        return
    names = list(rows[0] if fieldnames is None else fieldnames)
    if (
        not names
        or len(set(names)) != len(names)
        or any(type(name) is not str or not name for name in names)
    ):
        raise ValueError("CSV requires unique non-empty fieldnames")
    if any(set(row) != set(names) for row in rows):
        raise ValueError("CSV rows must match fieldnames")
    if "extrasaction" in writer_options:
        raise ValueError("CSV does not silently discard extra fields")
    with lock if lock is not None else nullcontext():
        with open(path, "a+", encoding=encoding, newline="") as stream:
            stream.seek(0)
            reader_options = {
                key: value
                for key, value in writer_options.items()
                if key not in {"restval", "lineterminator"}
            }
            existing = next(csv.reader(stream, **reader_options), None)
            if existing is not None and existing != names:
                raise ValueError("existing CSV header differs from fieldnames")
            stream.seek(0, 2)
            writer = csv.DictWriter(stream, names, **writer_options)
            if existing is None:
                writer.writeheader()
            writer.writerows(rows)


def to_mongo(
    context: Context,
    *,
    conn: Any,
    row_keys: Sequence[str] = (),
    ordered: bool = True,
) -> None:
    """写入显式 Mongo Collection，可按业务键 upsert，不创建客户端。

    Args:
        context: 显式领域上下文。
        conn: 同步 Collection，归调用方管理；业务键的唯一索引由调用方创建。
        row_keys: 非空时用这些字段定位并更新记录；默认 insert_many。
        ordered: insert_many 是否按序失败，默认 True。

    Raises:
        ValueError: 主键缺失或为空。
        Exception: 原生驱动错误原样传播；不声称多文档写入原子。
    """
    rows = records(context.items)
    keys = _keys(rows, row_keys)
    if not rows:
        return
    if not conn.write_concern.acknowledged:
        raise ValueError("Mongo storage requires acknowledged writes")
    if not keys:
        conn.insert_many(rows, ordered=ordered)
    else:
        for row in rows:
            conn.update_one({key: row[key] for key in keys}, {"$set": row}, upsert=True)


def to_redis(
    context: Context,
    *,
    path: str,
    conn: Any,
    key_type: str = "set",
    row_keys: Sequence[str] = (),
    ttl: int | None = None,
) -> None:
    """将严格 JSON 记录写入 Redis，不添加或弹出业务字段。

    Args:
        context: 显式领域上下文。
        path: set/list 的键；string 模式的键前缀。
        conn: 同步 redis-py 客户端，归调用方管理。
        key_type: set、list 或 string，默认 set。
        row_keys: string 必填，以规范 JSON 身份的摘要构造无分隔符冲突的键。
        ttl: 正整数存活秒数；None 不设置过期。

    Raises:
        ValueError: 类型、键名、主键或 TTL 非法。
        Exception: JSON 或 Redis 写入失败。事务不提供命令错误回滚。
    """
    if (
        key_type not in {"set", "list", "string"}
        or not isinstance(path, str)
        or not path
    ):
        raise ValueError("path and key_type must be valid")
    rows = records(context.items)
    keys = _keys(rows, row_keys)
    if key_type == "string" and not keys:
        raise ValueError("Redis string storage requires row_keys")
    if ttl is not None:
        integer(ttl, "ttl", minimum=1)
    encoded = [encode(row) for row in rows]
    if not rows:
        return
    with conn.pipeline(transaction=True) as pipeline:
        if key_type == "string":
            for row, value in zip(rows, encoded):
                identity = encode([row[key] for key in keys]).encode()
                name = path + ":" + hashlib.sha256(identity).hexdigest()
                pipeline.set(name, value, ex=ttl)
        else:
            if key_type == "set":
                pipeline.sadd(path, *encoded)
            else:
                pipeline.rpush(path, *encoded)
            if ttl is not None:
                pipeline.expire(path, ttl)
        pipeline.execute()
