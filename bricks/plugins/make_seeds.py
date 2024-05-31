# -*- coding: utf-8 -*-
"""
@File    : make_seeds.py
@Date    : 2023-12-06 16:39
@Author  : yintian
@Desc    : 生产种子插件
"""
import re
from typing import Optional, Union, List, Literal
from urllib.parse import urlencode

from bricks.db.redis_ import Redis
from bricks.db.sqlite import Sqlite
from bricks.utils.csv_ import Reader
from bricks.utils.pandora import json_or_eval

LIMIT_PATTERN = re.compile(r"(LIMIT\s+)(\d+)", flags=re.IGNORECASE)
OFFSET_PATTERN = re.compile(r"(OFFSET\s+)(\d+)", flags=re.IGNORECASE)


def _make_key(query: dict):
    return urlencode(query)


def by_csv(
        path: str,
        query: str = None,
        batch_size: int = 10000,
        skip: Union[str, int] = ...,
        reader_options: Optional[dict] = None,
        record: Optional[dict] = None,
):
    """
    从 `CSV` 中获取种子, `csv` 必须有表头，可以使用 `SQL` 进行数据查询

    :param path: 文件路径
    :param query: 查询 `sql`， 使用 `<TABLE>` 关键字来代替当前表名
    :param batch_size: 一次获取多少条数据
    :param skip: 跳过初始多少种子
    :param reader_options: 初始化 csv reader 的其他参数
    :param record:
    :return:
    """
    record = record or {}
    reader_options = reader_options or {}
    if skip is ...:
        if record:
            skip = 'auto'
        else:
            skip = 0

    raw_skip = skip
    query = query or "select * from <TABLE>"

    for file in Reader.get_files(path):
        reader = Reader(file, **reader_options)

        record_key = _make_key({
            "path": path,
            "file": file,
            "query": query,
            "skip": raw_skip,
        })
        if raw_skip == 'auto':
            skip = int(record.get(record_key, 0))
        else:
            skip = raw_skip

        if skip != 0:
            # 原来就有 offset
            if OFFSET_PATTERN.search(query):
                def add_skip(match):
                    # 将捕获的数字转换为整数，加上 skip，然后格式化回字符串
                    return f"{match.group(1)}{int(match.group(2)) + skip}"

                query = OFFSET_PATTERN.sub(add_skip, query)

            # 没有 offset 但是有 limit
            elif not LIMIT_PATTERN.search(query):
                query = query + f" LIMIT -1 OFFSET {skip}"

            # offset 和 limit 都没有
            else:
                query = LIMIT_PATTERN.sub(r"\1\2 OFFSET " + str(skip), query)

        for rows in reader.iter_data(query, batch_size=batch_size):
            skip += len(rows)
            record.update({record_key: skip})
            yield rows


def by_mongo(
        path: str,
        conn,
        query: Optional[dict] = None,
        projection: Optional[dict] = None,
        database: str = None,
        batch_size: int = 10000,
        skip: Union[str, int] = ...,
        record: Optional[dict] = None,
        sort: Optional[List[tuple]] = None,
):
    """
    从 `Mongo` 中加载数据作为种子

    :param projection: 过滤字段
    :param path: 文件路径
    :param conn:`Mongo` 连接
    :param query: 查询 `Query`， 使用的 `Mongo` 查询语法，是一个字典
    :param database: `Mongo` 的数据库名称
    :param batch_size: 一次获取多少条数据
    :param skip: 跳过初始多少种子
    :param record: 历史记录，记录投放状态的容器
    :param sort: 排序方式
    :return:
    """
    from bricks.db.mongo import Mongo
    conn: Mongo
    record = record or {}
    if skip is ...:
        if record:
            skip = 'auto'
        else:
            skip = 0

    raw_skip = skip
    record_key = _make_key({
        "path": path,
        "conn": conn,
        "query": query,
        "skip": raw_skip,
        "database": database,
        "sort": sort,
    })
    if raw_skip == 'auto':
        skip = int(record.get(record_key, 0))
    else:
        skip = raw_skip

    for rows in conn.iter_data(collection=path, query=query, skip=skip, count=batch_size, database=database, sort=sort):
        skip += len(rows)
        record.update({record_key: skip})
        yield rows


def by_sqlite(
        path: str,
        conn: Sqlite,
        batch_size: int = 10000,
        skip: Union[str, int] = ...,
        record: Optional[dict] = None,
):
    """
    通过 sqlite 初始化

    :param path: 查询 sql
    :param conn: Sqlite 连接
    :param batch_size: 一次获取多少条
    :param skip: 要跳过多少条
    :param record: 历史记录, 用于断点续投
    :return:
    """
    record = record or {}
    if skip is ...:
        if record:
            skip = 'auto'
        else:
            skip = 0

    raw_skip = skip
    record_key = _make_key({
        "path": path,
        "skip": raw_skip,
    })
    if raw_skip == 'auto':
        skip = int(record.get(record_key, 0))
    else:
        skip = raw_skip
    if skip != 0:
        # 原来就有 offset
        if OFFSET_PATTERN.search(path):
            def add_skip(match):
                # 将捕获的数字转换为整数，加上 skip，然后格式化回字符串
                return f"{match.group(1)}{int(match.group(2)) + skip}"

            path = OFFSET_PATTERN.sub(add_skip, path)

        # 没有 offset 但是有 limit
        elif not LIMIT_PATTERN.search(path):
            path = path + f"LIMIT -1 OFFSET {skip}"

        # offset 和 limit 都没有
        else:
            path = LIMIT_PATTERN.sub(r"\1\2 OFFSET " + str(skip), path)

    for rows in conn.find(sql=path, batch_size=batch_size):
        skip += len(rows)
        record.update({record_key: skip})
        yield rows


def by_redis(
        path: str,
        conn: Redis,
        batch_size: int = 10000,
        key_type: Literal["set", "list", "string"] = 'set',
):
    """
    通过 `redis` 初始化种子, 仅支持小批量数据, 不支持断点续投

    :param path: 键值, 当 `key_type` 为 `string` 时, 此参数为筛选条件, 如 `*test*`
    :param conn: `Redis` 实例
    :param batch_size: 一次获取多少条数据
    :param key_type: 存储类型
    :return:
    """
    assert key_type in ["set", "list", "string"], ValueError(f'不支持的存储类型-{key_type}')
    assert conn.type(path) == key_type or key_type == 'string', f'读取类型错误-{path}:{conn.type(path)}, 读取类型-{key_type}'

    def read():
        if key_type == 'string':
            keys: list = conn.keys(path)
            if keys:
                return conn.mget(keys)
        elif key_type == 'list':
            return conn.lrange(path, 0, -1)
        else:
            return list(conn.smembers(path))
        return []

    data = read()
    for i in range(0, len(data), batch_size):
        _data = data[i: i + batch_size]
        _data = [json_or_eval(_, errors="ignore") for _ in _data]
        yield _data


if __name__ == '__main__':
    from no_views.conn import redis
    for _ in by_redis(path="*|*", conn=redis, key_type="string", batch_size=1):
        print(_)
