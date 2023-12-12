# -*- coding: utf-8 -*-
# @Time    : 2023-12-09 10:00
# @Author  : Kem
# @Desc    : 存储插件
import json
from typing import List, Union, Optional, Literal

from bricks.db.redis_ import Redis
from bricks.db.sqllite import SqlLite
from bricks.lib.items import Items
from bricks.utils.csv_ import Writer
from bricks.utils.pandora import iterable


def to_sqllite(
        path: str,
        conn: SqlLite,
        items: Union[List[dict], Items],
        row_keys: Optional[List[str]] = None,
):
    """
    存入数据至 sqllite
    
    :param path: 表名
    :param conn: sqllite 实例
    :param items: 数据
    :param row_keys: 主键
    :return: 
    """
    if not items: return
    return conn.insert(path, *items, row_keys=row_keys)


def to_mongo(
        path: str,
        conn,
        items: Union[List[dict], Items],
        row_keys: Optional[List[str]] = None,
        database: str = None,
        **kwargs
):
    """
    存入数据至 mongo

    :param path: 表名
    :param conn: mongo 连接
    :param items: 数据
    :param row_keys: 主键
    :param database: 数据库名称
    :param kwargs: write 的其他参数
    :return:
    """

    from bricks.db.mongo import Mongo
    if not items: return
    conn: Mongo
    return conn.write(path, *items, query=row_keys, database=database, **kwargs)


def to_csv(
        path: str,
        items: Union[List[dict], Items],
        encoding: str = 'utf-8-sig',
        **kwargs
):
    """
    存入数据至 csv

    :param path: 表名
    :param items: 数据
    :param encoding: 编码
    :param kwargs: DictWriter 的其他参数
    :return:
    """
    if not items: return
    conn = Writer(path, encoding=encoding, **kwargs)
    return conn.writerows(items)


def to_redis(
        path: str,
        conn: Redis,
        items: Union[List[dict], Items],
        key_type: Literal["set", "list", "string"] = 'set',
        row_keys: Optional[list] = None,
        splice: str = '|',
        ttl: int = 0,
):
    row_keys = row_keys or []

    def generate_key(_row):
        key = splice.join([str(_row.get(key, "")) for key in row_keys])
        return key

    def write(item):
        nonlocal path

        if key_type == 'string':
            rows = {_.pop('$key'): json.dumps(_) for _ in item}
            conn.mset(rows)
            path = list(rows.keys())
        else:
            rows = [json.dumps(_) for _ in items]
            conn.add(path, *rows, genre=key_type)
        if ttl:
            for key in iterable(path):
                conn.expire(key, ttl)

    assert key_type in ["set", "list", "string"], ValueError(f'不支持的存储类型-{key_type}')
    assert conn.type(path) in [key_type, 'none'], f'存储类型错误, 已存在-{path}:{conn.type(path)}, 存储类型-{key_type}'
    assert row_keys or key_type != 'string', ValueError(f'存储类型为 string 时必须存在 row_keys')

    if row_keys:
        for row in items:
            row['$key'] = generate_key(row)
    write(items)


if __name__ == '__main__':
    from no_views.conn import redis
    to_redis(
        path="2",
        conn=redis,
        items=[{"a": 1, "b": 2}, {"a": 2, "b": 3}],
        key_type="string",
        row_keys=["a", "b"],
        ttl=60
    )
