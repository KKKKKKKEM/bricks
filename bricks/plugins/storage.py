# -*- coding: utf-8 -*-
# @Time    : 2023-12-09 10:00
# @Author  : Kem
# @Desc    : 存储插件
import json
from typing import List, Union, Optional, Literal

from bricks.db.redis_ import Redis
from bricks.db.sqlite import Sqlite
from bricks.lib.items import Items
from bricks.utils.csv_ import Writer
from bricks.utils.pandora import iterable


def to_sqlite(
        path: str,
        conn: Sqlite,
        items: Union[List[dict], Items],
        row_keys: Optional[List[str]] = None,
):
    """
    存入数据至 sqlite
    
    :param path: 表名
    :param conn: sqlite 实例
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
        conn: Optional[Writer] = None,
        encoding: str = 'utf-8-sig',
        **kwargs
):
    """
    存入数据至 csv

    :param conn:
    :param path: 表名
    :param items: 数据
    :param encoding: 编码
    :param kwargs: DictWriter 的其他参数
    :return:
    """
    if not items: return
    if not isinstance(items, Items):
        items = Items(items)
    kwargs.setdefault("header", tuple(items.columns))
    conn = conn or Writer.create_safe_writer(path=path, encoding=encoding, **kwargs)
    return conn.writerows(*items)


def to_redis(
        path: str,
        conn: Redis,
        items: Union[List[dict], Items],
        key_type: Literal["set", "list", "string"] = 'set',
        row_keys: Optional[list] = None,
        splice: str = '|',
        ttl: int = 0,
):
    """
    存入数据至 redis
    :param path: 键值, 当 key_type 为 string 时, 此参数为无用
    :param conn: Redis 实例
    :param items: 数据
    :param key_type: 存储类型
    :param row_keys: 存储类型
    :param splice: 分隔符
    :param ttl: key 的过期时间
    :return:
    """

    row_keys = row_keys or []

    # 仅支持 set, list, string
    assert key_type in ["set", "list", "string"], ValueError(f'不支持的存储类型-{key_type}')
    # 当存储类型不为 string 时, path 不能为空且类型必须与预期一致
    assert (path and conn.type(path) in [key_type,
                                         'none']) or key_type == 'string', f'存储类型错误, 已存在-{path}:{conn.type(path)}, 存储类型-{key_type}'
    # 当存储类型为 string 时, row_keys 不能为空
    assert row_keys or key_type != 'string', ValueError(f'存储类型为 string 时必须存在 row_keys')

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

    if row_keys:
        for row in items:
            row['$key'] = generate_key(row)
    write(items)
