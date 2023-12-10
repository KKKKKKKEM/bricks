# -*- coding: utf-8 -*-
# @Time    : 2023-12-09 10:00
# @Author  : Kem
# @Desc    : 存储插件
from typing import List, Union, Optional

from bricks.db.sqllite import SqlLite
from bricks.lib.items import Items
from bricks.utils.csv_ import Writer


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
