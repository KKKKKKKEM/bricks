# -*- coding: utf-8 -*-
"""
@File    : make_seeds.py
@Date    : 2023-12-06 16:39
@Author  : yintian
@Desc    : 生产种子插件
"""
import re
import time
from typing import Optional, Union, List
from urllib.parse import urlencode

from bricks.utils.csv_ import Reader

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
    从 CSV 中获取种子, csv 必须有表头

    :param path: 文件路径
    :param query: 查询 sql
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
                query = query + f"LIMIT -1 OFFSET {skip}"

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
        query: str = None,
        database: str = None,
        batch_size: int = 10000,
        skip: Union[str, int] = ...,
        record: Optional[dict] = None,
        sort: Optional[List[tuple]] = None,
):
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


if __name__ == '__main__':
    st = time.time()
    for d in by_csv(
            path='/Users/Kem/Documents/bricks/bricks/utils/test.csv',
            query="select cast(a as INTEGER) as a, b from <TABLE> where a =0",
            # skip=1
    ):
        print(d)
    print(time.time() - st)
