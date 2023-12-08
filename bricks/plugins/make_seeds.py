# -*- coding: utf-8 -*-
"""
@File    : make_seeds.py
@Date    : 2023-12-06 16:39
@Author  : yintian
@Desc    : 
"""
import math
import time
from typing import Optional, Union, List
from urllib.parse import urlencode

from bricks.utils.csv import CsvReader
from bricks.utils.pandora import get_files_by_path


def by_csv(
        path: Union[str, List[str]],
        fields: list = None,
        batch_size: int = 10000,
        skip: Union[str, int] = None,
        query: str = None,
        mapping: Optional[dict] = None,
        stop: Optional[int] = math.inf,
        reader_kwargs: Optional[dict] = None,
        record: Optional[dict] = None,
):
    """
    从 CSV 中获取种子

    :param path: 文件路径
    :param fields: 暂不支持
    :param batch_size: 一次投多少种子
    :param skip: 跳过初始多少种子
    :param query: 查询条件, 为 python 伪代码, 如 "a < 10 and 'bbb' in b.lower()"
    :param mapping: 暂不支持
    :param stop: 投到多少停止
    :param reader_kwargs: 初始化 csv reader 的其他参数
    :param record:
    :return:
    """
    record = record or {}
    reader_kwargs = reader_kwargs or {}
    mapping = mapping or {}
    if mapping or fields:
        raise ValueError(f'暂不支持 mapping/fields 参数')
    if skip is None:
        if record:
            skip = 'auto'
        else:
            skip = 0

    raw_skip = skip
    total = 0
    for file in get_files_by_path(path):
        _record = {
            "path": path,
            "file": file,
            "query": query,
        }
        record_key = urlencode(_record)
        if raw_skip == 'auto':
            total = int(record.get('total', 0))
            skip = int(record.get(record_key, 0))
        else:
            skip = raw_skip
        with CsvReader(file_path=file, **reader_kwargs) as reader:
            for row in reader.iter_data(
                    count=batch_size,
                    skip=skip,
                    fields=fields,
                    query=query
            ):
                total += len(row)
                skip += len(row)
                if total > stop:
                    return
                record.update({record_key: skip})
                yield row


if __name__ == '__main__':
    st = time.time()
    for __ in range(1000):
        with CsvReader(file_path='../../files/e.csv') as r:
            for _ in r.iter_data(skip=10, query='int(a) % 3 == 0'):
                # print(_)
                pass
    print(time.time() - st)
