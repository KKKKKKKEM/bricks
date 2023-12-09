# -*- coding: utf-8 -*-
# @Time    : 2023-12-09 10:00
# @Author  : Kem
# @Desc    :
from typing import List, Union, Optional

from bricks.db.sqllite import SqlLite
from bricks.lib.items import Items


def to_sqllite(
        path: str,
        db: SqlLite,
        items: Union[List[dict], Items],
        row_keys: Optional[List[str]] = None,
):
    """
    存入数据值 sqllite
    
    :param path: 表名
    :param db: sqllite 实例
    :param items: 数据
    :param row_keys: 
    :return: 
    """
    if not items: return
    return db.insert(path, *items, row_keys=row_keys)
