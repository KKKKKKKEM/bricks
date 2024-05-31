# -*- coding: utf-8 -*-
# @Time    : 2023-11-21 13:18
# @Author  : Kem
# @Desc    :
from __future__ import absolute_import
from typing import Iterable, Optional, List

from bricks.utils import pandora

pandora.require("pymongo==4.6.0")

import pymongo
from pymongo import UpdateOne, InsertOne


class Mongo(pymongo.MongoClient):
    """
    Mongo 工具类

    """

    def __init__(
            self,
            host='127.0.0.1',
            username=None,
            password=None,
            port=27017,
            database='admin',
            auth_database=None
    ):
        """
        实例化 Mongo 工具类

        :param host: host
        :param username: 用户名
        :param password: 密码
        :param port: 端口
        :param database:数据库
        :param auth_database: 认证数据库
        """

        auth_database = auth_database or 'admin'

        if username is None:
            uri = "mongodb://" + host + ":" + str(port) + "/"
        else:
            uri = "mongodb://" + username + ":" + password + "@" + host + ":" + str(port) + "/" + str(auth_database)

        self.database = database
        self._db = None
        self._caches = set()
        super().__init__(uri)

    @property
    def db(self):
        """
        切换至默认的 database

        :return:
        """
        if self._db is None:
            self._db = self.get_database(self.database)
        return self._db

    @db.setter
    def db(self, item):
        """
        设置默认的 database

        :param item:
        :return:
        """
        self._db = self.get_database(item)

    def get_fields(self, collection, database=None, exclude=()):
        """
        获取集合的所有字段

        :param exclude: 需要排除的字段
        :param collection:
        :param database:
        :return:
        """
        database = database or self.database
        r = self[database][collection].inline_map_reduce(
            map="function() {for (var key in this) { emit(key, null); }}",
            reduce="function(key, stuff) { return null; }",
        )
        return {i["_id"] for i in r if i['_id'] not in exclude}

    def batch_data(
            self,
            collection: str,
            query: Optional[dict] = None,
            database: str = None,
            sort: Optional[List[tuple]] = None,
            projection: Optional[dict] = None,
            skip: int = 0,
            count: int = 1000,
    ) -> Iterable[List[dict]]:
        """
        从 collection_name 获取迭代数据

        :param projection: 过滤字段
        :param collection: 表名
        :param query: 过滤条件
        :param sort: 排序条件
        :param database: 数据库
        :param skip: 要跳过多少
        :param count: 一次能得到多少
        :return:
        """
        database = database or self.database
        if skip:
            r = self[database][collection].find_one(filter=query, skip=skip - 1)
            if not r:
                return

            last_value = r['_id']
        else:
            last_value = None

        sort_condition = {"_id": 1}
        if sort:
            sort_condition.update({k: v for k, v in sort})

        while True:
            pipline = []
            projection and pipline.append({"$project": projection})
            sort_condition and pipline.append({"$sort": sort_condition})
            query and pipline.append({'$match': query})
            last_value and pipline.append({"$match": {"_id": {"$gt": last_value}}})
            pipline.append({'$limit': count})
            data: List[dict] = list(self[database][collection].aggregate(pipline, allowDiskUse=True))

            if not data:
                return

            else:
                last_value = data[-1]['_id']
                yield data

    def iter_data(
            self,
            collection: str,
            query: Optional[dict] = None,
            projection: Optional[dict] = None,
            database: str = None,
            sort: Optional[List[tuple]] = None,
            skip: int = 0,
            count: int = 1000,
    ) -> Iterable[List[dict]]:
        """
        从 collection_name 获取迭代数据

        :param projection: 过滤字段
        :param collection: 表名
        :param query: 过滤条件
        :param sort: 排序条件
        :param database: 数据库
        :param skip: 要跳过多少
        :param count: 一次能得到多少
        :return:
        """
        database = database or self.database
        sort = [sort] if (not isinstance(sort, list) and sort) else sort
        database = database or self.database
        query = query or {}

        while True:
            data = list(self[database][collection].find(
                filter=query,
                projection=projection,
                skip=skip,
                sort=sort,
                limit=count,
                allow_disk_use=True
            ))
            if not data:
                return

            else:
                yield data
                skip += len(data)

    def write(
            self,
            collection,
            *items: dict,
            query: Optional[List[str]] = None,
            database: Optional[str] = None,
            **kwargs
    ):
        """
        批量更新或者插入

        :param collection: 表名
        :param items: 需要写入的字段
        :param query: 过滤条件
        :param database: 数据库
        :param kwargs:
        :return:



        .. code:: python

            m = Mongo()
            # 插入模式
            m.write(
                'my_collection',
                *[{'name': 'kem', 'id': i} for i in range(10)]
            )

            # 更新模式
            m.write(
                'my_collection',
                *[{'name': 'kem', 'id': i} for i in range(10)],
                query=["id"]
            )

        """

        query = query or []
        action = kwargs.pop('action', '$set')  # default update operator
        upsert = kwargs.pop('upsert', True)  # default upsert mode
        update_op = kwargs.pop('update_op', None) or UpdateOne
        insert_op = kwargs.pop('insert_op', None) or InsertOne
        database = database or self.database
        requests = []

        for index, item in enumerate(items):
            item = dict(item)
            if query:
                _query = {i: item.get(i) for i in query}
                requests.append(
                    update_op(
                        filter=_query,
                        update={action: item},
                        upsert=upsert
                    )
                )
            else:
                requests.append(insert_op(dict(item)))

        return requests and self[database][collection].bulk_write(requests, ordered=False)

    def create_index(self, collection, *fields, database=None, **kwargs):
        if f'{collection}{fields}{database}{kwargs}' in self._caches:
            return
        else:
            self._caches.add(f'{collection}{fields}{database}{kwargs}')

        kwargs.setdefault('background', True)
        database = database or self.database
        self[database][collection].create_index(
            [(_, 1) for _ in fields],
            **kwargs
        )
