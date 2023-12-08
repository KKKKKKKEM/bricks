# -*- coding: utf-8 -*-
"""
@File    : csv.py
@Date    : 2023-12-08 11:10
@Author  : yintian
@Desc    : 
"""
import csv
import itertools
import os


class CsvReader:
    """CSV reader"""
    __cache = {}

    def __init__(self, file_path, page_size=10, encoding='utf-8-sig', **kwargs):
        self.file_path = file_path
        self.page_size = page_size
        self.current_page = 1
        self.total_rows = 0
        self.encoding = encoding
        for k, v in kwargs.items():
            setattr(self, k, v)

    def __enter__(self):
        if not os.path.exists(self.file_path):
            raise FileExistsError(f'文件 {self.file_path} 不存在')
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if reader := self.__cache.get(self.file_path):
            reader['file'].close()
            del reader['reader']
            self.__cache.pop(self.file_path)

    @property
    def data(self) -> csv.DictReader:
        if self.__cache.get(self.file_path):
            return self.__cache[self.file_path]['reader']
        file = open(self.file_path, 'r', encoding=self.encoding)
        csv_reader = csv.DictReader(file)
        self.__cache[self.file_path] = {
            'reader': csv_reader,
            'file': file
        }
        return csv_reader

    @staticmethod
    def build_query(query):
        """
        构建过滤方法
        :param query: 过滤条件, 伪 Python 代码
        :return:
        """
        if not query:
            return lambda x: True
        _query = (query.
                  replace(' AND ', ' and ').
                  replace(' OR ', ' or ').
                  replace(' NULL ', ' None ').
                  replace(' NOT ', ' not '))

        # 将 query 语句包装在一个假的表达式中
        _query = f"True if ({_query}) else False"

        if ' like ' in _query.lower():
            raise ValueError('筛选暂不支持 like 语法')
        if ' exists ' in _query.lower():
            raise ValueError('筛选暂不支持 exists 语法')
        if ' between ' in _query.lower():
            raise ValueError('筛选暂不支持 between 语法')

        def filter_func(item):
            return eval(_query, {**item})

        return filter_func

    def iter_data(self, count=1000, skip=0, query='', fields=None):
        """
        获取数据

        :param count: 要获取多少个
        :param skip: 跳过多少个
        :param query: 查询条件, 伪 Python 代码
        :param fields: 暂不支持
        :return:
        """
        fields = fields or []  # noqa
        query_func = self.build_query(query)
        try:
            if skip:
                _ = [next(self.data) for _ in range(skip)]
        except StopIteration:
            return
        while True:
            data = []
            row_count = itertools.count(1)
            for row in self.data:
                if not query_func(row):
                    continue
                data.append(row)
                if next(row_count) >= count:
                    break
            if not data:
                return
            yield data


if __name__ == '__main__':
    pass
