# -*- coding: utf-8 -*-
"""
@File    : csv.py
@Date    : 2023-12-08 11:10
@Author  : yintian
@Desc    : 
"""
import csv
import os.path
import re

from bricks.db.sqllite import SqlLite

TABLE_PATTERN = re.compile(r"<TABLE>", flags=re.IGNORECASE)
NAME_PATTERN = re.compile(rf'{os.sep}|[.]', flags=re.IGNORECASE)


class CsvReader:
    def __init__(
            self,
            path: str,
            structure: dict = None,
            options: dict = None,
            encoding: str = 'utf-8-sig',
    ):
        """
        强调: csv 文件必须有表头

        :param path: 文件路径 或者文件夹路径
        :param structure: 数据类型, 如 structure = {"a": int}, 这样查询出来的 a 就是 int 了
        """
        self.path = path
        self.encoding = encoding
        self.structure = structure or {}
        self.options = options or {}
        self.maps = {}

    @classmethod
    def get_files(cls, path: str):
        """
        递归查找 path 下的所有 csv 文件

        :param path:
        :return:
        """
        if os.path.isdir(path):
            for file in os.listdir(path):
                for subpath in cls.get_files(os.path.join(path, file)):
                    yield subpath

        else:
            if path.endswith(".csv"):
                yield path

    def iter_data(self, sql: str):
        """
        根据 sql 查询数据, sql 里面以 <TABLE> 指代当前表名

        :param sql:
        :return:
        """
        for path in self.get_files(self.path):
            table = NAME_PATTERN.sub("_", path[:-4])
            database = NAME_PATTERN.sub("_", self.path)
            if self.structure:
                with open(path, encoding=self.encoding) as f:
                    header = csv.DictReader(f, **self.options).fieldnames
                    structure = {k: self.structure.get(k, str) for k in header}
            else:
                structure = None

            try:
                conn = SqlLite.load_csv(
                    database=database,
                    table=table,
                    path=path,
                    structure=structure
                )
                for data in conn.run_sql(TABLE_PATTERN.sub(table, sql)):
                    yield data
            finally:
                os.remove(f'{database}.db')


if __name__ == '__main__':
    reader = CsvReader("/Users/Kem/Documents/bricks/bricks/utils/test.csv", structure={"a": int})
    count = 0
    for d in reader.iter_data('select cast(a as TEXT) as a, b from <TABLE> where a =0'):
        print(d)
        count += 1
    print(count)
