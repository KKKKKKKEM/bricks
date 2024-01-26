# -*- coding: utf-8 -*-
"""
@File    : csv.py
@Date    : 2023-12-08 11:10
@Author  : yintian
@Desc    : 
"""
import atexit
import csv
import functools
import os.path
import re
import threading
from typing import Optional, Callable, Literal

from loguru import logger

from bricks.db.sqlite import Sqlite

TABLE_PATTERN = re.compile(r"<TABLE>", flags=re.IGNORECASE)
NAME_PATTERN = re.compile(rf'{os.sep}|[.]', flags=re.IGNORECASE)

_lock = threading.Lock()


@functools.lru_cache(maxsize=None)
def _get_writer(p, **options):
    return Writer(p, **options)


class Reader:
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
        :param options: 其他实例化 DictReader 参数
        :param encoding: 编码
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

    def iter_data(self, sql: str, batch_size: int = 10000):
        """
        根据 sql 查询数据, sql 里面以 <TABLE> 指代当前表名

        :param batch_size:
        :param sql:
        :return:
        """
        for path in self.get_files(self.path):
            if not os.path.exists(path):
                raise FileNotFoundError(path)

            table = NAME_PATTERN.sub("_", path[:-4])
            database = NAME_PATTERN.sub("_", self.path)
            if self.structure:
                with open(path, encoding=self.encoding) as f:
                    header = csv.DictReader(f, **self.options).fieldnames
                    structure = {k: self.structure.get(k, str) for k in header}
            else:
                structure = None

            try:
                logger.debug(f'[加载数据] database: {database}, table: {table}, path: {path}')
                conn = Sqlite.load_csv(
                    database=database + ".db",
                    table=table,
                    path=path,
                    structure=structure
                )
                for data in conn.find(TABLE_PATTERN.sub(table, sql), batch_size=batch_size):
                    yield data
            finally:
                os.remove(f'{database}.db')


class Writer:
    def __init__(
            self,
            path: str,
            header: list,
            schema: Literal["sqlite:storage", "sqlite:memory", ""] = "",
            mode: str = "a+",
            newline="",
            encoding: str = 'utf-8-sig'
    ):
        """
        csv writer

        :param path: 文件路径
        :param header: csv 表头
        :param schema: normal: 常规写文件(线程不安全) / sqlite:storage: 将数据先写入到 sqlite, 可持久化(慢), 然后再导出为 csv / sqlite:memory: 将数据先写入到 sqlite, 内存(数据库), 然后再导出为 csv
        """
        assert header, "必须传入 header"

        if not path.endswith(".csv"):
            path = path + ".csv"

        self.header = header
        self.path = path
        self.mode = mode
        self.encoding = encoding
        self.newline = newline
        self.schema = schema
        self.table = NAME_PATTERN.sub("_", path[:-4])
        self.conn: Optional[Sqlite] = None
        self.writer: Optional[csv.DictWriter] = None
        self.file = None
        self.writerows: Optional[Callable] = None

        if self.schema == "sqlite:storage":
            self.database = NAME_PATTERN.sub("_", path)
            self.install_sqlite()
        elif self.schema == "sqlite:memory":
            self.database = ":memory:"
            self.install_sqlite()
        else:
            self.install_writer()

    def install_sqlite(self):
        self.writerows = self._by_sqlite
        self.conn = Sqlite(self.database)
        if "w" in self.mode: self.conn.drop(self.table)
        self.init_table()
        atexit.register(lambda: self.flush(done=True))

    def install_writer(self):
        self.writerows = self._by_writer
        if os.path.exists(self.path) and os.path.getsize(self.path):
            write_header = False
        else:
            write_header = True

        self.file = open(self.path, mode=self.mode, newline=self.newline, encoding=self.encoding)
        atexit.register(lambda: self.file.close())
        self.writer = csv.DictWriter(self.file, self.header)
        write_header and self.writer.writeheader()

    def writerows(self, *rows: dict):
        pass

    def _by_sqlite(self, *rows: dict):
        return self.conn.insert(self.table, *rows)

    def _by_writer(self, *rows: dict):
        try:
            return self.writer.writerows(rows)
        finally:
            self.flush()

    def init_table(self):
        self.conn.create_table(self.table, {h: str for h in self.header})

    def flush(self, done=False):
        """
        刷新缓存区, 将数据写入 csv 文件

        :param done: 是否为最后一次, 最后一次, 如果是 sqlite 模式, 会将表删除
        :return:
        """
        if "sqlite" in self.schema:
            self.conn.to_csv(sql=f'select * from {self.table}', path=self.path)
            self.conn.drop(self.table)
            if done:
                os.remove(f"{self.conn.database}")
            else:
                self.init_table()

        else:
            self.file and self.file.flush()

    @classmethod
    def create_safe_writer(cls, path: str, **options):
        if "header" in options:
            options['header'] = tuple(options['header'])
        with _lock:
            return _get_writer(path, **options)


if __name__ == '__main__':
    # writer = Writer("test.csv", header=["a", "b", "c"], schema="sqlite:storage")
    writer = Writer.create_safe_writer("test.csv", header=("a", "b", "c"), schema="sqlite:storage")
    # for _ in range(20000):
    #     # 生成一个随机数（这里以0到100之间的整数为例）
    #     writer.writerows({"a": _, "b": _, "c": _})

    # reader = Reader("test.csv", structure={"a": int})
    # count = 0
    # for d in reader.iter_data('select cast(a as TEXT) as a, b from <TABLE> where a < 1000'):
    #     print(d)
    #     count += 1
    # print(count)
