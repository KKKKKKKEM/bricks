# -*- coding: utf-8 -*-
# @Time    : 2023-11-15 16:42
# @Author  : Kem
# @Desc    :
import csv
import os
import pickle
import sqlite3
import subprocess
from typing import List, Optional


class Sqlite:

    def __init__(self, database=":memory:", **kwargs):
        sqlite3.register_adapter(bool, int)
        sqlite3.register_adapter(object, pickle.dumps)
        sqlite3.register_adapter(list, pickle.dumps)
        sqlite3.register_adapter(set, pickle.dumps)
        sqlite3.register_adapter(tuple, pickle.dumps)
        sqlite3.register_adapter(dict, pickle.dumps)
        sqlite3.register_converter("BOOLEAN", lambda v: bool(int(v)))
        sqlite3.register_converter("OBJECT", pickle.loads)
        if database != ":memory:" and not database.endswith(".db"):
            database = database + ".db"

        self.database = database
        kwargs.setdefault("check_same_thread", False)
        self.connection = sqlite3.connect(database, detect_types=sqlite3.PARSE_DECLTYPES, **kwargs)
        self._cursor: sqlite3.Cursor
        self.python_to_sqlite_types = {
            type(None): "NULL",
            int: "INTEGER",
            float: "REAL",
            str: "TEXT",
            bytes: "BLOB",
            bool: "BOOLEAN",
            list: "OBJECT",
            set: "OBJECT",
            tuple: "OBJECT",
            dict: "OBJECT",
        }

    register_adapter = sqlite3.register_adapter
    register_converter = sqlite3.register_converter

    def __enter__(self):
        self._cursor = self.connection.cursor()
        return self._cursor

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._cursor and self._cursor.close()
        self.connection.commit()

    def find(self, sql: str, batch_size: int = 10000, unpack: bool = False) -> List[dict]:
        with self as cursor:
            cursor: sqlite3.Cursor
            cursor.execute(sql)
            columns = [description[0] for description in cursor.description]

            while True:
                rows = cursor.fetchmany(batch_size)
                if rows:
                    rows = [dict(zip(columns, row)) for row in rows]
                    if not unpack:
                        rows = [rows]

                    for row in rows:
                        yield row

                else:
                    break

    def insert(self, table: str, *docs: dict, row_keys: Optional[list] = None):
        if not docs:
            return

        keys = docs[0].keys()
        if row_keys:
            query = f' ON CONFLICT({",".join(row_keys)}) DO UPDATE SET {", ".join([f"{key} = excluded.{key}" for key in set(keys) - set(row_keys)])}'
        else:
            query = ""

        sql = 'INSERT INTO ' + table + f' ({",".join(keys)}) VALUES ({",".join(["?"] * len(keys))}){query}'
        with self as cur:
            cur.executemany(sql, [tuple(doc.values()) for doc in docs])

    def upsert(self, table: str, *docs: dict):
        sql = 'INSERT OR REPLACE INTO ' + table + f' ({",".join(docs[0].keys())}) VALUES ({",".join(["?"] * len(docs[0]))})'
        with self as cur:
            cur.executemany(sql, [tuple(doc.values()) for doc in docs])

    def update(self, table: str, query: str, update: dict):
        sql = f"UPDATE {table}" + f" SET {','.join([f'{k}=?' for k, v in update.items()])} WHERE {query}"
        with self as cur:
            cur.execute(sql, tuple(update.values()))

    def delete(self, table: str, query: str):
        sql = "DELETE FROM " + f"{table} WHERE {query}"
        with self as cur:
            cur.execute(sql)

    def drop(self, table: str):
        sql = f'DROP TABLE IF EXISTS {table};'
        with self as cur:
            cur.execute(sql)
            return cur.fetchone()

    def create_table(self, name, structure: dict):

        with self as cur:
            sql = f"CREATE TABLE IF NOT EXISTS {name}(" + ",".join(
                [f'{k} {self.python_to_sqlite_types.get(v, "OBJECT")}' if v else k for k, v in structure.items()]) + ")"
            cur.execute(sql)

    def execute(self, sql: str):
        with self as cursor:
            cursor.execute(sql)
            return cursor.fetchall()

    @classmethod
    def load_csv(
            cls,
            database: str,
            table: str,
            path: str,
            structure: dict = None,
            reload=True,
            debug=False
    ):
        """
        从 csv 中加载数据

        :param structure: 数据类型, 如 structure = {"a": int}, 这样查询出来的 a 就是 int 了
        :param database: 数据库名称
        :param table: 表名
        :param path: 路径
        :param reload: 是否重新加载, 是的话如果数据库存在, 会先删除数据库
        :param debug: debug 模式会
        :return:
        """
        if database != ":memory:" and database.endswith(".db"):
            database = database[:-3]

        conn = cls(database=database + ".db")

        if reload:
            conn.execute(f'DROP TABLE IF EXISTS {table};')
        if structure:
            conn.create_table(table, structure=structure)
        cmd = f'sqlite3 {database}.db ".mode csv" ".import {path!r} {table!r}"'

        options = {}
        if not debug:
            options.update({
                "stdout": subprocess.DEVNULL,
                "stderr": subprocess.DEVNULL,
            })
        subprocess.run(cmd, shell=True, text=True, **options)

        return conn

    def to_csv(
            self,
            sql: str,
            path: str,
            debug: bool = False,
            mode: int = ...
    ):
        """
        导出为 csv

        :param sql: 查询 sql
        :param path: 导出路径
        :param debug: debug 信息
        :param mode: 导出模式, 为 1 的时候会优先读取导出, 否则根据模式来使用导出命令或者读取导出
        :return:
        """
        if self.database == ":memory:" or mode == 1:
            if os.path.exists(path) and os.path.getsize(path):
                write_header = False
            else:
                write_header = True

            with open(path, "a+", newline='', encoding='utf-8') as f:
                writer = None
                for data in self.find(sql):
                    if not data:
                        return

                    keys = list(data[0].keys())
                    if not writer:
                        writer = csv.DictWriter(f, keys)
                        write_header and writer.writeheader()

                    writer.writerows(data)

        else:

            cmd = f'sqlite3 {self.database} ".headers on" ".mode csv" ".output {path}" "{sql}" ".output stdout"'

            options = {}
            if not debug:
                options.update({
                    "stdout": subprocess.DEVNULL,
                    "stderr": subprocess.DEVNULL,
                })
            subprocess.run(cmd, shell=True, text=True, **options)

    def close(self):
        self._cursor and self._cursor.close()
        self.connection.close()


if __name__ == '__main__':
    class People:
        def __init__(self, name: str):
            self.name = name

        def __str__(self):
            return f'<People name: {self.name}>'

        __repr__ = __str__


    # sqlite = Sqlite()
    # sqlite.register_adapter(People, pickle.dumps)
    # sqlite.create_table("test", {"a": object})
    # sqlite.insert("test", {"a": People("xxx")})
    # print(list(sqlite.find("select * from test")))
    # sqlite = Sqlite(database="/Users/Kem/Documents/bricks/bricks/utils/test_csv")
    # sqlite.to_csv(
    #     sql="select * from test where a< 100",
    #     path="xxx.csv",
    #     debug=True
    # )

    sqlite = Sqlite()
    # sqlite.create_table("test", {"id": int, "name": str})
    sqlite.execute("create table test (id INTEGER PRIMARY KEY, name TEXT)")
    # sqlite.insert("test", {"id": 1, "name": "kem"})
    # print(list(sqlite.execute("select * from test")))
    # sqlite.insert("test", {"id": 1, "name": "kem2"}, row_keys=['id'])
    # print(list(sqlite.execute("select * from test")))
