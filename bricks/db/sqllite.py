# -*- coding: utf-8 -*-
# @Time    : 2023-11-15 16:42
# @Author  : Kem
# @Desc    :
import contextlib
import json
import sqlite3
import subprocess


class SqlLite:

    def __init__(self, name, structure: dict = None, database=":memory:"):
        sqlite3.register_adapter(bool, int)
        sqlite3.register_adapter(list, json.dumps)
        sqlite3.register_adapter(list, json.dumps)
        sqlite3.register_adapter(dict, json.dumps)
        sqlite3.register_converter("BOOLEAN", lambda v: bool(int(v)))
        sqlite3.register_converter("OBJECT", json.loads)
        self.database = database
        self.name = name
        self._db = sqlite3.connect(database, detect_types=sqlite3.PARSE_DECLTYPES)
        self._columns = []
        self.structure: dict = structure
        structure and self.create_table(name, structure)

    @contextlib.contextmanager
    def cursor(self) -> sqlite3.Cursor:
        cur = self._db.cursor()
        yield cur
        cur.close()

    def insert(self, *docs: dict):
        sql = 'INSERT INTO ' + self.name + f' ({",".join(docs[0].keys())}) VALUES ({",".join(["?"] * len(docs[0]))})'
        with self.cursor() as cur:
            cur: sqlite3.Cursor
            cur.executemany(sql, [tuple(doc.values()) for doc in docs])

    def upsert(self, *docs: dict):
        sql = 'INSERT OR REPLACE INTO ' + self.name + f' ({",".join(docs[0].keys())}) VALUES ({",".join(["?"] * len(docs[0]))})'
        with self.cursor() as cur:
            cur: sqlite3.Cursor
            cur.executemany(sql, [tuple(doc.values()) for doc in docs])

    def find(self, query: str = None, fields: list = None, skip=0, limit=1000):
        if not fields:
            fields = ["*"]
            header = self.columns
        else:
            header = fields

        sql = f'SELECT {",".join(fields)} FROM ' + self.name
        if query:
            sql += f" where {query} "

        sql += f" limit {skip}"
        if limit != -1:
            sql += f',{limit}'

        with self.cursor() as cur:
            cur: sqlite3.Cursor
            cur.execute(f'pragma table_info({self.name})')
            cur.execute(sql)
            for data in cur.fetchall():
                yield dict(zip(header, data))

    def find_one(self, query: str = None, fields: list = None, skip=0):
        return next(self.find(query=query, fields=fields, skip=skip, limit=1), None)

    def update(self, query: str, update: dict):
        sql = f"UPDATE {self.name}" + f" SET {','.join([f'{k}=?' for k, v in update.items()])} WHERE {query}"
        with self.cursor() as cur:
            cur.execute(sql, tuple(update.values()))

    def delete(self, query: str):
        sql = "DELETE FROM " + f"{self.name} WHERE {query}"
        with self.cursor() as cur:
            cur.execute(sql)

    def create_table(self, name, structure: dict):
        python_to_sqlite_types = {
            type(None): "NULL",
            int: "INTEGER",
            float: "REAL",
            str: "TEXT",
            bytes: "BLOB"
        }
        with self.cursor() as cur:
            sql = f"CREATE TABLE IF NOT EXISTS {name}(" + ",".join(
                [f'{k} {python_to_sqlite_types.get(v, "TEXT")}' if v else k for k, v in structure.items()]) + ")"
            cur.execute(sql)

    @property
    def columns(self):
        if not self._columns:
            with self.cursor() as cur:
                cur.execute(f'pragma table_info({self.name})')
                self._columns = [i[1] for i in cur.fetchall()]

        return self._columns

    def run_sql(self, sql: str) -> sqlite3.Cursor:
        with self.cursor() as cursor:
            cursor.execute(sql)
            rows = cursor.fetchall()
            columns = [description[0] for description in cursor.description]
            for row in rows:
                yield dict(zip(columns, row))

    def execute(self, sql: str):
        with self.cursor() as cursor:
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

        :param structure:
        :param database: 数据库名称
        :param table: 表名
        :param path: 路径
        :param reload: 是否重新加载, 是的话如果数据库存在, 会先删除数据库
        :param debug: debug 模式会
        :return:
        """
        conn = cls(database=database + ".db", name=table)

        if reload:
            conn.execute(f'DROP TABLE IF EXISTS {table};')
        if structure:
            conn.create_table(table, structure=structure)
        cmd = f'sqlite3 {database}.db ".mode csv" ".import {path} {table}"'

        options = {}
        if not debug:
            options.update({
                "stdout": subprocess.DEVNULL,
                "stderr": subprocess.DEVNULL,
            })
        subprocess.run(cmd, shell=True, text=True, **options)

        return conn
