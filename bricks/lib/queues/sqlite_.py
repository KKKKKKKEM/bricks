# -*- coding: utf-8 -*-
# @Time    : 2025-12-18
# @Author  : Assistant
# @Desc    : SQLite-based persistent queue with multi-process/thread safety
import json
import os
import re
import sqlite3
import threading
import time
from collections import defaultdict
from contextlib import contextmanager
from pathlib import Path
from typing import Literal, Optional

from loguru import logger

from bricks.lib.queues import Item, TaskQueue
from bricks.utils import pandora

pandora.require("filelock")
from filelock import FileLock  # noqa: E402


class SQLiteQueue(TaskQueue):
    """
    基于 SQLite 的持久化队列

    特性：
    - 单机持久化存储，程序重启可恢复
    - 多进程/多线程安全（文件锁 + IMMEDIATE 事务 + WAL 模式）
    - 三队列模型：current（待处理）、temp（处理中）、failure（失败）
    - 接口与 LocalQueue/RedisQueue 保持一致
    - 每个队列（name + qtype）使用独立的表

    安全机制：
    - 文件锁（FileLock）：每个队列独立的文件锁，保证多进程/多线程安全
      * FileLock 本身是线程安全的，无需额外的线程锁
      * 不同队列使用不同的文件锁，支持多队列并发操作
    - IMMEDIATE 事务：确保读-写操作的原子性
    - WAL 模式：提高并发读写性能
    """

    def __init__(self, db_path: Optional[str] = None, timeout: float = 30.0) -> None:
        """
        初始化 SQLite 队列

        :param db_path: 数据库文件路径，默认为 ./data/queues.db
        :param timeout: 数据库锁超时时间（秒）
        """
        if db_path is None:
            db_path = os.path.join(os.getcwd(), "data", "queues.db")

        self.db_path = db_path
        self.timeout = timeout
        self._local = threading.local()
        self._status = defaultdict(threading.Event)
        self._table_cache = set()  # 缓存已创建的表名

        # 确保数据库目录存在
        self.db_dir = Path(self.db_path).parent
        self.db_dir.mkdir(parents=True, exist_ok=True)

        # 文件锁字典（每个队列一个文件锁，支持多队列并发）
        self._file_locks = {}

        # 初始化连接
        with self._get_connection() as conn:
            cursor = conn.cursor()

            # 记录表（用于存储队列元数据）
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS _queue_records (
                    queue_name TEXT PRIMARY KEY,
                    record_data TEXT NOT NULL,
                    updated_at REAL NOT NULL
                )
            """)

            # 状态表（用于存储队列状态）
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS _queue_status (
                    queue_name TEXT PRIMARY KEY,
                    status INTEGER DEFAULT 0,
                    updated_at REAL NOT NULL
                )
            """)

        conn.commit()

    def _sanitize_table_name(self, name: str) -> str:
        """
        将队列名转换为合法的表名

        :param name: 队列名
        :return: 合法的表名
        """
        # 移除特殊字符，只保留字母、数字、下划线
        safe_name = re.sub(r'[^a-zA-Z0-9_]', '_', name)
        return f"queue_{safe_name}"

    def _ensure_table_exists(self, table_name: str):
        """
        确保表存在，不存在则创建
        注意：此方法应在事务内调用，不会自动 commit

        :param table_name: 表名
        """
        if table_name in self._table_cache:
            return

        # 获取当前线程的连接（不创建新的上下文管理器）
        conn = self._local.conn if hasattr(
            self._local, "conn") and self._local.conn else None
        if not conn:
            # 如果没有连接，创建一个临时的
            with self._get_connection() as conn:
                cursor = conn.cursor()
                self._create_table(conn, cursor, table_name)
        else:
            # 在现有事务中创建表
            cursor = conn.cursor()
            self._create_table(conn, cursor, table_name)

    def _create_table(self, conn, cursor, table_name: str):
        """创建表的实际逻辑"""
        # 创建队列数据表，添加 qtype 字段
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS "{table_name}" (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                qtype TEXT NOT NULL,
                value TEXT NOT NULL,
                priority INTEGER DEFAULT 0,
                created_at REAL NOT NULL,
                UNIQUE(value, qtype)
            )
        """)

        # 创建索引以提高查询性能
        cursor.execute(f"""
            CREATE INDEX IF NOT EXISTS "idx_{table_name}_qtype_priority" 
            ON "{table_name}"(qtype, priority DESC, id ASC)
        """)

        # 标记为已创建（无论在不在事务中）
        self._table_cache.add(table_name)

    @contextmanager
    def _get_connection(self):
        """
        获取数据库连接（线程安全）
        使用 WAL 模式以提高并发性能
        """
        if not hasattr(self._local, "conn") or self._local.conn is None:
            self._local.conn = sqlite3.connect(
                self.db_path,
                timeout=self.timeout,
                check_same_thread=False,
                isolation_level=None,  # 设置为 None，手动控制事务
            )
            # 启用 WAL 模式以支持并发读写
            self._local.conn.execute("PRAGMA journal_mode=WAL")
            # 启用外键约束
            self._local.conn.execute("PRAGMA foreign_keys=ON")
            # 设置同步模式为 NORMAL 以提高性能
            self._local.conn.execute("PRAGMA synchronous=NORMAL")

        try:
            yield self._local.conn
        except Exception as e:
            self._local.conn.rollback()
            raise e

    def __str__(self):
        return f"<SQLiteQueue path={self.db_path}>"

    def _get_lock(self, name: str) -> FileLock:
        """获取队列的文件锁（每个队列独立，支持多队列并发）"""
        if name not in self._file_locks:
            lock_file = self.db_dir / f"{Path(self.db_path).stem}_{name}.lock"
            self._file_locks[name] = FileLock(str(lock_file))
        return self._file_locks[name]

    @contextmanager
    def _lock(self, name: str):
        """
        获取队列的文件锁

        FileLock 本身是线程安全的，无需额外的线程锁
        每个队列有独立的文件锁，支持多队列并发操作
        """
        lock = self._get_lock(name)
        with lock:
            yield

    def size(
        self, *names: str, qtypes: tuple = ("current", "temp", "failure"), **kwargs
    ) -> int:
        """获取队列大小（只读操作，不需要 IMMEDIATE 事务）"""
        if not names:
            return 0

        count = 0
        with self._get_connection() as conn:
            cursor = conn.cursor()
            for name in names:
                table_name = self._sanitize_table_name(name)
                # 检查表是否存在
                cursor.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                    (table_name,)
                )
                if cursor.fetchone():
                    for qtype in qtypes:
                        cursor.execute(
                            f'SELECT COUNT(*) FROM "{table_name}" WHERE qtype = ?',
                            (qtype,)
                        )
                        result = cursor.fetchone()
                        count += result[0] if result else 0

        return count

    def put(self, name: str, *values, **kwargs):
        """添加任务（原子性写操作）"""
        qtypes = kwargs.pop("qtypes", "current")
        unique = kwargs.pop("unique", True)
        priority = kwargs.pop("priority", None)
        limit = kwargs.pop("limit", 0)

        if not values:
            return 0

        with self._get_connection() as conn:
            cursor = conn.cursor()
            try:
                # 使用 IMMEDIATE 事务确保原子性
                conn.execute("BEGIN IMMEDIATE")
                count = 0
                priority_value = 1 if priority else 0

                for qtype in pandora.iterable(qtypes):
                    table_name = self._sanitize_table_name(name)
                    self._ensure_table_exists(table_name)

                    # 检查限制
                    if limit > 0:
                        cursor.execute(
                            f'SELECT COUNT(*) FROM "{table_name}" WHERE qtype = ?',
                            (qtype,)
                        )
                        current_size = cursor.fetchone()[0]
                        if current_size >= limit:
                            continue

                    for value in values:
                        # value 已经被 _when_put 装饰器序列化为字符串了
                        value_str = value if isinstance(value, str) else json.dumps(
                            value, ensure_ascii=False, default=str)

                        try:
                            cursor.execute(
                                f"""
                                INSERT INTO "{table_name}" (qtype, value, priority, created_at)
                                VALUES (?, ?, ?, ?)
                                """,
                                (qtype, value_str, priority_value, time.time()),
                            )
                            count += cursor.rowcount
                        except sqlite3.IntegrityError:
                            # 如果 unique=True 且值已存在，忽略
                            if not unique:
                                # 如果不需要唯一，更新优先级
                                cursor.execute(
                                    f"""
                                    UPDATE "{table_name}" 
                                    SET priority = ?, created_at = ?
                                    WHERE qtype = ? AND value = ?
                                    """,
                                    (priority_value, time.time(), qtype, value_str),
                                )

                conn.execute("COMMIT")
                return count
            except Exception as e:
                self._safe_rollback(conn, on_error="log")
                raise e

    def get(self, name: str, count: int = 1, **kwargs) -> Item:
        """
        获取任务（原子性写操作）

        使用文件锁 + IMMEDIATE 事务确保多进程/多线程安全：
        1. 在查询前就获取该队列的文件锁
        2. 开启 IMMEDIATE 事务后再查询
        3. 原子性地将数据从 current 移动到 temp
        """
        table_name = self._sanitize_table_name(name)
        tail = kwargs.pop("tail", False)

        # 使用双重锁保护整个读-写过程
        with self._lock(name):
            with self._get_connection() as conn:
                cursor = conn.cursor()

                # 检查表是否存在
                cursor.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                    (table_name,)
                )
                if not cursor.fetchone():
                    return

                # 确保表存在
                self._ensure_table_exists(table_name)

                # 使用 IMMEDIATE 事务确保从查询到更新的原子性
                try:
                    conn.execute("BEGIN IMMEDIATE")

                    # 按优先级和 ID 排序获取
                    order = "ASC" if tail else "ASC"
                    cursor.execute(
                        f"""
                            SELECT id, value, priority, created_at FROM "{table_name}"
                            WHERE qtype = 'current'
                            ORDER BY priority DESC, id {order}
                            LIMIT ?
                            """,
                        (count,),
                    )

                    rows = cursor.fetchall()
                    if not rows:
                        self._safe_rollback(conn, on_error="log")
                        return None

                    items = []
                    ids_to_update = []

                    for row_id, value_str, priority, created_at in rows:
                        try:
                            value = json.loads(value_str)
                            items.append(value)
                            ids_to_update.append(row_id)
                        except json.JSONDecodeError:
                            logger.error(
                                f"Failed to decode value: {value_str}")

                    if items:
                        # 将 qtype 从 'current' 改为 'temp'
                        for row_id in ids_to_update:
                            cursor.execute(
                                f'UPDATE "{table_name}" SET qtype = \'temp\', priority = 0 WHERE id = ?',
                                (row_id,)
                            )

                        conn.execute("COMMIT")
                        return items
                    else:
                        self._safe_rollback(conn, on_error="log")
                        return None

                except Exception as e:
                    self._safe_rollback(conn, on_error="log")
                    raise e

    def remove(self, name: str, *values, **kwargs):
        """删除任务（原子性写操作）"""
        backup = kwargs.pop("backup", None)
        qtypes = kwargs.get("qtypes", "temp")

        if not values:
            return 0

        with self._get_connection() as conn:
            cursor = conn.cursor()
            try:
                # 使用 IMMEDIATE 事务确保原子性
                conn.execute("BEGIN IMMEDIATE")
                count = 0
                table_name = self._sanitize_table_name(name)

                # 检查表是否存在
                cursor.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                    (table_name,)
                )
                if not cursor.fetchone():
                    self._safe_rollback(conn, on_error="log")
                    return 0

                for value in values:
                    # value 已经被 _when_remove 装饰器序列化为字符串了
                    value_str = value if isinstance(value, str) else json.dumps(
                        value, ensure_ascii=False, default=str)

                    if backup:
                        self._ensure_table_exists(table_name)

                        for qtype in pandora.iterable(qtypes):
                            cursor.execute(
                                f'SELECT qtype, value, priority, created_at FROM "{table_name}" WHERE qtype = ? AND value = ?',
                                (qtype, value_str)
                            )
                            row = cursor.fetchone()
                            if row:
                                try:
                                    # 备份到 backup qtype
                                    cursor.execute(
                                        f'INSERT INTO "{table_name}" (qtype, value, priority, created_at) VALUES (?, ?, ?, ?)',
                                        (backup, row[1], row[2], row[3])
                                    )
                                except sqlite3.IntegrityError:
                                    pass

                                cursor.execute(
                                    f'DELETE FROM "{table_name}" WHERE qtype = ? AND value = ?',
                                    (qtype, value_str)
                                )
                                count += cursor.rowcount
                    else:
                        for qtype in pandora.iterable(qtypes):
                            cursor.execute(
                                f'DELETE FROM "{table_name}" WHERE qtype = ? AND value = ?',
                                (qtype, value_str)
                            )
                            count += cursor.rowcount

                conn.execute("COMMIT")
                return count
            except Exception as e:
                self._safe_rollback(conn, on_error="log")
                raise e

    def replace(self, name: str, *values, **kwargs):
        """替换任务（原子性写操作）"""
        qtypes = kwargs.pop("qtypes", ["current", "temp", "failure"])
        count = 0

        with self._get_connection() as conn:
            cursor = conn.cursor()
            try:
                # 使用 IMMEDIATE 事务确保原子性
                conn.execute("BEGIN IMMEDIATE")
                table_name = self._sanitize_table_name(name)

                # 检查表是否存在
                cursor.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                    (table_name,)
                )
                if not cursor.fetchone():
                    self._safe_rollback(conn, on_error="log")
                    return 0

                for old, new in values:
                    for qtype in pandora.iterable(qtypes):
                        # 注意：replace 方法没有装饰器，所以需要序列化
                        old_str = old if isinstance(old, str) else json.dumps(
                            old, ensure_ascii=False, default=str)
                        new_str = new if isinstance(new, str) else json.dumps(
                            new, ensure_ascii=False, default=str)

                        cursor.execute(
                            f'DELETE FROM "{table_name}" WHERE qtype = ? AND value = ?',
                            (qtype, old_str),
                        )
                        if cursor.rowcount > 0:
                            try:
                                cursor.execute(
                                    f"""
                                    INSERT INTO "{table_name}" (qtype, value, priority, created_at)
                                    VALUES (?, ?, 0, ?)
                                    """,
                                    (qtype, new_str, time.time()),
                                )
                                count += cursor.rowcount
                            except sqlite3.IntegrityError:
                                pass

                conn.execute("COMMIT")
                return count
            except Exception as e:
                self._safe_rollback(conn, on_error="log")
                raise e

    def reverse(self, name: str, **kwargs) -> bool:
        """队列翻转（原子性写操作） - 使用 DELETE 重复 + UPDATE qtype"""
        qtypes = kwargs.pop("qtypes", None) or ["temp", "failure"]
        table_name = self._sanitize_table_name(name)

        with self._get_connection() as conn:
            cursor = conn.cursor()
            try:
                # 使用 IMMEDIATE 事务确保原子性
                conn.execute("BEGIN IMMEDIATE")

                # 检查表是否存在
                cursor.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                    (table_name,)
                )
                if not cursor.fetchone():
                    self._ensure_table_exists(table_name)

                # 处理 reverse: 需要处理 UNIQUE(value, qtype) 约束
                # 策略：
                # 1. 先删除 current 中已存在的 value（来自任何源 qtype）
                # 2. 删除源 qtypes 之间重复的 value（保留第一个）
                # 3. 然后 UPDATE 剩余的记录到 current

                if len(qtypes) > 0:
                    # 构建所有源 qtype 的 IN 子句
                    qtype_placeholders = ','.join('?' * len(qtypes))

                    # 删除 current 中与任何源 qtype 重复的 value
                    cursor.execute(f'''
                        DELETE FROM "{table_name}"
                        WHERE qtype = 'current' 
                        AND value IN (
                            SELECT value FROM "{table_name}" 
                            WHERE qtype IN ({qtype_placeholders})
                        )
                    ''', qtypes)

                    # 删除源 qtypes 之间的重复（保留 id 最小的）
                    if len(qtypes) > 1:
                        cursor.execute(f'''
                            DELETE FROM "{table_name}"
                            WHERE qtype IN ({qtype_placeholders})
                            AND id NOT IN (
                                SELECT MIN(id) FROM "{table_name}"
                                WHERE qtype IN ({qtype_placeholders})
                                GROUP BY value
                            )
                        ''', qtypes + qtypes)

                    # UPDATE 所有源 qtype 到 current
                    cursor.execute(
                        f'UPDATE "{table_name}" SET qtype = \'current\' WHERE qtype IN ({qtype_placeholders})',
                        qtypes
                    )

                conn.execute("COMMIT")
                return True
            except Exception as e:
                self._safe_rollback(conn, on_error="log")
                raise e

    def smart_reverse(self, name: str, status=0) -> bool:
        """智能翻转"""
        tc = self.size(name, qtypes=("temp",))
        cc = self.size(name, qtypes=("current",))
        fc = self.size(name, qtypes=("failure",))

        if cc == 0 and fc != 0:
            qtypes = ["failure"]
            need_reverse = True
        elif cc == 0 and fc == 0 and tc != 0 and status == 0:
            qtypes = ["temp"]
            need_reverse = True
        else:
            need_reverse = False
            qtypes = []

        if need_reverse:
            self.reverse(name, qtypes=qtypes)
            return True
        return False

    def merge(self, dest: str, *queues: str, **kwargs):
        """合并队列（原子性写操作）"""
        # 注意: merge 这里 dest 和 queues 可能是完整的表名，不是 queue name
        # 这里保留原有逻辑，但如果是同一个 name 的不同 qtype，应该用 reverse
        with self._get_connection() as conn:
            cursor = conn.cursor()
            try:
                # 使用 IMMEDIATE 事务确保原子性
                conn.execute("BEGIN IMMEDIATE")

                cursor.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                    (dest,)
                )
                if not cursor.fetchone():
                    # dest 可能是表名，直接创建
                    cursor.execute(f"""
                        CREATE TABLE IF NOT EXISTS "{dest}" (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            qtype TEXT NOT NULL,
                            value TEXT NOT NULL,
                            priority INTEGER DEFAULT 0,
                            created_at REAL NOT NULL,
                            UNIQUE(value, qtype)
                        )
                    """)

                for source_key in queues:
                    cursor.execute(
                        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                        (source_key,)
                    )
                    if not cursor.fetchone():
                        continue

                    cursor.execute(f"""
                        INSERT OR IGNORE INTO "{dest}" (qtype, value, priority, created_at)
                        SELECT qtype, value, priority, created_at FROM "{source_key}"
                    """)

                    cursor.execute(f'DELETE FROM "{source_key}"')

                conn.execute("COMMIT")
                return True
            except Exception as e:
                self._safe_rollback(conn, on_error="log")
                raise e

    def clear(
        self, *names, qtypes=("current", "temp", "failure", "lock", "record"), **kwargs
    ):
        """清空队列（原子性写操作）"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            try:
                # 使用 IMMEDIATE 事务确保原子性
                conn.execute("BEGIN IMMEDIATE")

                for name in names:
                    table_name = self._sanitize_table_name(name)
                    cursor.execute(
                        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                        (table_name,)
                    )
                    if cursor.fetchone():
                        # 如果指定了 qtypes，删除对应 qtype 的数据
                        # 否则 DROP 整个表
                        if qtypes and len(qtypes) < 5:  # 不是全部 qtypes
                            for qtype in qtypes:
                                cursor.execute(
                                    f'DELETE FROM "{table_name}" WHERE qtype = ?',
                                    (qtype,)
                                )
                        else:
                            cursor.execute(
                                f'DROP TABLE IF EXISTS "{table_name}"')
                            self._table_cache.discard(table_name)

                    cursor.execute(
                        "DELETE FROM _queue_records WHERE queue_name = ?", (name,))
                    cursor.execute(
                        "DELETE FROM _queue_status WHERE queue_name = ?", (name,))

                conn.execute("COMMIT")
            except Exception as e:
                self._safe_rollback(conn, on_error="raise")
                raise e

    def _safe_rollback(self, conn, on_error: Literal["raise", "log"] = "log"):
        """回滚事务"""
        try:
            conn.execute("ROLLBACK")
        except Exception as e:
            if on_error == "raise":
                raise e
            else:
                logger.error(f"Rollback failed: {e}")

    def command(self, name: str, order: dict):
        """执行命令"""
        def set_record():
            record_data = json.dumps(order["record"], default=str)
            with self._get_connection() as conn:
                cursor = conn.cursor()
                try:
                    conn.execute("BEGIN IMMEDIATE")
                    cursor.execute(
                        """
                        INSERT OR REPLACE INTO _queue_records (queue_name, record_data, updated_at)
                        VALUES (?, ?, ?)
                        """,
                        (name, record_data, time.time()),
                    )
                    conn.execute("COMMIT")
                except Exception as e:
                    self._safe_rollback(conn, on_error="log")
                    raise e

        def reset_init_record():
            with self._get_connection() as conn:
                cursor = conn.cursor()
                try:
                    conn.execute("BEGIN IMMEDIATE")
                    cursor.execute(
                        "DELETE FROM _queue_records WHERE queue_name = ?", (name,))
                    cursor.execute(
                        "DELETE FROM _queue_status WHERE queue_name = ?", (name,))
                    conn.execute("COMMIT")
                except Exception as e:
                    self._safe_rollback(conn, on_error="log")
                    raise e
            self.clear(name)

        def wait_for_init_start():
            while not self._status[name].is_set() and self.is_empty(name):
                time.sleep(1)
                logger.debug("等待初始化开始")

        def release_init():
            with self._get_connection() as conn:
                cursor = conn.cursor()
                try:
                    conn.execute("BEGIN IMMEDIATE")
                    cursor.execute(
                        "DELETE FROM _queue_records WHERE queue_name = ?", (name,))
                    cursor.execute(
                        "DELETE FROM _queue_status WHERE queue_name = ?", (name,))
                    conn.execute("COMMIT")
                except Exception as e:
                    self._safe_rollback(conn, on_error="log")
                    raise e
            self._status[name].clear()

        def get_record():
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT record_data FROM _queue_records WHERE queue_name = ?",
                    (name,)
                )
                row = cursor.fetchone()

                if row:
                    record = json.loads(row[0])
                    if record.get("status") == 0:
                        try:
                            conn.execute("BEGIN IMMEDIATE")
                            cursor.execute(
                                "DELETE FROM _queue_records WHERE queue_name = ?",
                                (name,)
                            )
                            conn.execute("COMMIT")
                        except Exception as _:
                            self._safe_rollback(conn, on_error="log")
                        return {}
                    else:
                        return record
                else:
                    return {}

        def set_init():
            self._status[name].set()

        def is_init():
            return self._status[name].is_set()

        actions = {
            self.COMMANDS.GET_PERMISSION: lambda: {"state": True, "msg": "success"},
            self.COMMANDS.GET_RECORD: get_record,
            self.COMMANDS.CONTINUE_RECORD: lambda: self.reverse(name),
            self.COMMANDS.SET_RECORD: set_record,
            self.COMMANDS.RESET_INIT: reset_init_record,
            self.COMMANDS.WAIT_INIT: wait_for_init_start,
            self.COMMANDS.SET_INIT: set_init,
            self.COMMANDS.IS_INIT: is_init,
            self.COMMANDS.RELEASE_INIT: release_init,
        }

        action = order["action"]
        if action in actions:
            return actions[action]()

    def close(self):
        """关闭数据库连接"""
        if hasattr(self._local, "conn") and self._local.conn is not None:
            self._local.conn.close()
            self._local.conn = None

    def __del__(self):
        """析构函数，确保连接被关闭"""
        try:
            self.close()
        except Exception as e:
            logger.error(f"Error during SQLiteQueue cleanup: {e}")
