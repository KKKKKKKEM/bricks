"""
SQLiteQueue

特性：
- 基于 `TaskQueue` 的持久化任务队列实现
- 多线程/多进程并发安全（SQLite 事务 + WAL）
- 三态模型：current（待处理）/ temp（处理中）/ failure（失败）
- `get` 会将数据从 current 原子性移动到 temp；`remove(backup=...)` 可将数据移动到 failure
"""

from __future__ import annotations

import json
import os
import re
import sqlite3
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Literal, Optional

from loguru import logger

from bricks import state
from bricks.lib.queues import TaskQueue
from bricks.utils import pandora


class SQLiteQueue(TaskQueue):
    VALID_QTYPES = ("current", "temp", "failure")

    def __init__(self, db_path: Optional[str] = None, timeout: float = 30.0) -> None:
        if db_path is None:
            db_path = os.path.join(os.getcwd(), "data", "queues.db")

        self.db_path = str(db_path)
        self.timeout = float(timeout)

        self._local = threading.local()
        self._guard = threading.RLock()
        self._table_cache: set[str] = set()

        self.db_dir = Path(self.db_path).parent
        self.db_dir.mkdir(parents=True, exist_ok=True)

        with self._connection() as conn:
            self._init_meta_tables(conn)

    def __str__(self) -> str:
        return f"<SQLiteQueue path={self.db_path}>"

    # ------------------------- Connection / Transaction -------------------------

    @contextmanager
    def _connection(self):
        if not hasattr(self._local, "conn") or self._local.conn is None:
            conn = sqlite3.connect(
                self.db_path,
                timeout=self.timeout,
                check_same_thread=False,
                isolation_level=None,
            )
            self._configure_connection(conn)
            self._local.conn = conn

        try:
            yield self._local.conn
        except Exception:
            try:
                self._local.conn.execute("ROLLBACK")
            except Exception:
                pass
            raise

    @staticmethod
    def _configure_connection(conn: sqlite3.Connection) -> None:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA temp_store=MEMORY")

    @contextmanager
    def _tx(
        self, conn: sqlite3.Connection, mode: Literal["IMMEDIATE", "DEFERRED"] = "IMMEDIATE"
    ):
        try:
            conn.execute(f"BEGIN {mode}")
            yield
            conn.execute("COMMIT")
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except Exception:
                pass
            raise

    # ------------------------- Locks (thread + process) -------------------------

    # ------------------------- Schema helpers -------------------------

    @staticmethod
    def _queue_table(name: str) -> str:
        safe_name = re.sub(r"[^a-zA-Z0-9_]", "_", name)
        return f"queue_{safe_name}"

    @staticmethod
    def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
        cur = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (table,),
        )
        return cur.fetchone() is not None

    @staticmethod
    def _table_has_column(conn: sqlite3.Connection, table: str, column: str) -> bool:
        rows = conn.execute(f'PRAGMA table_info("{table}")').fetchall()
        return any(r[1] == column for r in rows)

    def _ensure_queue_table(self, conn: sqlite3.Connection, table: str) -> None:
        with self._guard:
            cached = table in self._table_cache

        if not cached:
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS "{table}" (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    qtype TEXT NOT NULL,
                    value TEXT NOT NULL,
                    priority INTEGER NOT NULL DEFAULT 0,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    UNIQUE(value, qtype)
                )
                """
            )

            with self._guard:
                self._table_cache.add(table)

        conn.execute(
            f"""
            CREATE INDEX IF NOT EXISTS "idx_{table}_qtype_priority_id"
            ON "{table}"(qtype, priority DESC, id ASC)
            """
        )
        conn.execute(
            f"""
            CREATE INDEX IF NOT EXISTS "idx_{table}_qtype_value"
            ON "{table}"(qtype, value)
            """
        )

    def _init_meta_tables(self, conn: sqlite3.Connection) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS _queue_records (
                queue_name TEXT PRIMARY KEY,
                record_data TEXT NOT NULL,
                updated_at REAL NOT NULL,
                expires_at REAL
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS _queue_init_lock (
                queue_name TEXT PRIMARY KEY,
                owner TEXT NOT NULL,
                heartbeat REAL NOT NULL,
                interval REAL NOT NULL
            )
            """
        )

    def _cleanup_expired_records(self, conn: sqlite3.Connection, name: str) -> None:
        now = time.time()
        conn.execute(
            "DELETE FROM _queue_records WHERE queue_name = ? AND expires_at IS NOT NULL AND expires_at <= ?",
            (name, now),
        )

    @staticmethod
    def _parse_record(text: str) -> dict:
        try:
            obj = json.loads(text)
        except Exception:
            return {}
        return obj if isinstance(obj, dict) else {}

    # ------------------------- Core queue ops -------------------------

    def size(self, *names: str, qtypes: tuple = ("current", "temp", "failure"), **kwargs) -> int:
        if not names:
            return 0

        qtypes = tuple(pandora.iterable(qtypes))
        for qt in qtypes:
            if qt not in self.VALID_QTYPES:
                raise ValueError(f"invalid qtype: {qt}")

        with self._connection() as conn:
            def _count() -> int:
                total = 0
                for name in names:
                    table = self._queue_table(name)
                    if not self._table_exists(conn, table):
                        continue
                    for qt in qtypes:
                        cur = conn.execute(
                            f'SELECT COUNT(*) FROM "{table}" WHERE qtype = ?',
                            (qt,),
                        )
                        total += int(cur.fetchone()[0])
                return total

            if conn.in_transaction:
                return _count()
            with self._tx(conn, "DEFERRED"):
                return _count()

    def put(self, name: str, *values, **kwargs) -> int:
        if not name or not values:
            return 0

        qtypes = kwargs.pop("qtypes", "current")
        unique = bool(kwargs.pop("unique", True))
        priority = bool(kwargs.pop("priority", False))
        limit = int(kwargs.pop("limit", 0) or 0)

        priority_value = 1 if priority else 0
        now = time.time()

        with self._connection() as conn:
            with self._tx(conn, "IMMEDIATE"):
                table = self._queue_table(name)
                self._ensure_queue_table(conn, table)

                inserted = 0
                for qt in pandora.iterable(qtypes):
                    if qt not in self.VALID_QTYPES:
                        raise ValueError(f"invalid qtype: {qt}")

                    if limit > 0:
                        cur = conn.execute(
                            f'SELECT COUNT(*) FROM "{table}" WHERE qtype = ?',
                            (qt,),
                        )
                        current_size = int(cur.fetchone()[0])
                        if current_size >= limit:
                            continue
                        room = limit - current_size
                        batch_values = list(values)[:room]
                    else:
                        batch_values = list(values)

                    if not batch_values:
                        continue

                    if unique:
                        sql = f"""
                        INSERT OR IGNORE INTO "{table}" (qtype, value, priority, created_at, updated_at)
                        VALUES (?, ?, ?, ?, ?)
                        """
                    else:
                        sql = f"""
                        INSERT INTO "{table}" (qtype, value, priority, created_at, updated_at)
                        VALUES (?, ?, ?, ?, ?)
                        ON CONFLICT(value, qtype) DO UPDATE SET
                            priority = excluded.priority,
                            created_at = excluded.created_at,
                            updated_at = excluded.updated_at
                        """

                    before = conn.total_changes
                    conn.executemany(sql, [(qt, v, priority_value, now, now) for v in batch_values])
                    inserted += conn.total_changes - before

                return inserted

    def get(self, name: str, count: int = 1, **kwargs):
        tail = bool(kwargs.pop("tail", False))
        count = int(count or 1)
        if count <= 0:
            return None

        with self._connection() as conn:
            table = self._queue_table(name)
            if not self._table_exists(conn, table):
                return None

            with self._tx(conn, "IMMEDIATE"):
                self._ensure_queue_table(conn, table)
                order = "DESC" if tail else "ASC"
                rows = conn.execute(
                    f"""
                    SELECT id, value
                    FROM "{table}"
                    WHERE qtype = 'current'
                    ORDER BY priority DESC, id {order}
                    LIMIT ?
                    """,
                    (count,),
                ).fetchall()

                if not rows:
                    return None

                ids = [r[0] for r in rows]
                values = [r[1] for r in rows]

                placeholders = ",".join("?" for _ in ids)
                now = time.time()
                conn.execute(
                    f"""
                    UPDATE "{table}"
                    SET qtype = 'temp', priority = 0, updated_at = ?
                    WHERE qtype = 'current' AND id IN ({placeholders})
                    """,
                    (now, *ids),
                )
                return values

    def remove(self, name: str, *values, **kwargs) -> int:
        if not name or not values:
            return 0

        backup = kwargs.pop("backup", None)
        qtypes = kwargs.pop("qtypes", "temp")

        with self._connection() as conn:
            table = self._queue_table(name)
            if not self._table_exists(conn, table):
                return 0

            with self._tx(conn, "IMMEDIATE"):
                self._ensure_queue_table(conn, table)

                qt_list = [qt for qt in pandora.iterable(qtypes)]
                for qt in qt_list:
                    if qt not in self.VALID_QTYPES:
                        raise ValueError(f"invalid qtype: {qt}")

                if backup is not None and backup not in self.VALID_QTYPES:
                    raise ValueError(f"invalid backup qtype: {backup}")

                removed = 0
                now = time.time()

                for qt in qt_list:
                    for v in values:
                        if backup:
                            conn.execute(
                                f"""
                                INSERT OR IGNORE INTO "{table}" (qtype, value, priority, created_at, updated_at)
                                SELECT ?, value, 0, created_at, ?
                                FROM "{table}"
                                WHERE qtype = ? AND value = ?
                                """,
                                (backup, now, qt, v),
                            )
                        cur = conn.execute(
                            f'DELETE FROM "{table}" WHERE qtype = ? AND value = ?',
                            (qt, v),
                        )
                        removed += int(cur.rowcount or 0)

                return removed

    def replace(self, name: str, *values, **kwargs) -> int:
        if not name or not values:
            return 0

        qtypes = kwargs.pop("qtypes", ("current", "temp", "failure"))
        qt_list = [qt for qt in pandora.iterable(qtypes)]
        for qt in qt_list:
            if qt not in self.VALID_QTYPES:
                raise ValueError(f"invalid qtype: {qt}")

        with self._connection() as conn:
            table = self._queue_table(name)
            if not self._table_exists(conn, table):
                return 0

            with self._tx(conn, "IMMEDIATE"):
                self._ensure_queue_table(conn, table)
                now = time.time()
                changed = 0

                for old, new in values:
                    old_v = old if isinstance(old, str) else json.dumps(old, ensure_ascii=False, default=str)
                    new_v = new if isinstance(new, str) else json.dumps(new, ensure_ascii=False, default=str)

                    for qt in qt_list:
                        row = conn.execute(
                            f'SELECT priority, created_at FROM "{table}" WHERE qtype = ? AND value = ?',
                            (qt, old_v),
                        ).fetchone()
                        if not row:
                            continue

                        conn.execute(
                            f'DELETE FROM "{table}" WHERE qtype = ? AND value = ?',
                            (qt, old_v),
                        )
                        before = conn.total_changes
                        conn.execute(
                            f"""
                            INSERT OR IGNORE INTO "{table}" (qtype, value, priority, created_at, updated_at)
                            VALUES (?, ?, ?, ?, ?)
                            """,
                            (qt, new_v, int(row[0]), float(row[1]), now),
                        )
                        changed += max(conn.total_changes - before, 0)

                return changed

    def reverse(self, name: str, **kwargs) -> bool:
        qtypes = kwargs.pop("qtypes", ("temp", "failure"))
        qt_list = [qt for qt in pandora.iterable(qtypes)]
        for qt in qt_list:
            if qt not in self.VALID_QTYPES:
                raise ValueError(f"invalid qtype: {qt}")
            if qt == "current":
                raise ValueError("reverse qtypes cannot include 'current'")

        with self._connection() as conn:
            table = self._queue_table(name)
            if not self._table_exists(conn, table):
                return False

            with self._tx(conn, "IMMEDIATE"):
                self._ensure_queue_table(conn, table)
                now = time.time()
                placeholders = ",".join("?" for _ in qt_list)
                before = conn.total_changes

                conn.execute(
                    f"""
                    INSERT OR IGNORE INTO "{table}" (qtype, value, priority, created_at, updated_at)
                    SELECT 'current', value, 0, created_at, ?
                    FROM "{table}"
                    WHERE qtype IN ({placeholders})
                    """,
                    (now, *qt_list),
                )
                conn.execute(
                    f'DELETE FROM "{table}" WHERE qtype IN ({placeholders})',
                    (*qt_list,),
                )
                return (conn.total_changes - before) > 0

    def smart_reverse(self, name: str, status=0) -> bool:
        tc = self.size(name, qtypes=("temp",))
        cc = self.size(name, qtypes=("current",))
        fc = self.size(name, qtypes=("failure",))

        if cc == 0 and fc != 0:
            return bool(self.reverse(name, qtypes=("failure",)))
        if cc == 0 and fc == 0 and tc != 0 and int(status) == 0:
            return bool(self.reverse(name, qtypes=("temp",)))
        return False

    def merge(self, dest: str, *queues: str, **kwargs):
        qtypes = kwargs.pop("qtypes", ("current", "temp", "failure"))
        clear_source = bool(kwargs.pop("clear_source", True))

        qt_list = [qt for qt in pandora.iterable(qtypes)]
        for qt in qt_list:
            if qt not in self.VALID_QTYPES:
                raise ValueError(f"invalid qtype: {qt}")

        with self._connection() as conn:
            with self._tx(conn, "IMMEDIATE"):
                dest_table = self._queue_table(dest)
                self._ensure_queue_table(conn, dest_table)

                moved = 0
                for src in queues:
                    if not src or src == dest:
                        continue
                    src_table = self._queue_table(src)
                    if not self._table_exists(conn, src_table):
                        continue
                    self._ensure_queue_table(conn, src_table)

                    placeholders = ",".join("?" for _ in qt_list)
                    now = time.time()
                    before = conn.total_changes
                    conn.execute(
                        f"""
                        INSERT OR IGNORE INTO "{dest_table}" (qtype, value, priority, created_at, updated_at)
                        SELECT qtype, value, priority, created_at, ?
                        FROM "{src_table}"
                        WHERE qtype IN ({placeholders})
                        """,
                        (now, *qt_list),
                    )
                    moved += conn.total_changes - before

                    if clear_source:
                        conn.execute(
                            f'DELETE FROM "{src_table}" WHERE qtype IN ({placeholders})',
                            (*qt_list,),
                        )

                return moved

    def clear(self, *names, qtypes=("current", "temp", "failure", "lock", "record"), **kwargs):
        if not names:
            return 0

        qtypes = tuple(pandora.iterable(qtypes))

        with self._connection() as conn:
            with self._tx(conn, "IMMEDIATE"):
                cleared = 0
                for name in names:
                    table = self._queue_table(name)
                    if self._table_exists(conn, table):
                        self._ensure_queue_table(conn, table)
                        for qt in qtypes:
                            if qt in self.VALID_QTYPES:
                                cur = conn.execute(f'DELETE FROM "{table}" WHERE qtype = ?', (qt,))
                                cleared += int(cur.rowcount or 0)

                    if "record" in qtypes:
                        cur = conn.execute("DELETE FROM _queue_records WHERE queue_name = ?", (name,))
                        cleared += int(cur.rowcount or 0)

                    if "lock" in qtypes:
                        cur = conn.execute("DELETE FROM _queue_init_lock WHERE queue_name = ?", (name,))
                        cleared += int(cur.rowcount or 0)

                return cleared

    # ------------------------- Init / Record commands -------------------------

    def command(self, name: str, order: dict):
        action = order["action"]

        if action == self.COMMANDS.SET_RECORD:
            return self._cmd_set_record(name, order["record"])
        if action == self.COMMANDS.GET_RECORD:
            return self._cmd_get_record(name)
        if action == self.COMMANDS.CONTINUE_RECORD:
            return self.reverse(name)
        if action == self.COMMANDS.RESET_INIT:
            self.clear(name, qtypes=("current", "temp", "failure", "lock", "record"))
            return True
        if action == self.COMMANDS.WAIT_INIT:
            return self._cmd_wait_init(name, order)
        if action == self.COMMANDS.SET_INIT:
            return self._cmd_set_init(name)
        if action == self.COMMANDS.IS_INIT:
            return self._cmd_is_init(name)
        if action == self.COMMANDS.RELEASE_INIT:
            return self._cmd_release_init(name, order)
        if action == self.COMMANDS.GET_PERMISSION:
            return self._cmd_get_permission(name, order)

    def _cmd_set_record(self, name: str, record: dict) -> None:
        with self._connection() as conn:
            with self._tx(conn, "IMMEDIATE"):
                self._cleanup_expired_records(conn, name)
                now = time.time()
                row = conn.execute(
                    "SELECT record_data FROM _queue_records WHERE queue_name = ?",
                    (name,),
                ).fetchone()
                merged = self._parse_record(row[0]) if row else {}
                merged.update(record or {})
                data = json.dumps(merged, default=str, ensure_ascii=False)
                conn.execute(
                    """
                    INSERT INTO _queue_records(queue_name, record_data, updated_at, expires_at)
                    VALUES(?, ?, ?, COALESCE((SELECT expires_at FROM _queue_records WHERE queue_name=?), NULL))
                    ON CONFLICT(queue_name) DO UPDATE SET
                        record_data=excluded.record_data,
                        updated_at=excluded.updated_at
                    """,
                    (name, data, now, name),
                )

    def _cmd_get_record(self, name: str) -> dict:
        with self._connection() as conn:
            with self._tx(conn, "IMMEDIATE"):
                self._cleanup_expired_records(conn, name)
                row = conn.execute(
                    "SELECT record_data FROM _queue_records WHERE queue_name = ?",
                    (name,),
                ).fetchone()
                if not row:
                    return {}

                record = self._parse_record(row[0])
                if record.get("status") == 0:
                    conn.execute("DELETE FROM _queue_records WHERE queue_name = ?", (name,))
                    return {}
                return record

    def _cmd_set_init(self, name: str) -> bool:
        now_ms = int(time.time() * 1000)
        with self._connection() as conn:
            with self._tx(conn, "IMMEDIATE"):
                self._cleanup_expired_records(conn, name)
                row = conn.execute(
                    "SELECT record_data FROM _queue_records WHERE queue_name = ?",
                    (name,),
                ).fetchone()
                record = self._parse_record(row[0]) if row else {}
                record.update({"time": now_ms, "status": 1})
                now = time.time()
                conn.execute(
                    """
                    INSERT INTO _queue_records(queue_name, record_data, updated_at, expires_at)
                    VALUES(?, ?, ?, NULL)
                    ON CONFLICT(queue_name) DO UPDATE SET
                        record_data=excluded.record_data,
                        updated_at=excluded.updated_at
                    """,
                    (name, json.dumps(record, default=str, ensure_ascii=False), now),
                )
                return True

    def _cmd_is_init(self, name: str) -> bool:
        if not self.is_empty(name):
            return True
        record = self._cmd_get_record(name)
        return bool(record and record.get("status") == 1)

    def _cmd_wait_init(self, name: str, order: dict) -> None:
        start_time = float(order.get("time") or 0)
        while True:
            if not self.is_empty(name):
                return
            record = self._cmd_get_record(name) or {}
            if int(record.get("status") or 0) == 1:
                return
            t2 = record.get("time")
            if t2 and float(t2) >= start_time:
                return
            logger.debug("等待初始化开始")
            time.sleep(1)

    def _cmd_release_init(self, name: str, order: dict) -> bool:
        record_ttl = float(order.get("record_ttl", -1))
        with self._connection() as conn:
            with self._tx(conn, "IMMEDIATE"):
                now = time.time()
                row = conn.execute(
                    "SELECT record_data FROM _queue_records WHERE queue_name = ?",
                    (name,),
                ).fetchone()
                record = self._parse_record(row[0]) if row else {}
                record["status"] = 0
                record["stop_heartbeat"] = 1

                if record_ttl == 0:
                    conn.execute("DELETE FROM _queue_records WHERE queue_name = ?", (name,))
                else:
                    expires_at = None
                    if record_ttl > 0:
                        expires_at = now + record_ttl
                    conn.execute(
                        """
                        INSERT INTO _queue_records(queue_name, record_data, updated_at, expires_at)
                        VALUES(?, ?, ?, ?)
                        ON CONFLICT(queue_name) DO UPDATE SET
                            record_data=excluded.record_data,
                            updated_at=excluded.updated_at,
                            expires_at=excluded.expires_at
                        """,
                        (name, json.dumps(record, default=str, ensure_ascii=False), now, expires_at),
                    )

                conn.execute("DELETE FROM _queue_init_lock WHERE queue_name = ?", (name,))
                return True

    def _cmd_get_permission(self, name: str, order: dict) -> dict:
        interval = float(order.get("interval", 10) or 10)
        machine_id = getattr(state, "MACHINE_ID", "unknown")

        with self._connection() as conn:
            with self._tx(conn, "IMMEDIATE"):
                self._cleanup_expired_records(conn, name)

                queue_size = self.size(name, qtypes=self.VALID_QTYPES)
                row = conn.execute(
                    "SELECT record_data FROM _queue_records WHERE queue_name = ?",
                    (name,),
                ).fetchone()
                record = self._parse_record(row[0]) if row else {}
                status = int(record.get("status") or 0)

                if status != 1 and queue_size > 0:
                    return {"state": False, "msg": "已投完且存在种子没有消费完毕"}

                now = time.time()
                row = conn.execute(
                    "SELECT owner, heartbeat, interval FROM _queue_init_lock WHERE queue_name = ?",
                    (name,),
                ).fetchone()

                acquired = False
                if not row:
                    conn.execute(
                        "INSERT INTO _queue_init_lock(queue_name, owner, heartbeat, interval) VALUES(?, ?, ?, ?)",
                        (name, machine_id, now, interval),
                    )
                    acquired = True
                else:
                    owner, heartbeat, last_interval = row
                    expire_before = now - max(float(last_interval), interval) * 3
                    if owner == machine_id or float(heartbeat) <= expire_before:
                        conn.execute(
                            "UPDATE _queue_init_lock SET owner = ?, heartbeat = ?, interval = ? WHERE queue_name = ?",
                            (machine_id, now, interval, name),
                        )
                        acquired = True

                if acquired:
                    self._start_heartbeat(name, interval=interval, owner=machine_id)
                    return {"state": True, "msg": "成功获取权限"}

                return {"state": False, "msg": "存在其他活跃的初始化机器"}

    def _start_heartbeat(self, name: str, interval: float, owner: str) -> None:
        def heartbeat():
            while True:
                try:
                    with self._connection() as conn:
                        with self._tx(conn, "IMMEDIATE"):
                            row = conn.execute(
                                "SELECT owner FROM _queue_init_lock WHERE queue_name = ?",
                                (name,),
                            ).fetchone()
                            if not row or row[0] != owner:
                                return

                            record_row = conn.execute(
                                "SELECT record_data FROM _queue_records WHERE queue_name = ?",
                                (name,),
                            ).fetchone()
                            if record_row:
                                record = self._parse_record(record_row[0])
                                if str(record.get("stop_heartbeat") or "0") == "1":
                                    record.pop("stop_heartbeat", None)
                                    conn.execute(
                                        "UPDATE _queue_records SET record_data = ?, updated_at = ? WHERE queue_name = ?",
                                        (
                                            json.dumps(record, default=str, ensure_ascii=False),
                                            time.time(),
                                            name,
                                        ),
                                    )
                                    return

                            conn.execute(
                                "UPDATE _queue_init_lock SET heartbeat = ?, interval = ? WHERE queue_name = ? AND owner = ?",
                                (time.time(), interval, name, owner),
                            )
                    time.sleep(max(interval - 1, 1))
                except (KeyboardInterrupt, SystemExit):
                    raise
                except Exception as e:
                    logger.error(f"[sqlite-queue heartbeat] {e}")
                    time.sleep(1)

        t = threading.Thread(target=heartbeat, daemon=True, name=f"SQLiteQueueHeartbeat:{name}")
        t.start()

    # ------------------------- Lifecycle -------------------------

    def close(self) -> None:
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            try:
                conn.close()
            finally:
                self._local.conn = None

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass
