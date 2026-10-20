"""以 SQLite 文件事务实现的单机持久任务存储，同机多进程可共享。"""

from __future__ import annotations

import os
import sqlite3
import time
from contextlib import closing
from typing import Any

from .base import _Queue
from .models import seconds

_COLUMNS = (
    "id, body, status, attempt, maximum, ready, deadline, token, owner, error, result"
)
_SCHEMA = (
    """
    CREATE TABLE IF NOT EXISTS bricks_tasks (
        queue TEXT NOT NULL,
        id TEXT NOT NULL,
        body TEXT NOT NULL,
        status TEXT NOT NULL,
        attempt INTEGER NOT NULL,
        maximum INTEGER NOT NULL,
        ready REAL NOT NULL,
        deadline REAL NOT NULL,
        token TEXT NOT NULL,
        owner TEXT NOT NULL,
        error TEXT NOT NULL,
        result TEXT,
        PRIMARY KEY (queue, id)
    )
    """,
    "CREATE INDEX IF NOT EXISTS bricks_tasks_ready ON bricks_tasks (queue, status, ready)",
    "CREATE INDEX IF NOT EXISTS bricks_tasks_deadline"
    " ON bricks_tasks (queue, status, deadline)",
    """
    CREATE TABLE IF NOT EXISTS bricks_task_init (
        queue TEXT PRIMARY KEY,
        owner TEXT NOT NULL,
        token TEXT NOT NULL,
        deadline REAL NOT NULL,
        fence INTEGER NOT NULL,
        progress TEXT NOT NULL,
        status TEXT NOT NULL,
        error TEXT NOT NULL
    )
    """,
)


class SqliteTaskQueue(_Queue):
    """借助 SQLite 文件持久化任务，进程退出或重启后任务与初始化断点仍保留。

    每次操作打开独立连接并在 ``BEGIN IMMEDIATE`` 事务内完成校验与迁移，同一文件可由同机多个进程
    和线程共享；租约时间使用本机墙上时钟，不支持跨机器共享或网络文件系统。终态保留去重身份，
    需显式 forget。持久性取决于 SQLite 的同步配置与磁盘，不保证外部副作用恰好执行一次。

    Attributes:
        _path: 数据库文件路径，构造时固定。
        _timeout: 等待其他进程释放写锁的秒数。
    """

    def __init__(
        self,
        path: str | os.PathLike[str],
        name: str,
        *,
        max_attempts: int = 3,
        timeout: float = 30,
    ) -> None:
        """打开或创建数据库文件中的任务表。

        Args:
            path: 数据库文件路径；不接受内存数据库。
            name: 队列名，同一文件中按名称隔离。
            max_attempts: 新任务的最大交付次数，默认 3，随任务保存。
            timeout: 等待写锁的秒数，默认 30。

        Raises:
            TypeError: 路径类型错误。
            ValueError: 路径为空或为内存数据库，或配置非法。
            sqlite3.Error: 数据库无法打开或建表。
        """
        super().__init__(name, max_attempts)
        location = os.fspath(path)
        if not isinstance(location, str):
            raise TypeError("path must be a filesystem path")
        if location in ("", ":memory:") or location.startswith("file::memory:"):
            raise ValueError("SqliteTaskQueue requires a database file")
        self._path = location
        self._timeout = seconds(timeout)
        with closing(self._connect()) as connection:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute("BEGIN IMMEDIATE")
            for statement in _SCHEMA:
                connection.execute(statement)
            connection.execute("COMMIT")

    def _connect(self) -> sqlite3.Connection:
        """打开一次操作使用的自动提交连接，事务由调用方显式开始。

        Returns:
            新连接，调用方负责关闭。
        """
        connection = sqlite3.connect(
            self._path, timeout=self._timeout, isolation_level=None
        )
        connection.row_factory = sqlite3.Row
        return connection

    def _call(self, operation: str, **args: Any) -> Any:
        """在单个写事务内完成校验与迁移，失败时回滚。

        Args:
            operation: 内部操作名。
            args: 已校验参数，业务 JSON 作为不透明文本保存。

        Returns:
            独立的操作结果。
        """
        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                result = self._apply(connection, operation, args, time.time())
            except BaseException:
                connection.execute("ROLLBACK")
                raise
            connection.execute("COMMIT")
            return result

    def _record(
        self, connection: sqlite3.Connection, task_id: str
    ) -> dict[str, Any] | None:
        """读取任务记录，ID 以 _id 返回。

        Returns:
            记录副本或 None。
        """
        row = connection.execute(
            f"SELECT {_COLUMNS} FROM bricks_tasks WHERE queue = ? AND id = ?",
            (self.name, task_id),
        ).fetchone()
        return None if row is None else self._decode(row)

    @staticmethod
    def _decode(row: sqlite3.Row) -> dict[str, Any]:
        """把数据库行转换为与其他存储一致的记录字典。

        Returns:
            含 _id 的记录。
        """
        record = dict(row)
        record["_id"] = record.pop("id")
        return record

    def _write(self, connection: sqlite3.Connection, record: dict[str, Any]) -> None:
        """写回记录的可变字段。"""
        connection.execute(
            "UPDATE bricks_tasks SET status = ?, attempt = ?, ready = ?, deadline = ?,"
            " token = ?, owner = ?, error = ?, result = ? WHERE queue = ? AND id = ?",
            (
                record["status"],
                record["attempt"],
                record["ready"],
                record["deadline"],
                record["token"],
                record["owner"],
                record["error"],
                record["result"],
                self.name,
                record["_id"],
            ),
        )

    @staticmethod
    def _valid(
        record: dict[str, Any] | None,
        credentials: dict[str, Any],
        now: float,
        *,
        initial: bool = False,
    ) -> bool:
        """检查租约身份、状态和到期时间。

        Returns:
            凭据是否仍有写入权。
        """
        return bool(
            record is not None
            and record["status"] == ("running" if initial else "leased")
            and record["deadline"] > now
            and record["token"] == credentials["token"]
            and record["owner"] == credentials["owner"]
            and (not initial or record["fence"] == credentials["fence"])
        )

    @staticmethod
    def _expire(record: dict[str, Any], now: float) -> None:
        """回收过期交付：次数耗尽进入 dead，否则立即可再领取。"""
        record.update(
            status="dead" if record["attempt"] >= record["maximum"] else "pending",
            ready=now,
            token="",
            owner="",
            deadline=0.0,
            error="delivery lease expired",
        )

    def _lease(
        self,
        connection: sqlite3.Connection,
        record: dict[str, Any],
        args: dict[str, Any],
        now: float,
    ) -> dict[str, Any]:
        """把 pending 记录交给新的租约持有者。

        Returns:
            含新凭据的记录。
        """
        record.update(
            status="leased",
            attempt=record["attempt"] + 1,
            owner=args["owner"],
            token=args["token"],
            deadline=now + args["ttl"],
        )
        self._write(connection, record)
        return record

    def _revive(
        self, connection: sqlite3.Connection, record: dict[str, Any], now: float
    ) -> None:
        """把 dead 记录恢复为立即可领取，交付次数清零。"""
        record.update(status="pending", attempt=0, error="", result=None, ready=now)
        self._write(connection, record)

    def _initial(self, connection: sqlite3.Connection) -> dict[str, Any] | None:
        """读取初始化状态。

        Returns:
            初始化记录或 None。
        """
        row = connection.execute(
            "SELECT owner, token, deadline, fence, progress, status, error"
            " FROM bricks_task_init WHERE queue = ?",
            (self.name,),
        ).fetchone()
        return None if row is None else dict(row)

    def _save_initial(
        self, connection: sqlite3.Connection, initial: dict[str, Any]
    ) -> None:
        """写入或替换初始化状态。"""
        connection.execute(
            "INSERT OR REPLACE INTO bricks_task_init"
            " (queue, owner, token, deadline, fence, progress, status, error)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                self.name,
                initial["owner"],
                initial["token"],
                initial["deadline"],
                initial["fence"],
                initial["progress"],
                initial["status"],
                initial["error"],
            ),
        )

    def _apply(
        self,
        connection: sqlite3.Connection,
        op: str,
        args: dict[str, Any],
        now: float,
    ) -> Any:
        """在写事务内执行状态机；错误返回标记而不修改任务。

        Args:
            connection: 已开始写事务的连接。
            op: 操作名称。
            args: 已校验参数。
            now: 当前墙上时钟秒数。

        Returns:
            操作结果或领域错误标记。

        Raises:
            ValueError: 内部操作名未知。
        """
        record = self._record(connection, args["id"]) if "id" in args else None
        if op in ("put", "put_claim"):
            if (
                op == "put"
                and args["init"] is not None
                and not self._valid(
                    self._initial(connection), args["init"], now, initial=True
                )
            ):
                return {"failure": "lease"}
            if record is not None:
                if record["body"] != args["body"]:
                    return {"failure": "conflict"}
                if op == "put":
                    return False
                if record["status"] == "leased" and record["deadline"] <= now:
                    self._expire(record, now)
                    self._write(connection, record)
                if record["status"] != "pending" or record["ready"] > now:
                    return None
            else:
                record = {
                    "_id": args["id"],
                    "body": args["body"],
                    "status": "pending",
                    "attempt": 0,
                    "maximum": args["maximum"],
                    "ready": now,
                    "deadline": 0.0,
                    "token": "",
                    "owner": "",
                    "error": "",
                    "result": None,
                }
                connection.execute(
                    f"INSERT INTO bricks_tasks (queue, {_COLUMNS})"
                    " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        self.name,
                        args["id"],
                        args["body"],
                        "pending",
                        0,
                        args["maximum"],
                        now,
                        0.0,
                        "",
                        "",
                        "",
                        None,
                    ),
                )
                if op == "put":
                    return True
            return self._lease(connection, record, args, now)
        if op == "claim":
            expired = connection.execute(
                f"SELECT {_COLUMNS} FROM bricks_tasks WHERE queue = ?"
                " AND status = 'leased' AND deadline <= ? ORDER BY deadline LIMIT 100",
                (self.name, now),
            ).fetchall()
            for row in expired:
                item = self._decode(row)
                self._expire(item, now)
                self._write(connection, item)
            row = connection.execute(
                f"SELECT {_COLUMNS} FROM bricks_tasks WHERE queue = ?"
                " AND status = 'pending' AND ready <= ? ORDER BY ready, rowid LIMIT 1",
                (self.name, now),
            ).fetchone()
            if row is None:
                return None
            return self._lease(connection, self._decode(row), args, now)
        if op == "get":
            return record
        if op == "counts":
            counts = {"pending": 0, "leased": 0, "acked": 0, "dead": 0}
            for status, count in connection.execute(
                "SELECT status, COUNT(*) FROM bricks_tasks WHERE queue = ?"
                " GROUP BY status",
                (self.name,),
            ):
                counts[status] = count
            return counts
        if op == "forget":
            if record is None:
                return False
            if record["status"] not in ("acked", "dead"):
                return {"failure": "only terminal tasks can be forgotten"}
            connection.execute(
                "DELETE FROM bricks_tasks WHERE queue = ? AND id = ?",
                (self.name, args["id"]),
            )
            return True
        if op == "revive":
            if record is None or record["status"] != "dead":
                return False
            self._revive(connection, record, now)
            return True
        if op == "revive_dead":
            rows = connection.execute(
                f"SELECT {_COLUMNS} FROM bricks_tasks WHERE queue = ?"
                " AND status = 'dead' ORDER BY rowid LIMIT ?",
                (self.name, args["limit"]),
            ).fetchall()
            for row in rows:
                self._revive(connection, self._decode(row), now)
            return len(rows)
        if op in ("renew", "ack", "retry", "reject"):
            if not self._valid(record, args, now):
                return {"failure": "lease"}
            assert record is not None
            if op == "renew":
                record["deadline"] = now + args["ttl"]
                self._write(connection, record)
                return record
            if op == "ack":
                record.update(status="acked", result=args["result"])
            else:
                retry = op == "retry" and record["attempt"] < record["maximum"]
                record.update(
                    status="pending" if retry else "dead",
                    ready=now + args.get("delay", 0),
                    error=args["error"],
                )
            record.update(token="", owner="", deadline=0.0)
            self._write(connection, record)
            return record["status"] if op == "retry" else None
        initial = self._initial(connection)
        if op == "initialization":
            return initial
        if op == "acquire_init":
            if initial is not None and (
                initial["status"] == "succeeded"
                or (initial["status"] == "running" and initial["deadline"] > now)
            ):
                return None
            initial = {
                "owner": args["owner"],
                "token": args["token"],
                "deadline": now + args["ttl"],
                "fence": 1 if initial is None else initial["fence"] + 1,
                "progress": "null" if initial is None else initial["progress"],
                "status": "running",
                "error": "",
            }
            self._save_initial(connection, initial)
            return initial
        if op in ("renew_init", "save_init", "finish_init"):
            if not self._valid(initial, args, now, initial=True):
                return {"failure": "lease"}
            assert initial is not None
            if op == "renew_init":
                initial["deadline"] = now + args["ttl"]
            elif op == "save_init":
                initial["progress"] = args["progress"]
            else:
                initial.update(
                    status="succeeded" if args["success"] else "failed",
                    error=args["error"],
                )
            self._save_initial(connection, initial)
            return initial if op == "renew_init" else None
        raise ValueError(f"unknown queue operation: {op}")
