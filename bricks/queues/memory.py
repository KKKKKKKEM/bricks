"""进程内种子任务队列，使用互斥锁保证状态迁移原子性。"""

from __future__ import annotations

from collections.abc import Callable
from copy import deepcopy
from threading import RLock
from time import monotonic
from typing import Any

from .base import _Queue


class MemoryTaskQueue(_Queue):
    """本地任务存储；线程间可共享，进程结束后数据丢失。

    Attributes:
        _records: ID 对应的内部任务记录，不返回给调用方。
        _init: 当前初始化状态；尚未开始时为 None。
        _clock: 单调秒时钟，测试可注入可控时钟。
        _lock: 保护所有任务、初始化状态及计数的互斥锁。
    """

    def __init__(
        self,
        name: str,
        *,
        max_attempts: int = 3,
        clock: Callable[[], float] = monotonic,
    ) -> None:
        """创建独立的具名内存存储。

        Args:
            name: 队列名，相同名称的不同实例仍各自独立。
            max_attempts: 最大领取次数，默认 3。
            clock: 秒时钟，默认单调时钟。

        Raises:
            TypeError: 时钟不可调用。
        """
        super().__init__(name, max_attempts)
        if not callable(clock):
            raise TypeError("clock must be callable")
        self._records: dict[str, dict[str, Any]] = {}
        self._init: dict[str, Any] | None = None
        self._clock = clock
        self._lock = RLock()

    def _call(self, operation: str, **args: Any) -> Any:
        """在单一临界区迁移状态，并复制返回记录。

        Args:
            operation: 内部操作名。
            args: 已校验参数。

        Returns:
            独立的原始操作结果。
        """
        with self._lock:
            return deepcopy(self._apply(operation, args, self._clock()))

    @staticmethod
    def _valid(
        record: dict[str, Any] | None,
        credentials: dict[str, Any],
        now: float,
        *,
        initial: bool = False,
    ) -> bool:
        """检查租约身份、状态和到期时间。

        Args:
            record: 当前存储记录。
            credentials: 调用方凭据。
            now: 当前存储时间。
            initial: 是否校验初始化租约。

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

    def _apply(self, op: str, args: dict[str, Any], now: float) -> Any:
        """执行持锁状态机；错误返回标记而不部分修改任务。

        Args:
            op: 操作名称。
            args: 已校验参数。
            now: 当前存储时间。

        Returns:
            操作结果或领域错误标记。

        Raises:
            ValueError: 内部操作名未知。
        """
        record = self._records.get(args.get("id", ""))
        if op == "put":
            if args["init"] is not None and not self._valid(
                self._init, args["init"], now, initial=True
            ):
                return {"failure": "lease"}
            if record is not None:
                return (
                    False if record["body"] == args["body"] else {"failure": "conflict"}
                )
            self._records[args["id"]] = {
                "body": args["body"],
                "status": "pending",
                "attempt": 0,
                "maximum": args["maximum"],
                "ready": now,
                "deadline": 0,
                "token": "",
                "owner": "",
                "error": "",
                "result": None,
            }
            return True
        if op == "claim":
            expired = [
                item
                for item in self._records.values()
                if item["status"] == "leased" and item["deadline"] <= now
            ][:100]
            for item in expired:
                item.update(
                    status="dead" if item["attempt"] >= item["maximum"] else "pending",
                    ready=now,
                    token="",
                    owner="",
                    deadline=0,
                    error="delivery lease expired",
                )
            ready = [
                item
                for item in self._records.values()
                if item["status"] == "pending" and item["ready"] <= now
            ]
            if not ready:
                return None
            record = min(ready, key=lambda item: (item["ready"], item["body"]))
            record.update(
                status="leased",
                attempt=record["attempt"] + 1,
                owner=args["owner"],
                token=args["token"],
                deadline=now + args["ttl"],
            )
            return record
        if op == "get":
            return record
        if op == "counts":
            return {
                status: sum(item["status"] == status for item in self._records.values())
                for status in ("pending", "leased", "acked", "dead")
            }
        if op == "forget":
            if record is None:
                return False
            if record["status"] not in ("acked", "dead"):
                return {"failure": "only terminal tasks can be forgotten"}
            del self._records[args["id"]]
            return True
        if op in ("renew", "ack", "retry", "reject"):
            if not self._valid(record, args, now):
                return {"failure": "lease"}
            assert record is not None
            if op == "renew":
                record["deadline"] = now + args["ttl"]
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
            record.update(token="", owner="", deadline=0)
            return None
        if op == "initialization":
            return self._init
        if op == "acquire_init":
            previous = self._init
            if previous is not None and (
                previous["status"] == "succeeded"
                or (previous["status"] == "running" and previous["deadline"] > now)
            ):
                return None
            self._init = {
                "owner": args["owner"],
                "token": args["token"],
                "deadline": now + args["ttl"],
                "fence": 1 if previous is None else previous["fence"] + 1,
                "progress": "null" if previous is None else previous["progress"],
                "status": "running",
                "error": "",
            }
            return self._init
        if op in ("renew_init", "save_init", "finish_init"):
            if not self._valid(self._init, args, now, initial=True):
                return {"failure": "lease"}
            assert self._init is not None
            if op == "renew_init":
                self._init["deadline"] = now + args["ttl"]
                return self._init
            if op == "save_init":
                self._init["progress"] = args["progress"]
            else:
                self._init.update(
                    status="succeeded" if args["success"] else "failed",
                    error=args["error"],
                )
            return None
        raise ValueError(f"unknown queue operation: {op}")
