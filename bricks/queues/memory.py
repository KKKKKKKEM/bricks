"""进程内种子任务队列，使用互斥锁保证状态迁移原子性。"""

from __future__ import annotations

import heapq
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
        _lock: 保护任务、状态计数及索引的互斥锁。
        _ready: 按就绪时间排序的惰性失效堆。
        _active: 按租约截止时间排序的惰性失效堆。
        _counts: 各任务状态的常数时间计数。
        _sequence: 为堆项分配的递增代数。
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
        self._ready: list[tuple[float, int, str]] = []
        self._active: list[tuple[float, int, str]] = []
        self._counts = {"pending": 0, "leased": 0, "acked": 0, "dead": 0}
        self._sequence = 0

    def _status(self, record: dict[str, Any], status: str) -> None:
        """更新状态并维护常数时间计数。

        Args:
            record: 队列内部任务记录。
            status: 新状态。
        """
        self._counts[record["status"]] -= 1
        record["status"] = status
        self._counts[status] += 1

    def _index_ready(self, record: dict[str, Any], task_id: str, ready: float) -> None:
        """加入就绪堆并使旧索引失效。

        Args:
            record: 内部任务记录。
            task_id: 队列任务 ID。
            ready: 可领取时间。
        """
        self._sequence += 1
        record["ready"] = ready
        record["ready_seq"] = self._sequence
        heapq.heappush(self._ready, (ready, self._sequence, task_id))

    def _index_active(self, record: dict[str, Any], task_id: str) -> None:
        """加入租约到期堆并使旧心跳索引失效。

        Args:
            record: 内部任务记录。
            task_id: 队列任务 ID。
        """
        self._sequence += 1
        record["lease_seq"] = self._sequence
        heapq.heappush(self._active, (record["deadline"], self._sequence, task_id))

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
        if op in ("put", "put_claim"):
            if (
                op == "put"
                and args["init"] is not None
                and not self._valid(self._init, args["init"], now, initial=True)
            ):
                return {"failure": "lease"}
            if record is not None:
                if record["body"] != args["body"]:
                    return {"failure": "conflict"}
                if op == "put":
                    return False
                if record["status"] == "leased" and record["deadline"] <= now:
                    status = (
                        "dead" if record["attempt"] >= record["maximum"] else "pending"
                    )
                    self._status(record, status)
                    record.update(
                        ready=now,
                        token="",
                        owner="",
                        deadline=0,
                        error="delivery lease expired",
                    )
                    if status == "pending":
                        self._index_ready(record, args["id"], now)
                if record["status"] != "pending" or record["ready"] > now:
                    return None
            else:
                record = self._records[args["id"]] = {
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
                self._counts["pending"] += 1
                self._index_ready(record, args["id"], now)
                if op == "put":
                    return True
            self._status(record, "leased")
            record.update(
                attempt=record["attempt"] + 1,
                owner=args["owner"],
                token=args["token"],
                deadline=now + args["ttl"],
            )
            self._index_active(record, args["id"])
            return dict(record, _id=args["id"])
        if op == "claim":
            recovered = 0
            while self._active and self._active[0][0] <= now and recovered < 100:
                deadline, sequence, task_id = heapq.heappop(self._active)
                item = self._records.get(task_id)
                if (
                    item is None
                    or item["status"] != "leased"
                    or item["deadline"] != deadline
                    or item.get("lease_seq") != sequence
                ):
                    continue
                status = "dead" if item["attempt"] >= item["maximum"] else "pending"
                self._status(item, status)
                item.update(
                    ready=now,
                    token="",
                    owner="",
                    deadline=0,
                    error="delivery lease expired",
                )
                if status == "pending":
                    self._index_ready(item, task_id, now)
                recovered += 1
            record = None
            task_id = ""
            while self._ready and self._ready[0][0] <= now:
                ready_at, sequence, candidate_id = heapq.heappop(self._ready)
                candidate = self._records.get(candidate_id)
                if (
                    candidate is not None
                    and candidate["status"] == "pending"
                    and candidate["ready"] == ready_at
                    and candidate.get("ready_seq") == sequence
                ):
                    record = candidate
                    task_id = candidate_id
                    break
            if record is None:
                return None
            self._status(record, "leased")
            record.update(
                attempt=record["attempt"] + 1,
                owner=args["owner"],
                token=args["token"],
                deadline=now + args["ttl"],
            )
            self._index_active(record, task_id)
            return dict(record, _id=task_id)
        if op == "get":
            return record
        if op == "counts":
            return dict(self._counts)
        if op == "forget":
            if record is None:
                return False
            if record["status"] not in ("acked", "dead"):
                return {"failure": "only terminal tasks can be forgotten"}
            self._counts[record["status"]] -= 1
            del self._records[args["id"]]
            return True
        if op in ("renew", "ack", "retry", "reject"):
            if not self._valid(record, args, now):
                return {"failure": "lease"}
            assert record is not None
            if op == "renew":
                record["deadline"] = now + args["ttl"]
                self._index_active(record, args["id"])
                return record
            if op == "ack":
                self._status(record, "acked")
                record["result"] = args["result"]
            else:
                retry = op == "retry" and record["attempt"] < record["maximum"]
                status = "pending" if retry else "dead"
                self._status(record, status)
                record.update(
                    ready=now + args.get("delay", 0),
                    error=args["error"],
                )
                if status == "pending":
                    self._index_ready(record, args["id"], record["ready"])
            record.update(token="", owner="", deadline=0)
            return record["status"] if op == "retry" else None
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
