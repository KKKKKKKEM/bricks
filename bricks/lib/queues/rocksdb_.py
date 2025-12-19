# -*- coding: utf-8 -*-
# @Time    : 2025-12-19
# @Author  : Kem
# @Desc    : 基于 RocksDB 的持久化任务队列
import uuid
from collections import defaultdict
from pathlib import Path
from threading import Event, Lock
from typing import List, Optional, Union

from bricks.db.rocksdb import RocksDB, WriteBatch
from bricks.lib.queues import TaskQueue
from bricks.utils import pandora

pandora.require("filelock")
from filelock import FileLock  # noqa: E402


class RocksQueue(TaskQueue):
    """
    基于 RocksDB 的持久化任务队列

    特性：
    - 单机持久化存储，程序重启可恢复
    - 多进程/多线程安全（文件锁 + WriteBatch 事务）
    - 三队列模型：current（待处理）、temp（处理中）、failure（失败）
    - 接口与 LocalQueue/RedisQueue 保持一致

    数据结构：
    - 每个队列项存储为：collection = "queue:{name}:{qtype}"
    - 使用 UUID 作为文档 ID，保证唯一性

    线程/进程安全：
    - 使用 filelock 的 FileLock 保护关键操作
    - 使用 WriteBatch 保证事务原子性
    """

    def __init__(self, db_path: Optional[str] = None):
        """
        初始化队列

        Args:
            db_path: 数据库路径，默认 ./rocks_queue_data
        """
        self.db_path = Path(db_path or "rocks_queue_data")
        self.db = RocksDB(str(self.db_path))

        # 多进程锁（文件锁）
        self._lock_file = self.db_path / "queue.lock"
        self._file_lock = FileLock(str(self._lock_file))

        # 多线程锁（内存锁）
        self._thread_lock = Lock()

        # 队列状态事件
        self._status = defaultdict(Event)

    def __str__(self):
        return f"<RocksQueue path={self.db_path}>"

    # ==================== 辅助方法 ====================

    @staticmethod
    def _make_collection(name: str, qtype: str = "current") -> str:
        """构建集合名：queue:{name}:{qtype}"""
        return f"queue:{name}:{qtype}"

    def _lock(self):
        """获取双重锁（线程锁 + 文件锁）"""
        return _DoubleLock(self._thread_lock, self._file_lock)

    # ==================== 核心队列操作 ====================

    def size(self, *names: str, qtypes: tuple = ("current", "temp", "failure"), **kwargs) -> int:
        """
        获取队列大小

        Args:
            names: 队列名称列表
            qtypes: 队列类型元组

        Returns:
            总文档数量
        """
        if not names:
            return 0

        total = 0
        for name in names:
            for qtype in qtypes:
                collection = self._make_collection(name, qtype)
                total += self.db.count(collection)

        return total

    def put(self, name: str, *values, **kwargs) -> int:
        """
        添加任务到队列

        Args:
            name: 队列名称
            values: 任务数据列表
            qtypes: 队列类型，默认 "current"
            limit: 限制数量（队列满时）

        Returns:
            实际添加的任务数量
        """
        if not values:
            return 0

        qtypes = kwargs.pop("qtypes", "current")
        limit = kwargs.pop("limit", 0)

        count = 0
        for qtype in pandora.iterable(qtypes):
            collection = self._make_collection(name, qtype)

            # 检查队列是否已满
            if limit > 0:
                current_size = self.db.count(collection)
                if current_size >= limit:
                    continue
                available = limit - current_size
                batch_values = list(values)[:available]
            else:
                batch_values = list(values)

            # 批量插入（自动生成 UUID）
            docs = [{"value": v} for v in batch_values]
            self.db.insert(collection, *docs, row_keys=None)

            count += len(batch_values)

            # 触发状态事件
            self._status[name].set()

        return count

    def get(self, name: str, count: int = 1, **kwargs) -> Optional[List[dict]]:
        """
        从队列获取任务（原子性地从 current 移动到 temp）

        Args:
            name: 队列名称
            count: 获取数量，默认 1
            **kwargs: 其他参数（block, timeout 等）

        Returns:
            任务列表（字典对象，由 TaskQueue 基类装饰器转换为 Item）
        """
        block = kwargs.pop("block", False)
        timeout = kwargs.pop("timeout", None)

        while True:
            # 使用双重锁保护读取和移动操作
            with self._lock():
                from_col = self._make_collection(name, "current")
                to_col = self._make_collection(name, "temp")

                # 1. 查找任务
                docs = self.db.find(from_col, limit=count)

                if not docs:
                    # 队列为空
                    if not block:
                        return None
                    # 阻塞模式：等待新任务
                    pass
                else:
                    # 2. 使用 WriteBatch 原子性移动
                    batch = WriteBatch()
                    items = []

                    for doc in docs:
                        # 找到源文档的 key
                        prefix = f"{from_col}:"
                        src_key = None

                        for key, value in self.db.db.items():
                            if isinstance(key, str) and key.startswith(prefix) and value == doc:
                                src_key = key
                                break

                        if src_key:
                            # 生成新的目标 key
                            dst_key = self.db._build_key(
                                to_col, str(uuid.uuid4()))

                            # 添加到事务
                            batch.put(dst_key, doc)
                            batch.delete(src_key)

                            # 返回原始字典（由 TaskQueue 基类的装饰器转换为 Item）
                            items.append(doc)

                    # 3. 执行事务
                    if items:
                        self.db.write_batch(batch)
                        return items

            # 阻塞模式：等待新任务
            if block:
                self._status[name].clear()
                if self._status[name].wait(timeout):
                    continue  # 有新任务，重试
                else:
                    return None  # 超时
            else:
                return None

    def remove(self, name: str, *values, **kwargs) -> int:
        """
        从队列中移除任务（支持备份到 failure）

        Args:
            name: 队列名称
            values: 要移除的任务值列表
            qtypes: 队列类型，默认 "temp"
            backup: 是否备份到 failure，默认 True

        Returns:
            移除的任务数量
        """
        if not values:
            return 0

        qtypes = kwargs.pop("qtypes", "temp")
        backup = kwargs.pop("backup", True)

        removed_count = 0

        for qtype in pandora.iterable(qtypes):
            with self._lock():
                from_col = self._make_collection(name, qtype)
                failure_col = self._make_collection(name, "failure")

                # 查找要移除的文档
                value_set = set(values)

                batch = WriteBatch()
                count = 0

                for key, doc in self.db.items(from_col):
                    if doc.get("value") in value_set:
                        # 删除源文档
                        batch.delete(key)
                        count += 1

                        # 备份到 failure 队列
                        if backup:
                            failure_key = self.db._build_key(
                                failure_col, str(uuid.uuid4()))
                            batch.put(failure_key, doc)

                # 执行事务
                if count > 0:
                    self.db.write_batch(batch)
                    removed_count += count

        return removed_count

    def replace(self, name: str, *values, **kwargs) -> int:
        """
        替换队列中的任务值

        Args:
            name: 队列名称
            values: [(old_value, new_value), ...] 替换对列表
            qtypes: 队列类型，默认 "current"

        Returns:
            替换的任务数量
        """
        if not values:
            return 0

        qtypes = kwargs.pop("qtypes", "current")
        replaced_count = 0

        # 构建替换映射
        replace_map = {}
        for item in values:
            if isinstance(item, (list, tuple)) and len(item) == 2:
                old_val, new_val = item
                replace_map[old_val] = new_val

        for qtype in pandora.iterable(qtypes):
            with self._lock():
                collection = self._make_collection(name, qtype)

                batch = WriteBatch()
                count = 0

                for key, doc in self.db.items(collection):
                    old_value = doc.get("value")
                    if old_value in replace_map:
                        doc["value"] = replace_map[old_value]
                        batch.put(key, doc)
                        count += 1

                if count > 0:
                    self.db.write_batch(batch)
                    replaced_count += count

        return replaced_count

    def reverse(self, name: str, *values, **kwargs) -> bool:
        """
        将任务从 temp/failure 逆向移回 current

        Args:
            name: 队列名称
            values: 要恢复的任务值列表（空则全部恢复）
            qtypes: 源队列类型，默认 ("temp", "failure")

        Returns:
            是否成功恢复
        """
        qtypes = kwargs.pop("qtypes", ("temp", "failure"))
        reversed_count = 0

        for qtype in pandora.iterable(qtypes):
            with self._lock():
                from_col = self._make_collection(name, qtype)
                to_col = self._make_collection(name, "current")

                # 查找要恢复的文档
                if values:
                    value_set = set(values)
                    def filter_func(doc): return doc.get("value") in value_set
                else:
                    def filter_func(doc): return True

                batch = WriteBatch()
                count = 0

                for key, doc in self.db.items(from_col):
                    if filter_func(doc):
                        # 删除源文档
                        batch.delete(key)

                        # 添加到目标队列
                        dst_key = self.db._build_key(to_col, str(uuid.uuid4()))
                        batch.put(dst_key, doc)
                        count += 1

                if count > 0:
                    self.db.write_batch(batch)
                    reversed_count += count

        return reversed_count > 0

    def smart_reverse(self, name: str, status: int = 0, **kwargs) -> bool:
        """
        智能恢复（根据状态恢复任务）

        Args:
            name: 队列名称
            status: 状态值（0: temp, 1: failure）

        Returns:
            是否成功
        """
        qtype = "temp" if status == 0 else "failure"
        return self.reverse(name, qtypes=qtype)

    def merge(self, dest: str, *queues: str, **kwargs) -> int:
        """
        合并多个队列到目标队列

        Args:
            dest: 目标队列名称
            queues: 源队列名称列表
            clear_source: 是否清空源队列，默认 True
            qtypes: 队列类型，默认 "current"

        Returns:
            合并的任务数量
        """
        clear_source = kwargs.pop("clear_source", True)
        qtypes = kwargs.pop("qtypes", "current")
        merged_count = 0

        for source_name in queues:
            for qtype in pandora.iterable(qtypes):
                with self._lock():
                    src_col = self._make_collection(source_name, qtype)
                    dst_col = self._make_collection(dest, qtype)

                    batch = WriteBatch()
                    count = 0

                    for key, doc in self.db.items(src_col):
                        # 添加到目标队列
                        dst_key = self.db._build_key(
                            dst_col, str(uuid.uuid4()))
                        batch.put(dst_key, doc)
                        count += 1

                        # 清空源队列
                        if clear_source:
                            batch.delete(key)

                    if count > 0:
                        self.db.write_batch(batch)
                        merged_count += count

        return merged_count

    def clear(self, *names, **kwargs) -> int:
        """
        清空队列

        Args:
            names: 队列名称列表
            qtypes: 队列类型，默认 ("current", "temp", "failure")

        Returns:
            清除的任务数量
        """
        qtypes = kwargs.pop("qtypes", ("current", "temp", "failure"))
        cleared_count = 0

        for name in names:
            for qtype in pandora.iterable(qtypes):
                collection = self._make_collection(name, qtype)
                cleared_count += self.db.clear(collection)

        return cleared_count


class _DoubleLock:
    """双重锁上下文管理器（线程锁 + 文件锁）"""

    def __init__(self, thread_lock: Lock, file_lock):
        self.thread_lock = thread_lock
        self.file_lock = file_lock

    def __enter__(self):
        self.thread_lock.acquire()
        self.file_lock.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            self.file_lock.release()
        finally:
            self.thread_lock.release()
