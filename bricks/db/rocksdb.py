# -*- coding: utf-8 -*-
# @Time    : 2025-12-19
# @Author  : Kem
# @Desc    : RocksDB 数据库工具类 - 纯粹的 CRUD 封装
import json
import uuid
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Optional, Union

from bricks.utils import pandora

pandora.require("rocksdict==0.3.29")
from rocksdict import AccessType, Rdict, WriteBatch  # noqa: E402


class RocksDB:
    """
    RocksDB 数据库连接工具类

    - 提供 MongoDB 风格的 CRUD 接口
    - 支持集合（collection）概念
    - 支持单主键和组合主键
    - 支持批量操作和事务
    - RocksDB 本身是线程安全的，不需要额外加锁

    """

    def __init__(
        self,
        path: str = "rocks_db_data",
        access_type: AccessType = AccessType.read_write(),
        **options
    ):
        """
        初始化 RocksDB 连接

        Args:
            path: 数据库文件路径
            access_type: 访问类型（读写/只读等）
            **options: Rdict 的其他选项
        """
        self.path = Path(path)
        self.db = Rdict(str(self.path), access_type=access_type, **options)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        """关闭数据库连接"""
        if hasattr(self.db, 'close'):
            self.db.close()

    # ==================== 键管理 ====================

    @staticmethod
    def _serialize_key(key: Any) -> str:
        """将任意类型的键序列化为字符串"""
        if isinstance(key, (str, int, float)):
            return str(key)
        return json.dumps(key, ensure_ascii=False, sort_keys=True)

    def _build_key(self, collection: str, doc_id: Any) -> str:
        """构建完整的存储键：collection:id"""
        return f"{collection}:{self._serialize_key(doc_id)}"

    def _parse_key(self, key: str) -> tuple:
        """解析存储键，返回 (collection, doc_id)"""
        if ':' not in key:
            return key, None
        collection, doc_id = key.split(':', 1)
        try:
            return collection, json.loads(doc_id)
        except (json.JSONDecodeError, ValueError):
            return collection, doc_id

    @staticmethod
    def _extract_id(doc: Dict[str, Any], row_keys: Optional[Union[str, List[str]]]) -> Any:
        """
        从文档提取 ID

        Args:
            doc: 文档字典
            row_keys: ID 字段名（单个或列表），None 则自动生成 UUID

        Returns:
            单个值或元组（组合主键）
        """
        if row_keys is None:
            return str(uuid.uuid4())

        fields = [row_keys] if isinstance(row_keys, str) else row_keys

        # 检查字段是否存在
        missing = [f for f in fields if f not in doc]
        if missing:
            raise ValueError(f"Missing ID fields: {missing}")

        values = [doc[f] for f in fields]
        return values[0] if len(values) == 1 else tuple(values)

    # ==================== 基础 CRUD ====================

    def insert(self, collection: str, *docs: Dict[str, Any], row_keys: Optional[Union[str, List[str]]] = None) -> List[Any]:
        """
        插入单个文档

        Args:
            collection: 集合名
            *docs: 文档内容（支持多个）
            row_keys: ID 字段名，None 则自动生成 UUID

        Returns:
            文档 ID 列表
        """
        if not docs:
            return []

        batch = WriteBatch()
        doc_ids = []

        for doc in docs:
            doc_id = self._extract_id(doc, row_keys)
            key = self._build_key(collection, doc_id)
            batch.put(key, doc)
            doc_ids.append(doc_id)

        self.db.write(batch)
        return doc_ids

    def find_one(self, collection: str, doc_id: Any) -> Optional[Dict[str, Any]]:
        """
        根据 ID 查找单个文档

        Returns:
            文档内容，不存在返回 None
        """
        key = self._build_key(collection, doc_id)
        return self.db.get(key)

    def find(
        self,
        collection: str,
        filter_func: Optional[Callable[[Dict[str, Any]], bool]] = None,
        limit: int = 0,
        skip: int = 0
    ) -> List[Dict[str, Any]]:
        """
        查找多个文档

        Args:
            collection: 集合名
            filter_func: 过滤函数
            limit: 返回数量限制，0 表示不限制
            skip: 跳过的数量

        Returns:
            文档列表
        """
        prefix = f"{collection}:"
        results = []
        skipped = 0

        for key, value in self.db.items():
            if not str(key).startswith(prefix):
                continue

            if filter_func and not filter_func(value):
                continue

            if skipped < skip:
                skipped += 1
                continue

            results.append(value)

            if limit > 0 and len(results) >= limit:
                break

        return results

    def find_by_id(self, collection: str, doc_id: Any) -> tuple:
        """
        根据 ID 查找文档，同时返回键名

        Returns:
            (key, doc) 或 (None, None)
        """
        key = self._build_key(collection, doc_id)
        doc = self.db.get(key)
        return (key, doc) if doc else (None, None)

    def update(self, collection: str, doc_id: Any, update_data: Dict[str, Any]) -> bool:
        """
        更新文档（合并更新）

        Returns:
            是否更新成功
        """
        key = self._build_key(collection, doc_id)
        doc = self.db.get(key)

        if doc is None:
            return False

        doc.update(update_data)
        self.db[key] = doc
        return True

    def update_many(
        self,
        collection: str,
        update_data: Dict[str, Any],
        filter_func: Optional[Callable[[Dict[str, Any]], bool]] = None
    ) -> int:
        """
        批量更新文档

        Returns:
            更新的文档数量
        """
        prefix = f"{collection}:"
        batch = WriteBatch()
        count = 0

        for key, value in self.db.items():
            if not str(key).startswith(prefix):
                continue

            if filter_func and not filter_func(value):
                continue

            value.update(update_data)
            batch.put(key, value)
            count += 1

        if count > 0:
            self.db.write(batch)

        return count

    def delete(self, collection: str, doc_id: Any) -> bool:
        """
        删除单个文档

        Returns:
            是否删除成功
        """
        key = self._build_key(collection, doc_id)

        if key not in self.db:
            return False

        del self.db[key]
        return True

    def delete_many(
        self,
        collection: str,
        filter_func: Optional[Callable[[Dict[str, Any]], bool]] = None
    ) -> int:
        """
        批量删除文档

        Returns:
            删除的文档数量
        """
        prefix = f"{collection}:"
        batch = WriteBatch()
        count = 0

        for key, value in self.db.items():
            if not str(key).startswith(prefix):
                continue

            if filter_func and not filter_func(value):
                continue

            batch.delete(key)
            count += 1

        if count > 0:
            self.db.write(batch)

        return count

    def upsert(self, collection: str, doc: Dict[str, Any], row_keys: Optional[Union[str, List[str]]] = None) -> Any:
        """
        插入或更新文档

        Args:
            collection: 集合名
            doc: 文档内容
            row_keys: ID 字段名，None 则自动生成 UUID

        Returns:
            文档 ID
        """
        doc_id = self._extract_id(doc, row_keys)
        key = self._build_key(collection, doc_id)
        self.db[key] = doc
        return doc_id

    def upsert_many(self, collection: str, docs: List[Dict[str, Any]], row_keys: Optional[Union[str, List[str]]] = None) -> List[Any]:
        """
        批量插入或更新文档

        Args:
            collection: 集合名
            docs: 文档列表
            row_keys: ID 字段名，None 则自动生成 UUID

        Returns:
            文档 ID 列表
        """
        if not docs:
            return []

        batch = WriteBatch()
        doc_ids = []

        for doc in docs:
            doc_id = self._extract_id(doc, row_keys)
            key = self._build_key(collection, doc_id)
            batch.put(key, doc)
            doc_ids.append(doc_id)

        self.write_batch(batch)
        return doc_ids

    # ==================== 工具方法 ====================

    def count(self, collection: str, filter_func: Optional[Callable[[Dict[str, Any]], bool]] = None) -> int:
        """统计文档数量"""
        prefix = f"{collection}:"
        count = 0

        for key, value in self.db.items():
            if not str(key).startswith(prefix):
                continue

            if filter_func and not filter_func(value):
                continue

            count += 1

        return count

    def exists(self, collection: str, doc_id: Any) -> bool:
        """检查文档是否存在"""
        key = self._build_key(collection, doc_id)
        return key in self.db

    def clear(self, collection: str) -> int:
        """清空集合中的所有文档"""
        return self.delete_many(collection)

    def keys(self, collection: str, limit: int = 0) -> List[str]:
        """获取集合中的所有键"""
        prefix = f"{collection}:"
        keys = []

        for key in self.db.keys():
            if not str(key).startswith(prefix):
                continue

            keys.append(key)

            if limit > 0 and len(keys) >= limit:
                break

        return keys

    def items(self, collection: str, limit: int = 0) -> Iterator[tuple]:
        """迭代集合中的所有 (key, doc)"""
        prefix = f"{collection}:"
        count = 0

        for key, value in self.db.items():
            if not str(key).startswith(prefix):
                continue

            yield (key, value)
            count += 1

            if limit > 0 and count >= limit:
                break

    # ==================== 事务支持 ====================

    def create_batch(self) -> WriteBatch:
        """创建批量操作对象"""
        return WriteBatch()

    def write_batch(self, batch: WriteBatch):
        """执行批量操作"""
        self.db.write(batch)

    # ==================== 便捷方法 ====================

    def write(self, collection: str, *docs, row_keys: Optional[Union[str, List[str]]] = None) -> List[Any]:
        """
        通用写入方法（自动判断 insert 或 upsert）

        - row_keys 为 None：使用 insert（自动生成 UUID）
        - row_keys 指定：使用 upsert
        """
        if not docs:
            return []

        if row_keys is None:
            # 插入模式
            return [self.insert(collection, doc) for doc in docs] if len(docs) < 10 else self.insert(collection, *docs)
        else:
            # Upsert 模式
            return [self.upsert(collection, doc, row_keys) for doc in docs] if len(docs) < 10 else self.upsert_many(collection, list(docs), row_keys)

    def __repr__(self):
        return f"RocksDB(path='{self.path}')"
