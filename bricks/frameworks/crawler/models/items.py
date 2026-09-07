"""解析结果的轻量记录容器与常用整理操作。"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, MutableSequence
from copy import deepcopy
from typing import Any, overload

Record = Mapping[str, Any]


def _record(value: object) -> dict[str, Any]:
    """校验记录结构并复制嵌套数据，拒绝非映射与非字符串字段名。"""
    if not isinstance(value, Mapping):
        raise TypeError("each item must be a mapping")
    if any(not isinstance(key, str) for key in value):
        raise TypeError("record keys must be strings")
    return deepcopy(dict(value))


class Items(MutableSequence[dict[str, Any]]):
    """解析结果容器，用于收集、筛选和转换字典记录。

    支持普通列表的索引、切片和修改，以及字段投影、重命名、去重和列取值。
    转换结果与原集合隔离，不承担数据校验规则、数据库保存或文件导出。
    """

    def __init__(self, records: Record | Iterable[Record] = ()) -> None:
        """构造记录集合。

        Args:
            records: 单条记录或记录迭代器，嵌套数据在加入时复制。

        Raises:
            TypeError: 记录不是映射或字段名不是字符串。
        """

        source = (records,) if isinstance(records, Mapping) else records
        self._records = [_record(record) for record in source]

    @overload
    def __getitem__(self, index: int) -> dict[str, Any]: ...

    @overload
    def __getitem__(self, index: slice) -> Items: ...

    def __getitem__(self, index: int | slice) -> dict[str, Any] | Items:
        """按 index 读取原记录，切片返回嵌套数据独立的新 Items。"""

        if isinstance(index, slice):
            return Items(self._records[index])
        return self._records[index]

    @overload
    def __setitem__(self, index: int, value: Record) -> None: ...

    @overload
    def __setitem__(self, index: slice, value: Iterable[Record]) -> None: ...

    def __setitem__(self, index: int | slice, value: Record | Iterable[Record]) -> None:
        """将 index 指定的记录或切片替换为 value 的独立副本。"""

        if isinstance(index, slice):
            if isinstance(value, Mapping):
                raise TypeError("slice assignment requires an iterable of records")
            self._records[index] = [_record(record) for record in value]
        else:
            if not isinstance(value, Mapping):
                raise TypeError("item assignment requires a mapping")
            self._records[index] = _record(value)

    def __delitem__(self, index: int | slice) -> None:
        """删除 index 指定的记录或切片。"""

        del self._records[index]

    def __len__(self) -> int:
        """返回记录数量。"""

        return len(self._records)

    def insert(self, index: int, value: Record) -> None:
        """在 index 位置插入 value 的独立副本。"""

        self._records.insert(index, _record(value))

    def extend(self, values: Iterable[Record]) -> None:
        """追加 values 中的记录副本，全部准备成功后再修改集合。"""

        prepared = [_record(record) for record in values]
        self._records.extend(prepared)

    @property
    def columns(self) -> tuple[str, ...]:
        """返回所有字段名，按首次出现顺序排列。"""

        return tuple(dict.fromkeys(key for record in self._records for key in record))

    def select(self, *columns: str) -> Items:
        """仅保留指定字段。

        Args:
            *columns: 要保留的字段，缺失字段保持缺失。

        Returns:
            嵌套数据独立的新 Items。
        """

        self._validate_columns(columns)
        return Items(
            {key: row[key] for key in columns if key in row} for row in self._records
        )

    def drop(self, *columns: str) -> Items:
        """移除指定字段。

        Args:
            *columns: 要移除的字段，缺失字段忽略。

        Returns:
            嵌套数据独立的新 Items。
        """

        self._validate_columns(columns)
        removed = set(columns)
        return Items(
            {key: value for key, value in row.items() if key not in removed}
            for row in self._records
        )

    def update_all(self, values: Record) -> None:
        """批量原地更新记录。

        Args:
            values: 要覆盖或新增的字段，每条记录取得独立的嵌套值副本。

        Raises:
            TypeError: values 不是记录映射或字段名不合法。
        """

        template = _record(values)
        prepared = [deepcopy(template) for _ in self._records]
        for record, update in zip(self._records, prepared):
            record.update(update)

    def copy(self) -> Items:
        """返回记录及嵌套数据均独立的新 Items。"""

        return Items(self._records)

    def to_list(self) -> list[dict[str, Any]]:
        """返回记录及嵌套数据均独立的普通列表。"""

        return [_record(record) for record in self._records]

    def values(self, column: str, default: Any = None) -> list[Any]:
        """提取一列，保持记录顺序。

        Args:
            column: 字段名。
            default: 缺失字段的替代值，默认 None；不替换已有的 None 值。

        Returns:
            与记录数量相同的列表，每个值都是独立副本。
        """

        self._validate_columns((column,))
        return [deepcopy(record.get(column, default)) for record in self._records]

    def filter(self, predicate: Callable[[dict[str, Any]], object]) -> Items:
        """筛选记录，不修改原集合。

        Args:
            predicate: 接收记录副本，以返回值的真值决定是否保留原记录。

        Returns:
            保持原顺序的新 Items；回调对副本的修改不进入结果。
        """

        if not callable(predicate):
            raise TypeError("predicate must be callable")
        return Items(row for row in self._records if predicate(_record(row)))

    def map(self, function: Callable[[dict[str, Any]], Record]) -> Items:
        """逐条转换记录。

        Args:
            function: 接收记录副本并返回新记录映射的函数。

        Returns:
            转换结果组成的新 Items。

        Raises:
            TypeError: function 不可调用或返回值不是合法记录。
        """

        if not callable(function):
            raise TypeError("function must be callable")
        return Items(function(_record(row)) for row in self._records)

    def rename(self, mapping: Mapping[str, str]) -> Items:
        """同时重命名字段，允许字段名互换，忽略不存在的源字段。

        Args:
            mapping: 原字段名到新字段名的映射。

        Returns:
            保持记录和字段顺序的新 Items。

        Raises:
            TypeError: mapping 或字段名类型不合法。
            ValueError: 一条记录中的多个字段被映射到相同名称。
        """

        if not isinstance(mapping, Mapping):
            raise TypeError("rename requires a mapping")
        self._validate_columns(tuple(mapping) + tuple(mapping.values()))
        renamed = []
        for row in self._records:
            names = [mapping.get(name, name) for name in row]
            if len(set(names)) != len(names):
                raise ValueError("renamed fields conflict")
            renamed.append(dict(zip(names, row.values())))
        return Items(renamed)

    def unique(self, *fields: str) -> Items:
        """按字段值去重，保留首次出现的记录及原始顺序。

        Args:
            *fields: 联合去重字段；不传时按整条记录比较。

        Returns:
            去重后的独立 Items；支持包含列表和字典的字段值。

        Raises:
            TypeError: 字段名不是字符串。
            KeyError: 任意记录缺少指定字段。
        """

        self._validate_columns(fields)
        hashed: set[Any] = set()
        unhashable: list[Any] = []
        result = []
        for row in self._records:
            key = tuple(row[name] for name in fields) if fields else row
            try:
                hash(key)
            except TypeError:
                if key in unhashable or any(key == prior for prior in hashed):
                    continue
                unhashable.append(key)
            else:
                if key in hashed or key in unhashable:
                    continue
                hashed.add(key)
            result.append(row)
        return Items(result)

    @staticmethod
    def _validate_columns(columns: tuple[str, ...]) -> None:
        if any(not isinstance(name, str) for name in columns):
            raise TypeError("column names must be strings")

    def __repr__(self) -> str:
        return f"Items({self._records!r})"

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Items):
            return self._records == other._records
        if isinstance(other, list):
            return self._records == other
        return NotImplemented
