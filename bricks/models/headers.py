"""保留重复字段并支持大小写无关查询的 HTTP 头容器。"""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping, MutableMapping
from types import MappingProxyType

from ._validation import entries, token

HeaderInput = Mapping[str, str] | Iterable[tuple[str, str]]


class Headers(Mapping[str, str]):
    """只读 HTTP 头容器，查询不区分大小写且保留重复字段。

    Attributes:
        __slots__: 实例允许保存的字段名称，限制动态增加属性。
        _pairs: 保留字段顺序与同名多值的键值对快照。
        _index: 不区分大小写的请求头查询索引。
    """

    __slots__ = ("_pairs", "_index")

    def __init__(self, values: HeaderInput | None = None) -> None:
        """校验请求头并建立保留重复值的有序快照和大小写无关索引。

        Args:
            values: 请求头映射或有序键值对，None 表示空请求头。

        Raises:
            TypeError: 参数类型或接口实现不符合当前契约。
            ValueError: 参数值或字段组合不合法。
        """

        source = values.raw if isinstance(values, Headers) else values
        fields = () if source is None else entries(source, "headers")
        pairs: list[tuple[str, str]] = []
        index: dict[str, tuple[str, str]] = {}
        for name, value in fields:
            token(name, "header name")
            if not isinstance(value, str):
                raise TypeError("header values must be strings")
            if any(
                ord(char) < 32 and char != "\t" or ord(char) == 127 for char in value
            ):
                raise ValueError("header values must not contain control characters")
            pairs.append((name, value))
            index[name.lower()] = (name, value)
        self._pairs = tuple(pairs)
        self._index = MappingProxyType(index)

    @property
    def raw(self) -> tuple[tuple[str, str], ...]:
        """返回包含重复字段的有序键值对快照。

        Returns:
            包含重复字段的有序键值对元组。
        """

        return self._pairs

    def get_all(self, name: str) -> tuple[str, ...]:
        """按原始顺序返回名称匹配的全部字段值。

        Args:
            name: 不区分大小写的请求头名称。

        Returns:
            按出现顺序排列的全部同名值，不存在时为空元组。
        """

        return tuple(value for key, value in self._pairs if key.lower() == name.lower())

    def __getitem__(self, name: str) -> str:
        """按大小写无关名称读取最后一个匹配的请求头值。

        Args:
            name: 不区分大小写的请求头名称。

        Returns:
            最后一个匹配字段的字符串值。
        """

        return self._index[name.lower()][1]

    def __iter__(self) -> Iterator[str]:
        """按首次出现顺序迭代不重复的请求头名称。

        Returns:
            遍历当前对象内容的独立迭代入口。
        """

        return (name for name, _ in self._index.values())

    def __len__(self) -> int:
        """返回不区分大小写的不同字段名数量。

        Returns:
            当前容器条目数量。
        """

        return len(self._index)

    def __eq__(self, other: object) -> bool:
        """按大小写无关名称和全部同名值比较请求头内容。

        Args:
            other: 参与内容比较的另一对象。

        Returns:
            内容相等时为 True；不支持的比较类型返回 NotImplemented。
        """

        own = {name: self.get_all(name) for name in self._index}
        if isinstance(other, Headers):
            return own == {name: other.get_all(name) for name in other._index}
        if isinstance(other, Mapping):
            if any(not isinstance(name, str) for name in other):
                return False
            return own == {name.lower(): (value,) for name, value in other.items()}
        return NotImplemented


class MutableHeaders(Headers, MutableMapping[str, str]):
    """可变请求头容器，赋值替换全部同名字段。"""

    def __setitem__(self, name: str, value: str) -> None:
        """校验并替换指定索引或名称对应的值。

        Args:
            name: 不区分大小写的请求头名称。
            value: 请求头字符串值，不允许换行或非法控制字符。
        """

        replacement = Headers(((name, value),))
        Headers.__init__(
            self,
            (
                *((key, item) for key, item in self.raw if key.lower() != name.lower()),
                *replacement.raw,
            ),
        )

    def __delitem__(self, name: str) -> None:
        """移除指定索引或名称对应的值。

        Args:
            name: 不区分大小写的请求头名称。
        """

        self[name]
        Headers.__init__(
            self,
            ((key, value) for key, value in self.raw if key.lower() != name.lower()),
        )

    def add(self, name: str, value: str) -> None:
        """追加字段，不覆盖已有的同名值。

        Args:
            name: 不区分大小写的请求头名称。
            value: 请求头字符串值，不允许换行或非法控制字符。
        """

        Headers.__init__(self, (*self.raw, (name, value)))
