"""HTTP headers with case-insensitive lookup and repeated field storage."""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping, MutableMapping
from types import MappingProxyType

from ._validation import entries, token

HeaderInput = Mapping[str, str] | Iterable[tuple[str, str]]


class Headers(Mapping[str, str]):
    """Read-only, case-insensitive lookup with lossless repeated field storage."""

    __slots__ = ("_pairs", "_index")

    def __init__(self, values: HeaderInput | None = None) -> None:
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
        return self._pairs

    def get_all(self, name: str) -> tuple[str, ...]:
        return tuple(value for key, value in self._pairs if key.lower() == name.lower())

    def __getitem__(self, name: str) -> str:
        return self._index[name.lower()][1]

    def __iter__(self) -> Iterator[str]:
        return (name for name, _ in self._index.values())

    def __len__(self) -> int:
        return len(self._index)

    def __eq__(self, other: object) -> bool:
        own = {name: self.get_all(name) for name in self._index}
        if isinstance(other, Headers):
            return own == {name: other.get_all(name) for name in other._index}
        if isinstance(other, Mapping):
            if any(not isinstance(name, str) for name in other):
                return False
            return own == {name.lower(): (value,) for name, value in other.items()}
        return NotImplemented


class MutableHeaders(Headers, MutableMapping[str, str]):
    """Editable request headers; assignment replaces all matching fields."""

    def __setitem__(self, name: str, value: str) -> None:
        replacement = Headers(((name, value),))
        Headers.__init__(
            self,
            (
                *((key, item) for key, item in self.raw if key.lower() != name.lower()),
                *replacement.raw,
            ),
        )

    def __delitem__(self, name: str) -> None:
        self[name]
        Headers.__init__(
            self,
            ((key, value) for key, value in self.raw if key.lower() != name.lower()),
        )

    def add(self, name: str, value: str) -> None:
        """Append a field without replacing existing values."""

        Headers.__init__(self, (*self.raw, (name, value)))
