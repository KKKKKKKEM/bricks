"""可复用的监听器过滤器。"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable


@dataclass(frozen=True, slots=True)
class NameFilter:
    names: frozenset[str]

    def __init__(self, *names: str) -> None:
        object.__setattr__(self, "names", frozenset(names))

    def __call__(self, value: Any) -> bool:
        return getattr(value, "name", None) in self.names


@dataclass(frozen=True, slots=True)
class AnyName:
    def __call__(self, value: Any) -> bool:
        return True


@dataclass(frozen=True, slots=True)
class PredicateFilter:
    predicate: Callable[[Any], bool]

    def __call__(self, value: Any) -> bool:
        return bool(self.predicate(value))
