"""幂等键和最小幂等存储协议。"""

from __future__ import annotations

from dataclasses import dataclass
import threading
from typing import Protocol


@dataclass(frozen=True, slots=True)
class IdempotencyKey:
    value: str

    def __post_init__(self) -> None:
        if not self.value:
            raise ValueError("idempotency key cannot be empty")

    def __str__(self) -> str:
        return self.value


class IdempotencyStore(Protocol):
    def claim(self, key: str) -> bool: ...

    def release(self, key: str) -> None: ...


class InMemoryIdempotencyStore:
    """进程内幂等存储，适合测试和单进程运行。"""

    def __init__(self) -> None:
        self._keys: set[str] = set()
        self._lock = threading.RLock()

    def claim(self, key: str) -> bool:
        with self._lock:
            if key in self._keys:
                return False
            self._keys.add(key)
            return True

    def release(self, key: str) -> None:
        with self._lock:
            self._keys.discard(key)
