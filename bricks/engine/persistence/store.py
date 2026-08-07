"""快照存储协议和内存参考实现。"""

from __future__ import annotations

import copy
import threading
from dataclasses import replace
from typing import Protocol

from ..errors import SnapshotConflictError
from .snapshot import ContextSnapshot


class SnapshotStore(Protocol):
    """保存当前运行快照的最小协议。

    ``save_if_current()`` 不是必需能力。需要并发控制的实现可以按结构约定额外提供
    同名方法，``PersistenceBinding`` 会自动使用它。

    ``read_history()`` 同样是可选能力；实现它后可以通过
    ``PersistenceBinding.history()`` 查询提交快照历史。
    """

    def save(self, snapshot: ContextSnapshot) -> None: ...

    def load(self, run_id: str) -> ContextSnapshot | None: ...

    def delete(self, run_id: str) -> None: ...


class AsyncSnapshotStore(Protocol):
    """异步存储的最小协议，供事件循环内的持久化绑定使用。"""

    async def save(self, snapshot: ContextSnapshot) -> None: ...

    async def load(self, run_id: str) -> ContextSnapshot | None: ...

    async def delete(self, run_id: str) -> None: ...


class InMemorySnapshotStore:
    def __init__(self) -> None:
        self._items: dict[str, ContextSnapshot] = {}
        self._history: dict[str, list[ContextSnapshot]] = {}
        self._lock = threading.RLock()

    def save(self, snapshot: ContextSnapshot) -> None:
        with self._lock:
            saved = _copy_snapshot(snapshot)
            self._items[snapshot.run_id] = saved
            self._history.setdefault(snapshot.run_id, []).append(saved)

    def save_if_current(self, snapshot: ContextSnapshot) -> ContextSnapshot:
        """以 revision 做一次进程内 compare-and-swap。"""
        with self._lock:
            current = self._items.get(snapshot.run_id)
            if current is not None and current.revision != snapshot.revision:
                raise SnapshotConflictError(
                    f"snapshot revision conflict for run {snapshot.run_id!r}: "
                    f"expected {snapshot.revision}, current {current.revision}"
                )
            saved = _copy_snapshot(
                replace(snapshot, revision=snapshot.revision + 1)
            )
            self._items[snapshot.run_id] = saved
            self._history.setdefault(snapshot.run_id, []).append(saved)
            return saved

    def load(self, run_id: str) -> ContextSnapshot | None:
        with self._lock:
            snapshot = self._items.get(run_id)
            return None if snapshot is None else _copy_snapshot(snapshot)

    def read_history(self, run_id: str) -> list[ContextSnapshot]:
        """按提交顺序返回快照历史；这是最小协议之外的可选能力。"""
        with self._lock:
            return [_copy_snapshot(item) for item in self._history.get(run_id, ())]

    def delete(self, run_id: str) -> None:
        with self._lock:
            self._items.pop(run_id, None)
            self._history.pop(run_id, None)


def _copy_snapshot(snapshot: ContextSnapshot) -> ContextSnapshot:
    """模拟存储序列化边界，避免调用方修改内部快照历史。"""
    return ContextSnapshot.from_dict(copy.deepcopy(snapshot.to_dict()))
