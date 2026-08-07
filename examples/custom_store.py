"""演示只有 save/load/delete 三个方法的外部快照存储。"""

from __future__ import annotations

import copy
from typing import Any

from bricks import GraphBuilder, Machine, Status
from bricks.engine.persistence import ContextSnapshot, PersistenceBinding, SnapshotStore


class DictSnapshotStore:
    """用字典模拟外部 KV 存储，不实现可选的 revision/CAS 能力。"""

    def __init__(self) -> None:
        self._items: dict[str, dict[str, Any]] = {}

    def save(self, snapshot: ContextSnapshot) -> None:
        # 外部存储通常会序列化数据；这里复制字典来模拟读写边界。
        self._items[snapshot.run_id] = copy.deepcopy(snapshot.to_dict())

    def load(self, run_id: str) -> ContextSnapshot | None:
        value = self._items.get(run_id)
        if value is None:
            return None
        return ContextSnapshot.from_dict(copy.deepcopy(value))

    def delete(self, run_id: str) -> None:
        self._items.pop(run_id, None)


def build_graph():
    builder = GraphBuilder("custom-store", initial="ready")
    builder.action("ready")
    builder.terminal("done")
    builder.transition("ready", "finish", "done")
    return builder.build()


graph = build_graph()
store: SnapshotStore = DictSnapshotStore()
machine = Machine(graph)
PersistenceBinding(machine, store).attach()

machine.start()
machine.dispatch("finish")

restored = PersistenceBinding.restore(graph, machine.context.run_id, store)
assert restored.status is Status.COMPLETED
print(restored.status.value)
