"""演示不持有 Machine 的外部驱动如何按 run_id 恢复并投递消息。"""

from dataclasses import dataclass
from typing import Any, Mapping

from bricks import Context, Event, Graph, GraphBuilder, Machine, Status
from bricks.engine.persistence import (
    EventLog,
    InMemoryEventLog,
    InMemorySnapshotStore,
    PersistenceBinding,
    SnapshotStore,
)
from bricks.engine.policies import IdempotencyStore, InMemoryIdempotencyStore


@dataclass(frozen=True)
class ExternalMessage:
    name: str
    payload: Any
    message_id: str

    def to_event(self) -> Event:
        """把传输层消息转换为引擎 Event。"""

        return Event(self.name, self.payload, event_id=self.message_id)


class ProcessDriver:
    """一个只负责加载、推进和保存运行实例的最小外部驱动。"""

    def __init__(
        self,
        graph: Graph,
        store: SnapshotStore,
        event_log: EventLog,
        idempotency: IdempotencyStore,
    ) -> None:
        self.graph = graph
        self.store = store
        self.event_log = event_log
        self.idempotency = idempotency

    def start(self, data: Mapping[str, Any] | None = None) -> str:
        machine = Machine(
            self.graph,
            context=Context(graph_id=self.graph.id, data=dict(data or {})),
            idempotency=self.idempotency,
        )
        PersistenceBinding(machine, self.store, self.event_log).attach()
        machine.start()
        return machine.context.run_id

    def deliver(self, run_id: str, message: ExternalMessage) -> Context:
        """每次投递都从快照恢复，处理后由 Binding 保存最新快照。"""

        machine = PersistenceBinding.restore(
            self.graph,
            run_id,
            self.store,
            event_log=self.event_log,
            idempotency=self.idempotency,
        )
        machine.route(run_id, message.to_event())
        return machine.context


def build_graph() -> Graph:
    builder = GraphBuilder("external-driver", initial="created")
    builder.action("created")
    builder.wait("waiting", resume_event="approved")
    builder.terminal("done")
    builder.transition("created", "submitted", "waiting")
    builder.transition("waiting", "approved", "done")
    return builder.build()


graph = build_graph()
store = InMemorySnapshotStore()
event_log = InMemoryEventLog()
# 真实部署中这里应替换成跨进程可共享的幂等存储。
idempotency = InMemoryIdempotencyStore()

first_process = ProcessDriver(graph, store, event_log, idempotency)
run_id = first_process.start()
waiting = first_process.deliver(
    run_id,
    ExternalMessage("submitted", {"order_id": "A-200"}, "message-1"),
)
assert waiting.status is Status.WAITING

# 模拟第一个进程退出，第二个进程只保留协议对象和 Graph 定义。
second_process = ProcessDriver(graph, store, event_log, idempotency)
completed = second_process.deliver(
    run_id,
    ExternalMessage("approved", {"by": "alice"}, "message-2"),
)
assert completed.status is Status.COMPLETED
print(completed.status.value)
