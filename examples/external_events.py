"""演示外部消息驱动、持久化和进程重启后的继续运行。"""

from dataclasses import dataclass

from bricks import Event, GraphBuilder, Machine, Status
from bricks.engine.events import EventBus
from bricks.engine.persistence import (
    InMemoryEventLog,
    InMemorySnapshotStore,
    PersistenceBinding,
)
from bricks.engine.semantics import ReactiveRuntime


@dataclass(frozen=True)
class ExternalMessage:
    """模拟由 HTTP、队列或 RPC 适配器接收的外部消息。"""

    name: str
    payload: dict
    message_id: str


def to_event(message: ExternalMessage) -> Event:
    """把外部消息转换为引擎能够理解的 Event。"""

    return Event(
        message.name,
        payload=message.payload,
        event_id=message.message_id,
    )


builder = GraphBuilder("external-events", initial="created")
builder.action("created")
builder.wait("waiting", resume_event="approved")
builder.terminal("done")
builder.transition("created", "submitted", "waiting")
builder.transition("waiting", "approved", "done")
graph = builder.build()

store = InMemorySnapshotStore()
log = InMemoryEventLog()

# 进程一：接收提交消息，然后在等待外部审批时保存快照。
events = EventBus()
machine = Machine(graph)
PersistenceBinding(machine, store, log).attach()
binding = ReactiveRuntime(events).attach(machine)
events.publish(
    to_event(
        ExternalMessage("submitted", {"order_id": "A-100"}, "message-1")
    )
)
assert machine.status is Status.WAITING
run_id = machine.context.run_id
binding.close()

# 进程二：用同一张 Graph 和外部存储恢复，再绑定新的事件入口。
restored = PersistenceBinding.restore(graph, run_id, store, event_log=log)
restarted_events = EventBus()
ReactiveRuntime(restarted_events).attach(restored, auto_start=False)
restarted_events.publish(
    to_event(ExternalMessage("approved", {"by": "alice"}, "message-2"))
)

assert restored.status is Status.COMPLETED
assert restored.context.last_event is not None
assert restored.context.last_event.name == "approved"
print(restored.context.last_event.name)
