"""显式的事件重放辅助函数。

重放会重新执行 Action，因此调用方必须自行确认外部副作用的幂等性；EventLog
不会自动调用本模块。
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any, Callable, TYPE_CHECKING
import uuid

from ..errors import PersistenceError
from ..events.messages import Event
from .event_log import EventRecord

if TYPE_CHECKING:
    from ..graph.graph import Graph
    from ..runtime.machine import Machine


def replay_events(
    graph: "Graph",
    records: Iterable[EventRecord],
    *,
    machine_factory: Callable[..., "Machine"] | None = None,
    **machine_options: Any,
) -> "Machine":
    """按日志中的外部事件重新运行 Graph。

    ``start``、普通外部事件和 Fork Join 记录会被处理；内部 ``Next`` 记录只作
    审计事实而跳过，因为重放外部事件时它会由 Action 再次产生。
    """

    from ..runtime.machine import Machine

    factory = machine_factory or Machine
    machine = factory(graph, **machine_options)
    started = False
    for record in records:
        if record.graph_id is not None and record.graph_id != graph.id:
            raise PersistenceError(
                f"event graph_id {record.graph_id!r} does not match graph {graph.id!r}"
            )
        if record.kind == "start":
            if not started:
                machine.start()
                started = True
            continue
        if record.kind == "internal":
            continue
        if record.kind == "join":
            if machine.fork_group is None:
                raise PersistenceError("join record has no pending Fork")
            machine.join()
            continue
        if record.kind == "retry":
            machine.resume_retry(record.payload)
            continue
        if record.kind != "event":
            continue
        if not started:
            machine.start()
            started = True
        event = Event(
            record.name,
            payload=record.payload,
            source=record.source,
            event_id=record.event_id or uuid.uuid4().hex,
            created_at=record.created_at,
            target_run_id=record.target_run_id,
        )
        waiting_kind = (machine.context.waiting or {}).get("kind")
        if machine.status.value == "waiting" and waiting_kind != "retry":
            machine.resume(event)
        else:
            machine.dispatch(event)
    if not started:
        machine.start()
    return machine
