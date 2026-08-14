"""execution-local 的确定性 keyed join Node。"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import Any, Hashable

from ..engine import Context, InputPolicy, Node, Output, Ports
from ..engine.errors import IncompleteInputsError


@dataclass(frozen=True, slots=True)
class KeyedValue:
    key: Hashable
    value: Any

    def __post_init__(self) -> None:
        hash(self.key)


@dataclass(frozen=True, slots=True)
class KeyedPair:
    key: Hashable
    left: Any
    right: Any


class KeyedJoin(Node):
    """按 key 配对乱序输入；同 key 的重复值分别保持 FIFO。"""

    input_ports = Ports(left=KeyedValue, right=KeyedValue)
    output_ports = Ports(joined=KeyedPair)
    input_policy = InputPolicy.ANY

    def __init__(self, *, max_pending: int = 10_000) -> None:
        if type(max_pending) is not int or max_pending < 1:
            raise ValueError("max_pending must be an integer greater than zero")
        self.max_pending = max_pending

    def execute(self, inputs, context: Context) -> Output | None:
        state = context.state("bricks.keyed-join")
        buffers = state.setdefault("buffers", {"left": {}, "right": {}})
        pending = state.setdefault("pending", 0)
        if not state.get("finalizer_registered"):
            state["finalizer_registered"] = True

            def validate_complete() -> None:
                if state["pending"]:
                    keys = {
                        side: tuple(side_buffers)
                        for side, side_buffers in buffers.items()
                        if side_buffers
                    }
                    raise IncompleteInputsError(
                        f"keyed join stopped with {state['pending']} unmatched values: "
                        f"{keys!r}"
                    )

            context.on_quiescence(validate_complete)
        port, item = next(iter(inputs.items()))
        other = "right" if port == "left" else "left"
        other_queue = buffers[other].get(item.key)
        if other_queue:
            counterpart = other_queue.popleft()
            state["pending"] = pending - 1
            if not other_queue:
                del buffers[other][item.key]
            left, right = (item.value, counterpart) if port == "left" else (
                counterpart,
                item.value,
            )
            return Output(KeyedPair(item.key, left, right), "joined")

        if pending >= self.max_pending:
            raise OverflowError(
                f"keyed join exceeded max_pending={self.max_pending}"
            )
        buffers[port].setdefault(item.key, deque()).append(item.value)
        state["pending"] = pending + 1
        return None
