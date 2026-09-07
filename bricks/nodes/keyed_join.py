"""execution-local 的确定性 keyed join Node。"""

from __future__ import annotations

from collections import deque
from collections.abc import Hashable
from dataclasses import dataclass
from typing import Any

from ..engine import Context, InputPolicy, Node, Output, Ports
from ..engine.errors import IncompleteInputsError


@dataclass(frozen=True, slots=True)
class KeyedValue:
    """携带领域关联键的一项输入值。

    Attributes:
        key: 用于关联左右输入的领域键。
        value: 当前记录携带的数据值。
    """

    key: Hashable
    value: Any

    def __post_init__(self) -> None:
        """校验构造字段并固定需要保持不变的数据。"""

        hash(self.key)


@dataclass(frozen=True, slots=True)
class KeyedPair:
    """同一领域键下已经配对的左右输入。

    Attributes:
        key: 用于关联左右输入的领域键。
        left: 同一关联键下的左侧输入值。
        right: 同一关联键下的右侧输入值。
    """

    key: Hashable
    left: Any
    right: Any


class KeyedJoin(Node):
    """按 key 配对乱序输入；同 key 的重复值分别保持 FIFO。

    Attributes:
        input_ports: 节点声明的输入端口及其类型。
        output_ports: 节点声明的输出端口及其类型。
        input_policy: 仅依据端口和 token 数量生效的输入策略。
        max_pending: 允许暂存的未匹配关联输入数量上限。
    """

    input_ports = Ports(left=KeyedValue, right=KeyedValue)
    output_ports = Ports(joined=KeyedPair)
    input_policy = InputPolicy.ANY

    def __init__(self, *, max_pending: int = 10_000) -> None:
        """校验未配对输入上限，关联状态由执行上下文保存。

        Args:
            max_pending: keyed join 允许保留的未配对输入数量上限。

        Raises:
            ValueError: 参数值或字段组合不合法。
        """

        if type(max_pending) is not int or max_pending < 1:
            raise ValueError("max_pending must be an integer greater than zero")
        self.max_pending = max_pending

    def execute(self, inputs, context: Context) -> Output | None:
        """在当前执行局部状态中暂存或配对同键输入并输出配对结果。

        Args:
            inputs: 入口数据或按端口名称组织的输入映射。
            context: 当前调用的执行或插件上下文。

        Returns:
            符合声明端口契约的 Output。
        """

        state = context.state("bricks.keyed-join")
        buffers = state.setdefault("buffers", {"left": {}, "right": {}})
        pending = state.setdefault("pending", 0)
        if not state.get("finalizer_registered"):
            state["finalizer_registered"] = True

            def validate_complete() -> None:
                """在静止阶段检查是否仍存在未配对的 keyed join 数据。

                Raises:
                    IncompleteInputsError: 数据流静止时仍有无法组合的输入。
                """

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
            left, right = (
                (item.value, counterpart)
                if port == "left"
                else (
                    counterpart,
                    item.value,
                )
            )
            return Output(KeyedPair(item.key, left, right), "joined")

        if pending >= self.max_pending:
            raise OverflowError(f"keyed join exceeded max_pending={self.max_pending}")
        buffers[port].setdefault(item.key, deque()).append(item.value)
        state["pending"] = pending + 1
        return None
