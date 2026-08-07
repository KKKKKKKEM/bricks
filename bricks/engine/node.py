"""节点的输入与输出抽象。"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterable, AsyncIterator, Iterable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from ._validation import require_non_empty_string
from .errors import InvalidOutputError
from .inputs import InputPolicy, NodeInputs
from .ports import Ports

if TYPE_CHECKING:
    from .context import ExecutionContext


@dataclass(frozen=True, slots=True)
class Output:
    """节点从命名端口产生的一项输出。"""

    value: Any = None
    port: str = "default"

    def __post_init__(self) -> None:
        """校验输出端口是非空字符串。

        异常：
            TypeError: port 不是字符串。
            ValueError: port 为空。
        """

        require_non_empty_string(self.port, "output port")


class NodeResult(AsyncIterable[Output]):
    """表示一次节点调用产生的零到多个普通或渐进式输出。"""

    __slots__ = ("_outputs",)

    def __init__(self, outputs: AsyncIterable[Output]) -> None:
        """创建节点结果。

        参数：
            outputs: 提供 Output 的异步可迭代对象。

        异常：
            TypeError: outputs 不是异步可迭代对象。
        """

        if not isinstance(outputs, AsyncIterable):
            raise TypeError("outputs must be an AsyncIterable")
        self._outputs = outputs

    def __aiter__(self) -> AsyncIterator[Output]:
        """返回一个会逐项校验类型的异步迭代器。

        返回：
            用于消费本次节点输出的异步迭代器。
        """

        return self._validated_outputs()

    async def _validated_outputs(self) -> AsyncIterator[Output]:
        """依次产出合法 Output，并拒绝其他类型。

        生成：
            底层异步可迭代对象产生的 Output。

        异常：
            InvalidOutputError: 底层对象产生的值不是 Output。
        """

        async for output in self._outputs:
            yield _require_output(output)

    @classmethod
    def empty(cls) -> NodeResult:
        """创建不产生 Output 的节点结果。

        返回：
            一个用于结束当前分支且不返回值的 NodeResult。
        """

        return cls(_iter_outputs(()))

    @classmethod
    def one(
        cls,
        value: Any = None,
        *,
        port: str = "default",
    ) -> NodeResult:
        """创建只包含一个 Output 的节点结果。

        参数：
            value: Output 携带的领域数据。
            port: Output 离开节点时使用的端口。

        返回：
            包含一个 Output 的 NodeResult。
        """

        return cls(_iter_outputs((Output(value=value, port=port),)))

    @classmethod
    def many(cls, outputs: Iterable[Output]) -> NodeResult:
        """从普通可迭代对象创建一次性节点结果。

        参数：
            outputs: 将被立即读取并保存的 Output 可迭代对象。

        返回：
            包含全部 Output 的 NodeResult。

        异常：
            TypeError: outputs 不是普通可迭代对象。
            InvalidOutputError: outputs 中包含非 Output 值。
        """

        if isinstance(outputs, (str, bytes)) or not isinstance(outputs, Iterable):
            raise TypeError("outputs must be an Iterable of Output")

        materialized = tuple(_require_output(output) for output in outputs)
        return cls(_iter_outputs(materialized))

    @classmethod
    def stream(cls, outputs: AsyncIterable[Output]) -> NodeResult:
        """从异步可迭代对象创建渐进式节点结果。

        参数：
            outputs: 按需产生 Output 的异步可迭代对象。

        返回：
            消费时才逐项获取 Output 的 NodeResult。
        """

        return cls(outputs)


def _require_output(value: object) -> Output:
    """校验并返回一个 Output。

    参数：
        value: 需要校验的节点输出值。

    返回：
        校验通过的 Output。

    异常：
        InvalidOutputError: value 不是 Output。
    """

    if not isinstance(value, Output):
        raise InvalidOutputError(value)
    return value


async def _iter_outputs(outputs: Iterable[Output]) -> AsyncIterator[Output]:
    """把普通 Output 可迭代对象适配为异步迭代器。

    参数：
        outputs: 需要异步产出的 Output 可迭代对象。

    生成：
        outputs 中的每一个 Output。
    """

    for output in outputs:
        yield output


class Node(ABC):
    """可复用的领域行为单元。"""

    input_ports = Ports(default=object)
    output_ports = Ports(default=object)
    input_policy = InputPolicy.all()

    @abstractmethod
    async def execute(
        self,
        inputs: NodeInputs,
        context: ExecutionContext,
    ) -> NodeResult:
        """执行一次节点行为并返回结果。

        参数：
            inputs: 当前 Task 根据输入策略消费的只读领域数据。
            context: 当前执行的只读身份和元数据。

        返回：
            本次调用产生的 NodeResult。
        """
