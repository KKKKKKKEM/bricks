"""Node 输入、输入可用情况和统一触发策略。"""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Literal

from ._validation import require_non_empty_string


class NodeInputs(Mapping[str, Any]):
    """保存一次 Node 执行实际消费的只读输入。"""

    __slots__ = ("_values",)

    def __init__(self, values: Mapping[str, Any]) -> None:
        """创建节点输入。

        参数：
            values: input port 到领域值的映射。

        异常：
            TypeError: values 不是映射，或端口名称不是字符串。
            ValueError: 端口名称为空。
        """

        if not isinstance(values, Mapping):
            raise TypeError("values must be a mapping")

        copied: dict[str, Any] = {}
        for port, value in values.items():
            require_non_empty_string(port, "input port")
            copied[port] = value
        self._values = MappingProxyType(copied)

    def __getitem__(self, port: str) -> Any:
        """读取指定 input port 的值。

        参数：
            port: 需要读取的 input port。

        返回：
            当前 Task 从该端口消费的领域值。
        """

        return self._values[port]

    def __iter__(self) -> Iterator[str]:
        """按消费顺序遍历 input port。

        返回：
            input port 名称迭代器。
        """

        return iter(self._values)

    def __len__(self) -> int:
        """返回本次执行消费的 input port 数量。

        返回：
            当前输入映射的元素数量。
        """

        return len(self._values)

    @classmethod
    def from_value(
        cls,
        value: Any,
        *,
        port: str = "default",
    ) -> NodeInputs:
        """从单个值创建节点输入。

        参数：
            value: 需要传给 Node 的领域值。
            port: 接收该值的 input port。

        返回：
            只包含一个输入的 NodeInputs。
        """

        return cls({port: value})

    def single(self) -> Any:
        """读取唯一的输入值。

        返回：
            当前 NodeInputs 中唯一的领域值。

        异常：
            ValueError: 当前输入数量不是一个。
        """

        if len(self._values) != 1:
            raise ValueError(
                f"expected one input, got {len(self._values)}"
            )
        return next(iter(self._values.values()))


@dataclass(frozen=True, slots=True)
class InputToken:
    """保存在 input port FIFO 队列中的一项输入。"""

    sequence: int
    value: Any

    def __post_init__(self) -> None:
        """校验 token 到达序号。

        异常：
            TypeError: sequence 不是整数。
            ValueError: sequence 小于零。
        """

        if type(self.sequence) is not int:
            raise TypeError("token sequence must be an integer")
        if self.sequence < 0:
            raise ValueError("token sequence must not be negative")


class InputAvailability:
    """向 InputPolicy 暴露只读的端口 token 数量和顺序。"""

    __slots__ = ("_queues",)

    def __init__(
        self,
        queues: Mapping[str, Iterable[InputToken]],
    ) -> None:
        """创建输入可用情况快照。

        参数：
            queues: input port 到 FIFO token 可迭代对象的映射。

        异常：
            TypeError: queues 不是映射，或包含非 InputToken 值。
            ValueError: input port 名称为空，或 token sequence 不是 FIFO 顺序。
        """

        if not isinstance(queues, Mapping):
            raise TypeError("queues must be a mapping")

        copied: dict[str, tuple[InputToken, ...]] = {}
        for port, tokens in queues.items():
            require_non_empty_string(port, "input port")
            values = tuple(tokens)
            if not all(isinstance(token, InputToken) for token in values):
                raise TypeError(
                    f"input port {port!r} must contain InputToken instances"
                )
            sequences = tuple(token.sequence for token in values)
            if sequences != tuple(sorted(sequences)):
                raise ValueError(
                    f"input port {port!r} tokens must use FIFO sequence order"
                )
            copied[port] = values
        self._queues = MappingProxyType(copied)

    @property
    def ports(self) -> tuple[str, ...]:
        """返回按声明顺序排列的全部 input port。"""

        return tuple(self._queues)

    def count(self, port: str) -> int:
        """返回指定端口当前可用的 token 数量。

        参数：
            port: 需要查询的 input port。

        返回：
            对应 FIFO 队列的 token 数量。
        """

        return len(self._queues[port])

    def first_sequence(self, port: str) -> int:
        """返回指定端口队首 token 的到达序号。

        参数：
            port: 需要查询的 input port。

        返回：
            队首 token 的单调递增到达序号。

        异常：
            LookupError: 指定端口当前没有 token。
        """

        queue = self._queues[port]
        if not queue:
            raise LookupError(f"input port {port!r} has no token")
        return queue[0].sequence


@dataclass(frozen=True, slots=True)
class InputGroup:
    """表示组内 AND 的一组 input port。"""

    ports: frozenset[str]

    def __post_init__(self) -> None:
        """规范化并校验组内端口。

        异常：
            TypeError: 端口名称不是字符串。
            ValueError: 输入组为空或端口名称为空。
        """

        if isinstance(self.ports, (str, bytes)):
            raise TypeError("input group ports must be a collection")
        ports = frozenset(self.ports)
        if not ports:
            raise ValueError("input group must not be empty")
        for port in ports:
            require_non_empty_string(port, "input policy port")
        object.__setattr__(self, "ports", ports)


@dataclass(frozen=True, slots=True)
class InputSelection:
    """描述一次 Node 执行需要消费的 input port。"""

    ports: tuple[str, ...]

    def __post_init__(self) -> None:
        """校验消费计划中的端口名称和唯一性。

        异常：
            TypeError: 端口名称不是字符串。
            ValueError: 端口名称为空或重复。
        """

        if isinstance(self.ports, (str, bytes)):
            raise TypeError("input selection ports must be a collection")
        ports = tuple(self.ports)
        for port in ports:
            require_non_empty_string(port, "selected input port")
        if len(set(ports)) != len(ports):
            raise ValueError("input selection ports must be unique")
        object.__setattr__(self, "ports", ports)


PolicyKind = Literal["all", "any", "groups", "on_start"]


@dataclass(frozen=True, slots=True)
class InputPolicy:
    """用组内 AND、组间 OR 统一描述 Node 的输入触发策略。"""

    _kind: PolicyKind
    _groups: tuple[InputGroup, ...] = ()

    def __post_init__(self) -> None:
        """校验策略类型和内部输入组结构。

        异常：
            TypeError: _groups 包含非 InputGroup 值。
            ValueError: 策略类型未知，或 groups 策略没有输入组。
        """

        groups = tuple(self._groups)
        if not all(isinstance(group, InputGroup) for group in groups):
            raise TypeError("input policy groups must contain InputGroup instances")
        object.__setattr__(self, "_groups", groups)

        if self._kind not in {"all", "any", "groups", "on_start"}:
            raise ValueError(f"unknown input policy kind {self._kind!r}")
        if self._kind == "groups" and not groups:
            raise ValueError("groups input policy requires input groups")
        if self._kind != "groups" and groups:
            raise ValueError(
                f"{self._kind} input policy must not define input groups"
            )

    @classmethod
    def all(cls) -> InputPolicy:
        """创建全部 input port 都就绪才触发的策略。

        返回：
            在解析时包含全部声明端口的 InputPolicy。
        """

        return cls("all")

    @classmethod
    def any(cls) -> InputPolicy:
        """创建任意一个 input port 就绪即可触发的策略。

        返回：
            在解析时把每个端口作为独立输入组的 InputPolicy。
        """

        return cls("any")

    @classmethod
    def require(cls, *ports: str) -> InputPolicy:
        """创建指定端口必须同时就绪的策略。

        参数：
            *ports: 本次执行必须一起消费的 input port。

        返回：
            只包含一个输入组的 InputPolicy。
        """

        return cls.groups(ports)

    @classmethod
    def groups(cls, *groups: Iterable[str]) -> InputPolicy:
        """创建多个备选输入组。

        参数：
            *groups: 任意一组完全就绪即可触发执行的端口集合。

        返回：
            组内 AND、组间 OR 的 InputPolicy。

        异常：
            TypeError: 某个输入组被错误地写成单个字符串。
            ValueError: 没有输入组、输入组为空或存在重复组。
        """

        normalized_groups: list[InputGroup] = []
        for group in groups:
            if isinstance(group, (str, bytes)):
                raise TypeError(
                    "each input group must be an iterable of port names"
                )
            normalized_groups.append(InputGroup(frozenset(group)))
        normalized = tuple(normalized_groups)
        if not normalized:
            raise ValueError("input policy must contain at least one group")
        if len(set(normalized)) != len(normalized):
            raise ValueError("input policy groups must be unique")
        return cls("groups", normalized)

    @classmethod
    def on_start(cls) -> InputPolicy:
        """创建只由 Flow 入口触发一次的无输入策略。

        返回：
            只适用于零 input port Node 的 InputPolicy。
        """

        return cls("on_start")

    def groups_for(self, ports: Iterable[str]) -> tuple[InputGroup, ...]:
        """根据 Node 声明解析最终输入组。

        参数：
            ports: Node 声明的全部 input port。

        返回：
            用于 readiness 判断的规范化 InputGroup。

        异常：
            ValueError: 策略与声明端口不兼容，或引用未知端口。
        """

        declared = tuple(ports)
        declared_set = set(declared)

        if self._kind == "on_start":
            if declared:
                raise ValueError("on_start policy requires zero input ports")
            return ()
        if not declared:
            raise ValueError(
                f"{self._kind} input policy requires at least one port"
            )

        if self._kind == "all":
            groups = (InputGroup(frozenset(declared)),)
        elif self._kind == "any":
            groups = tuple(
                InputGroup(frozenset({port}))
                for port in declared
            )
        else:
            groups = self._groups

        covered = set().union(*(group.ports for group in groups))
        unknown = covered - declared_set
        missing = declared_set - covered
        if unknown:
            raise ValueError(
                f"input policy references unknown ports: {sorted(unknown)!r}"
            )
        if missing:
            raise ValueError(
                f"input policy does not cover ports: {sorted(missing)!r}"
            )
        return groups

    def select(
        self,
        availability: InputAvailability,
    ) -> InputSelection | None:
        """选择当前最早完整就绪的输入组。

        参数：
            availability: 端口 token 数量和到达顺序的只读快照。

        返回：
            可以执行时返回消费计划，否则返回 None。on_start 返回空计划，
            具体运行时必须保证它只在 Flow 启动时使用一次。
        """

        if self._kind == "on_start":
            self.groups_for(availability.ports)
            return InputSelection(())

        groups = self.groups_for(availability.ports)
        ready: list[tuple[int, int, InputGroup]] = []
        for index, group in enumerate(groups):
            if all(availability.count(port) > 0 for port in group.ports):
                ready_sequence = max(
                    availability.first_sequence(port)
                    for port in group.ports
                )
                ready.append((ready_sequence, index, group))

        if not ready:
            return None

        _, _, selected = min(ready, key=lambda item: (item[0], item[1]))
        ordered_ports = tuple(
            port
            for port in availability.ports
            if port in selected.ports
        )
        return InputSelection(ordered_ports)
