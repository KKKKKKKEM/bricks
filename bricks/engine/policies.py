"""受控、可命名且可冻结的输入选择策略贡献点。"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from threading import RLock
from types import MappingProxyType
from typing import Any, Protocol

from .core import InputPolicy, require_non_empty_string


class InputSelector(Protocol):
    def select(
        self,
        ports: Sequence[str],
        available: Mapping[str, int],
        config: Mapping[str, Any],
    ) -> tuple[str, ...] | None:
        """只根据端口和可用数量选择本次各消费一个 token 的端口。

        Args:
            ports: 保持声明顺序的输入端口名称。
            available: 各端口当前可消费的 token 数量。
            config: 当前具名策略的参数映射。

        Returns:
            符合当前可用条件的选择结果，没有可执行输入时不触发。
        """


@dataclass(frozen=True, slots=True)
class PolicyRef:
    """带命名空间名称和只读参数的输入策略引用。

    Attributes:
        name: 当前注册项或具名策略的名称。
        config: 冻结的策略参数映射。
    """

    name: str
    config: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """校验构造字段并固定需要保持不变的数据。

        Raises:
            TypeError: 参数类型或接口实现不符合当前契约。
        """

        require_non_empty_string(self.name, "policy name")
        if not isinstance(self.config, Mapping):
            raise TypeError("policy config must be a mapping")
        object.__setattr__(self, "config", MappingProxyType(dict(self.config)))


@dataclass(frozen=True, slots=True)
class BoundPolicy:
    """Graph 冻结时绑定的策略引用与实现快照。

    Attributes:
        ref: 策略名称及参数的不可变引用。
        selector: 冻结时绑定的输入选择实现。
    """

    ref: PolicyRef
    selector: InputSelector

    @property
    def on_start(self) -> bool:
        """判断已绑定策略是否为启动时触发策略。

        Returns:
            绑定的是启动触发策略时返回 True。
        """

        return self.ref.name == "bricks.core/on-start"

    def select(
        self,
        ports: Sequence[str],
        queues: Mapping[str, Sequence[object]],
    ) -> tuple[str, ...] | None:
        """仅根据端口名称和可用 token 数量选择本次输入组合。

        Args:
            ports: 保持声明顺序的输入端口名称。
            queues: 各端口已有的输入 token 队列。

        Returns:
            符合当前可用条件的选择结果，没有可执行输入时不触发。

        Raises:
            RuntimeError: 当前生命周期状态或操作顺序不允许此操作。
        """

        available = MappingProxyType(
            {port: len(queues.get(port, ())) for port in ports}
        )
        selected = self.selector.select(ports, available, self.ref.config)
        if selected is None:
            return None
        if not isinstance(selected, tuple) or not selected:
            raise RuntimeError(
                f"policy {self.ref.name!r} must return a non-empty tuple or None"
            )
        if len(set(selected)) != len(selected):
            raise RuntimeError(
                f"policy {self.ref.name!r} selected a port more than once"
            )
        unknown = set(selected) - set(ports)
        empty = tuple(port for port in selected if available.get(port, 0) < 1)
        if unknown or empty:
            raise RuntimeError(
                f"policy {self.ref.name!r} made an invalid selection; "
                f"unknown={sorted(unknown)!r}, empty={empty!r}"
            )
        return selected


class _AllSelector:
    def select(
        self,
        ports: Sequence[str],
        available: Mapping[str, int],
        config: Mapping[str, Any],
    ) -> tuple[str, ...] | None:
        """所有声明端口都有数据时选择全部端口，否则暂不触发。

        Args:
            ports: 保持声明顺序的输入端口名称。
            available: 各端口当前可消费的 token 数量。
            config: 当前具名策略的参数映射。

        Returns:
            符合当前可用条件的选择结果，没有可执行输入时不触发。
        """

        del config
        selected = tuple(ports)
        return (
            selected if selected and all(available[port] for port in selected) else None
        )


class _AnySelector:
    def select(
        self,
        ports: Sequence[str],
        available: Mapping[str, int],
        config: Mapping[str, Any],
    ) -> tuple[str, ...] | None:
        """按声明顺序选择第一个非空端口。

        Args:
            ports: 保持声明顺序的输入端口名称。
            available: 各端口当前可消费的 token 数量。
            config: 当前具名策略的参数映射。

        Returns:
            符合当前可用条件的选择结果，没有可执行输入时不触发。
        """

        del config
        return next(((port,) for port in ports if available[port]), None)


class _OnStartSelector:
    def select(
        self,
        ports: Sequence[str],
        available: Mapping[str, int],
        config: Mapping[str, Any],
    ) -> None:
        """启动策略不选择端口 token，由执行器控制单次启动触发。

        Args:
            ports: 保持声明顺序的输入端口名称。
            available: 各端口当前可消费的 token 数量。
            config: 当前具名策略的参数映射。
        """

        del ports, available, config


class PolicyRegistry:
    """注册 namespaced selector；Graph freeze 会保存实现快照。

    Attributes:
        _selectors: 带命名空间的输入策略实现映射。
        _lock: 保护当前组件共享状态的进程内互斥锁。
    """

    def __init__(self) -> None:
        """注册三种内建输入策略并初始化策略注册锁。"""

        self._selectors: dict[str, InputSelector] = {
            "bricks.core/all": _AllSelector(),
            "bricks.core/any": _AnySelector(),
            "bricks.core/on-start": _OnStartSelector(),
        }
        self._lock = RLock()

    def register(self, name: str, selector: InputSelector) -> None:
        """注册具名输入选择器，拒绝重复名称和缺失命名空间。

        Args:
            name: 注册或查找使用的名称。
            selector: 仅依据端口和 token 数量选择输入的实现。

        Raises:
            TypeError: 参数类型或接口实现不符合当前契约。
            ValueError: 参数值或字段组合不合法。
        """

        name = require_non_empty_string(name, "policy name")
        if "/" not in name:
            raise ValueError("contributed policy names must be namespaced")
        if not callable(getattr(selector, "select", None)):
            raise TypeError("policy selector must define select()")
        with self._lock:
            if name in self._selectors:
                raise ValueError(f"input policy {name!r} is already registered")
            self._selectors[name] = selector

    def bind(self, policy: InputPolicy | PolicyRef) -> BoundPolicy:
        """将输入策略引用绑定为包含实现快照的 BoundPolicy。

        Args:
            policy: 内建输入策略或具名策略引用。

        Returns:
            本次操作得到的 BoundPolicy 实例。

        Raises:
            TypeError: 参数类型或接口实现不符合当前契约。
            ValueError: 参数值或字段组合不合法。
        """

        if isinstance(policy, InputPolicy):
            names = {
                InputPolicy.ALL: "bricks.core/all",
                InputPolicy.ANY: "bricks.core/any",
                InputPolicy.ON_START: "bricks.core/on-start",
            }
            ref = PolicyRef(names[policy])
        elif isinstance(policy, PolicyRef):
            ref = policy
        else:
            raise TypeError("input_policy must be InputPolicy or PolicyRef")
        with self._lock:
            try:
                selector = self._selectors[ref.name]
            except KeyError as exc:
                raise ValueError(
                    f"input policy {ref.name!r} is not registered"
                ) from exc
        return BoundPolicy(ref, selector)
