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
        """只根据端口和可用数量选择本次各消费一个 token 的端口。"""


@dataclass(frozen=True, slots=True)
class PolicyRef:
    name: str
    config: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        require_non_empty_string(self.name, "policy name")
        if not isinstance(self.config, Mapping):
            raise TypeError("policy config must be a mapping")
        object.__setattr__(self, "config", MappingProxyType(dict(self.config)))


@dataclass(frozen=True, slots=True)
class BoundPolicy:
    ref: PolicyRef
    selector: InputSelector

    @property
    def on_start(self) -> bool:
        return self.ref.name == "bricks.core/on-start"

    def select(
        self,
        ports: Sequence[str],
        queues: Mapping[str, Sequence[object]],
    ) -> tuple[str, ...] | None:
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
        del config
        return next(((port,) for port in ports if available[port]), None)


class _OnStartSelector:
    def select(
        self,
        ports: Sequence[str],
        available: Mapping[str, int],
        config: Mapping[str, Any],
    ) -> None:
        del ports, available, config


class PolicyRegistry:
    """注册 namespaced selector；Graph freeze 会保存实现快照。"""

    def __init__(self) -> None:
        self._selectors: dict[str, InputSelector] = {
            "bricks.core/all": _AllSelector(),
            "bricks.core/any": _AnySelector(),
            "bricks.core/on-start": _OnStartSelector(),
        }
        self._lock = RLock()

    def register(self, name: str, selector: InputSelector) -> None:
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
