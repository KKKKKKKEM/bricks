"""Action 结果和运行时指令。

结果描述运行时效果，但不会直接修改图；状态变化仍然通过具名迁移和 Guard 完成。
"""

from __future__ import annotations

from collections.abc import Mapping as MappingABC
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Mapping, Optional


class Outcome:
    """Action 结果的标记基类。"""

    @classmethod
    def next(
        cls,
        event: str,
        payload: Any = None,
        *,
        update: Optional[Mapping[str, Any]] = None,
    ) -> "Next":
        """更新 Context 后产生一个仍需经过 Graph 的内部事件。"""
        return Next(event, payload, {} if update is None else update)

    @classmethod
    def emit(cls, event: str, payload: Any = None) -> "Emit":
        return Emit(event, payload)

    @classmethod
    def wait(
        cls, delay: Optional[float] = None, *, resume_event: Optional[str] = None
    ) -> "Wait":
        return Wait(delay, resume_event)

    @classmethod
    def interrupt(cls, *, resume_event: Optional[str] = None) -> "Wait":
        """声明一次需要外部输入恢复的中断；底层仍使用统一的 Wait 语义。"""
        return Wait(None, resume_event)

    @classmethod
    def retry(
        cls,
        event: Optional[str] = None,
        delay: Optional[float] = None,
        reason: Any = None,
    ) -> "Retry":
        return Retry(event, delay, reason)

    @classmethod
    def fork(
        cls,
        *branches: "ForkBranch | Mapping[str, Any]",
        join_event: Optional[str] = None,
        policy: str = "all",
        failure_policy: str = "fail",
        max_concurrency: Optional[int] = None,
    ) -> "Fork":
        return Fork(
            *branches,
            join_event=join_event,
            policy=policy,
            failure_policy=failure_policy,
            max_concurrency=max_concurrency,
        )

    @classmethod
    def subgraph(
        cls,
        graph: Any,
        *,
        entry_event: Optional[str] = None,
        return_event: Optional[str] = None,
        data: Optional[Mapping[str, Any]] = None,
    ) -> "Fork":
        """启动一张独立 Graph；运行时使用单分支 Fork/Join 语义。"""
        return Fork(
            ForkBranch(
                event=entry_event,
                data=data or {},
                graph=graph,
            ),
            join_event=return_event,
            kind="subgraph",
        )

    @classmethod
    def stop(cls, reason: Any = None) -> "Stop":
        return Stop(reason)

    @classmethod
    def fail(cls, error: Any) -> "Fail":
        return Fail(error)

    @classmethod
    def update(cls, values: Mapping[str, Any] | None = None, **kwargs: Any) -> "Update":
        data = dict(values or {})
        data.update(kwargs)
        return Update(data)


@dataclass(frozen=True, slots=True)
class Next(Outcome):
    event: str
    payload: Any = None
    updates: Mapping[str, Any] = field(default_factory=dict)

    def __init__(
        self,
        event: str,
        payload: Any = None,
        update: Mapping[str, Any] | None = None,
    ) -> None:
        object.__setattr__(self, "event", event)
        object.__setattr__(self, "payload", payload)
        object.__setattr__(self, "updates", {} if update is None else update)
        self.__post_init__()

    def __post_init__(self) -> None:
        if not isinstance(self.event, str) or not self.event:
            raise ValueError("Next.event 不能为空")
        if not isinstance(self.updates, MappingABC):
            raise TypeError("Next.update 必须是映射")
        object.__setattr__(self, "updates", MappingProxyType(dict(self.updates)))


@dataclass(frozen=True, slots=True)
class Emit(Outcome):
    event: str
    payload: Any = None


@dataclass(frozen=True, slots=True)
class Wait(Outcome):
    delay: Optional[float] = None
    resume_event: Optional[str] = None

    def __post_init__(self) -> None:
        if self.delay is not None and self.delay < 0:
            raise ValueError("Wait.delay cannot be negative")


@dataclass(frozen=True, slots=True)
class Retry(Outcome):
    event: Optional[str] = None
    delay: Optional[float] = None
    reason: Any = None

    def __post_init__(self) -> None:
        if self.delay is not None and self.delay < 0:
            raise ValueError("Retry.delay cannot be negative")


@dataclass(frozen=True, slots=True)
class ForkBranch:
    """描述一个子运行要接收的初始事件。"""

    event: Optional[str] = None
    payload: Any = None
    data: Mapping[str, Any] = field(default_factory=dict)
    graph: Any = None

    def __post_init__(self) -> None:
        if self.event is not None and (
            not isinstance(self.event, str) or not self.event
        ):
            raise ValueError("ForkBranch.event 不能为空")


@dataclass(frozen=True, slots=True)
class Fork(Outcome):
    branches: tuple[ForkBranch, ...] = field(default_factory=tuple)
    join_event: Optional[str] = None
    policy: str = "all"
    failure_policy: str = "fail"
    max_concurrency: Optional[int] = None
    kind: str = "fork"

    def __init__(
        self,
        *branches: ForkBranch | Mapping[str, Any],
        join_event: Optional[str] = None,
        policy: str = "all",
        failure_policy: str = "fail",
        max_concurrency: Optional[int] = None,
        kind: str = "fork",
    ) -> None:
        normalized = []
        for branch in branches:
            if isinstance(branch, ForkBranch):
                normalized.append(branch)
            else:
                normalized.append(ForkBranch(**dict(branch)))
        policy = getattr(policy, "value", policy)
        if policy not in {"all", "any"}:
            raise ValueError("Fork policy must be 'all' or 'any'")
        failure_policy = getattr(failure_policy, "value", failure_policy)
        if failure_policy not in {"fail", "continue", "fail_fast"}:
            raise ValueError(
                "Fork failure_policy must be 'fail', 'continue' or 'fail_fast'"
            )
        if max_concurrency is not None and max_concurrency < 1:
            raise ValueError("max_concurrency must be positive or None")
        object.__setattr__(self, "branches", tuple(normalized))
        object.__setattr__(self, "join_event", join_event)
        object.__setattr__(self, "policy", policy)
        object.__setattr__(self, "failure_policy", failure_policy)
        object.__setattr__(self, "max_concurrency", max_concurrency)
        if kind not in {"fork", "subgraph"}:
            raise ValueError("Fork kind must be 'fork' or 'subgraph'")
        object.__setattr__(self, "kind", kind)


@dataclass(frozen=True, slots=True)
class Stop(Outcome):
    reason: Any = None


@dataclass(frozen=True, slots=True)
class Fail(Outcome):
    error: Any


@dataclass(frozen=True, slots=True)
class Update(Outcome):
    """将 Action 返回的增量数据合并到当前 Context。"""

    data: Mapping[str, Any]

    def __post_init__(self) -> None:
        if not isinstance(self.data, MappingABC):
            raise TypeError("Update.data 必须是映射")
        object.__setattr__(self, "data", MappingProxyType(dict(self.data)))
