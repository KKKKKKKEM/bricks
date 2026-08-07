"""Fork/Join 语义的声明式辅助对象。"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum
from typing import Any, Optional

from ..runtime.outcomes import Fork, ForkBranch


class JoinPolicy(str, Enum):
    """定义父运行等待哪些子运行成功或结束。"""

    ALL = "all"
    ANY = "any"


@dataclass(frozen=True, slots=True)
class ParallelPlan:
    """可复用的并行分支声明，最终转换为一个 Fork Outcome。"""

    branches: tuple[ForkBranch, ...]
    join_event: Optional[str] = None
    policy: JoinPolicy = JoinPolicy.ALL
    failure_policy: str = "fail"
    max_concurrency: Optional[int] = None

    def __init__(
        self,
        *branches: ForkBranch | Mapping[str, Any],
        join_event: Optional[str] = None,
        policy: JoinPolicy | str = JoinPolicy.ALL,
        failure_policy: str = "fail",
        max_concurrency: Optional[int] = None,
    ) -> None:
        normalized = tuple(
            branch if isinstance(branch, ForkBranch) else ForkBranch(**dict(branch))
            for branch in branches
        )
        normalized_policy = JoinPolicy(getattr(policy, "value", policy))
        object.__setattr__(self, "branches", normalized)
        object.__setattr__(self, "join_event", join_event)
        object.__setattr__(self, "policy", normalized_policy)
        failure_policy = getattr(failure_policy, "value", failure_policy)
        if failure_policy not in {"fail", "continue", "fail_fast"}:
            raise ValueError(
                "failure_policy must be 'fail', 'continue' or 'fail_fast'"
            )
        object.__setattr__(self, "failure_policy", failure_policy)
        if max_concurrency is not None and max_concurrency < 1:
            raise ValueError("max_concurrency must be positive or None")
        object.__setattr__(self, "max_concurrency", max_concurrency)

    def outcome(self) -> Fork:
        """生成由 Machine 执行的 Fork Outcome。"""
        return Fork(
            *self.branches,
            join_event=self.join_event,
            policy=self.policy.value,
            failure_policy=self.failure_policy,
            max_concurrency=self.max_concurrency,
        )


class Parallel:
    """创建 ParallelPlan 的便捷命名空间。"""

    @staticmethod
    def plan(
        *branches: ForkBranch | Mapping[str, Any],
        join_event: Optional[str] = None,
        policy: JoinPolicy | str = JoinPolicy.ALL,
        failure_policy: str = "fail",
        max_concurrency: Optional[int] = None,
    ) -> ParallelPlan:
        return ParallelPlan(
            *branches,
            join_event=join_event,
            policy=policy,
            failure_policy=failure_policy,
            max_concurrency=max_concurrency,
        )
