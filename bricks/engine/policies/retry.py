"""重试策略值对象。"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """定义一次 Action 最多可以进入多少次重试等待。

    ``retry_on`` 为空时，只处理 Action 主动返回的 ``Outcome.retry()``；传入异常
    类型后，节点进入行为抛出匹配异常时也会转成同一套 Retry 等待语义。
    """

    max_attempts: int = 3
    backoff: float = 0.0
    exponential: bool = False
    retry_on: tuple[type[BaseException], ...] = ()

    def __post_init__(self) -> None:
        if self.max_attempts < 0:
            raise ValueError("max_attempts 不能小于 0")
        if self.backoff < 0:
            raise ValueError("backoff 不能小于 0")
        if not isinstance(self.retry_on, tuple):
            raise TypeError("retry_on 必须是异常类型元组")
        if any(
            not isinstance(error_type, type)
            or not issubclass(error_type, BaseException)
            for error_type in self.retry_on
        ):
            raise TypeError("retry_on 只能包含 BaseException 子类")

    def allows(self, attempt: int) -> bool:
        return attempt < self.max_attempts

    def delay_for(self, attempt: int) -> float:
        return self.backoff * (2 ** max(attempt - 1, 0) if self.exponential else 1)

    def matches(self, error: BaseException) -> bool:
        """判断异常是否属于显式配置的自动重试范围。"""
        return bool(self.retry_on) and isinstance(error, self.retry_on)
