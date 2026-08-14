"""Runtime 角色共享的私有生命周期辅助函数。"""

from __future__ import annotations

import time
from typing import Any


def _unique(*components: object) -> tuple[object, ...]:
    unique: list[object] = []
    for component in components:
        if all(component is not item for item in unique):
            unique.append(component)
    return tuple(unique)


def _close_components(
    components: Any,
    failure: BaseException | None,
) -> BaseException | None:
    for component in components:
        try:
            component.close()
        except Exception as exc:  # noqa: BLE001
            if failure is None:
                failure = exc
    return failure


def _remaining(deadline: float | None) -> float | None:
    if deadline is None:
        return None
    return max(0.0, deadline - time.monotonic())
