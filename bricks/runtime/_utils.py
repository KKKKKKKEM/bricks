"""Runtime 角色共享的私有生命周期辅助函数。"""

from __future__ import annotations

import time
from typing import Any


def _unique(*components: object) -> tuple[object, ...]:
    """按对象身份去重组件，并保留首次出现顺序。

    Args:
        *components: 需要依次关闭的组件集合。

    Returns:
        保留首次出现顺序的独立组件集合。
    """

    unique: list[object] = []
    for component in components:
        if all(component is not item for item in unique):
            unique.append(component)
    return tuple(unique)


def _close_components(
    components: Any,
    failure: BaseException | None,
) -> BaseException | None:
    """依次关闭去重后的组件，保留最先出现的失败。

    Args:
        components: 需要依次关闭的组件集合。
        failure: 先前已经记录的失败，None 表示没有失败。

    Returns:
        最先出现的异常；全部关闭成功且没有先前失败时为 None。
    """

    for component in components:
        try:
            component.close()
        except Exception as exc:  # noqa: BLE001
            if failure is None:
                failure = exc
    return failure


def _remaining(deadline: float | None) -> float | None:
    """根据单调时钟截止点计算剩余等待秒数。

    Args:
        deadline: 单调时钟上的截止时间，None 表示不限时。

    Returns:
        距离截止点的非负秒数；未设置截止点时返回 None。
    """

    if deadline is None:
        return None
    return max(0.0, deadline - time.monotonic())
