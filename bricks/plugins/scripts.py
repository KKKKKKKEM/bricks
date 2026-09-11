"""用显式同步回调组合条件、分页与动作，不执行字符串脚本。"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING

from ._utils import condition, seed, sync
from bricks import signals

if TYPE_CHECKING:
    from bricks.spider.context import Context


def inject(context: Context, flows: Sequence[Callable[[Context], None]]) -> None:
    """按顺序执行显式动作，失败立即传播。

    Args:
        context: 显式领域上下文。
        flows: 同步动作序列，每个动作返回 None。

    Raises:
        TypeError: 动作返回非 None 或异步结果。
    """
    for callback in flows:
        if sync(callback, context) is not None:
            raise TypeError("plugin actions must return None")


def is_success(
    context: Context,
    *,
    match: Callable[[Context], bool],
    pre: Sequence[Callable[[Context], None]] = (),
    post: Sequence[Callable[[Context], None]] = (),
    flow: Callable[[Context, bool], None] | None = None,
) -> None:
    """运行前置动作、成功条件与后置动作，以 Pass/Retry 结束当前插槽。

    Args:
        context: 解析前的显式领域上下文。
        match: 同步布尔条件。
        pre: 判断前动作。
        post: 判断后动作。
        flow: 可选自定义结果处理，接收 context 和布尔值并返回 None。

    Raises:
        TypeError: 回调违反同步返回契约。
        signals.Pass: 条件成功，继续插槽的正常后继。
        signals.Retry: 条件失败，阶段节点校验是否允许重试。
    """
    inject(context, pre)
    accepted = condition(match, context)
    inject(context, post)
    if flow is not None:
        if sync(flow, context, accepted) is not None:
            raise TypeError("flow must return None")
    else:
        if accepted:
            raise signals.Pass()
        raise signals.Retry()


def turn_page(
    context: Context,
    *,
    match: Callable[[Context], bool],
    key: str = "page",
    step: int = 1,
    action: Callable[[Context], dict] | None = None,
    pre: Sequence[Callable[[Context], None]] = (),
    post: Sequence[Callable[[Context], None]] = (),
    success: bool = False,
) -> None:
    """条件满足时提交下一页独立种子，保留当前种子内容与交付状态。

    Args:
        context: 显式领域上下文，通常在解析或存储后使用。
        match: 是否存在下一页的同步布尔条件。
        key: 默认整数页码字段。
        step: 默认分页的非零整数步长。
        action: 可选自定义下一页种子工厂，支持游标分页。
        pre: 判断前动作。
        post: 提交或停止后动作。
        success: 是否在末尾抛出 Success 提前结束当前任务，默认 False。

    Raises:
        TypeError: 条件、回调、页码或种子违反契约。
        ValueError: 步长非法。
    """
    if type(success) is not bool or type(step) is not int or step == 0:
        raise ValueError("success must be bool and step must be a nonzero integer")
    inject(context, pre)
    if condition(match, context):
        if action is None:
            next_seed = seed(context.seeds)
            if type(next_seed.get(key)) is not int:
                raise TypeError("page field must be an integer")
            next_seed[key] += step
        else:
            next_seed = seed(sync(action, context))
        context.submit(next_seed)
    inject(context, post)
    if success:
        raise signals.Success()
