"""Context 执行局部状态与静止阶段回调契约测试。"""

from __future__ import annotations

from collections.abc import Callable, MutableMapping
from typing import Any

from bricks import Context


def test_context_state_is_shared_by_scope_and_namespace() -> None:
    """验证相同作用域和命名空间共享执行局部状态。"""

    local: dict[tuple[str, str], MutableMapping[str, Any]] = {}
    first = Context(lambda event: None, local=local, scope="first")
    same = Context(lambda event: None, local=local, scope="first")
    other = Context(lambda event: None, local=local, scope="other")

    first.state("example/state")["value"] = 1

    assert same.state("example/state") == {"value": 1}
    assert other.state("example/state") == {}


def test_context_state_does_not_cross_execution_storage() -> None:
    """验证局部状态不会跨越独立执行存储。"""

    first = Context(lambda event: None, local={}, scope="node")
    second = Context(lambda event: None, local={}, scope="node")

    first.state("example/state")["value"] = 1

    assert second.state("example/state") == {}


def test_context_scope_and_namespace_cannot_collide_on_separators() -> None:
    """验证作用域名称中的分隔符不会造成状态键碰撞。"""

    local: dict[tuple[str, str], MutableMapping[str, Any]] = {}
    first = Context(lambda event: None, local=local, scope="a:b")
    other = Context(lambda event: None, local=local, scope="a")
    unscoped = Context(lambda event: None, local=local)

    first.state("c")["value"] = 1

    assert other.state("b:c") == {}
    assert unscoped.state("a:b:c") == {}


def test_context_registers_quiescence_callbacks_in_order() -> None:
    """验证静止阶段回调按注册顺序保存。"""

    callbacks = []
    finalizers: list[Callable[[], None]] = []
    context = Context(lambda event: None, finalizers=finalizers)
    context.on_quiescence(lambda: callbacks.append("first"))
    context.on_quiescence(lambda: callbacks.append("second"))

    for finalizer in finalizers:
        finalizer()

    assert callbacks == ["first", "second"]
