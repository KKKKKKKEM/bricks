"""Context execution-local state and quiescence contracts."""

from __future__ import annotations

from bricks import Context


def test_context_state_is_shared_by_scope_and_namespace() -> None:
    local = {}
    first = Context(lambda event: None, local=local, scope="first")
    same = Context(lambda event: None, local=local, scope="first")
    other = Context(lambda event: None, local=local, scope="other")

    first.state("example/state")["value"] = 1

    assert same.state("example/state") == {"value": 1}
    assert other.state("example/state") == {}


def test_context_state_does_not_cross_execution_storage() -> None:
    first = Context(lambda event: None, local={}, scope="node")
    second = Context(lambda event: None, local={}, scope="node")

    first.state("example/state")["value"] = 1

    assert second.state("example/state") == {}


def test_context_registers_quiescence_callbacks_in_order() -> None:
    callbacks = []
    finalizers = []
    context = Context(lambda event: None, finalizers=finalizers)
    context.on_quiescence(lambda: callbacks.append("first"))
    context.on_quiescence(lambda: callbacks.append("second"))

    for finalizer in finalizers:
        finalizer()

    assert callbacks == ["first", "second"]