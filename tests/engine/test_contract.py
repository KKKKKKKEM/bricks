from pathlib import Path
import asyncio

import pytest

import bricks
from bricks import Event, GraphBuilder, Machine, Status
from bricks.engine.errors import MachineNotRunnable, MachineNotStarted


def test_top_level_api_contains_only_the_minimal_core_objects():
    assert bricks.__all__ == [
        "Context",
        "Event",
        "Graph",
        "GraphBuilder",
        "Machine",
        "Outcome",
        "Status",
    ]
    assert not hasattr(bricks, "State")


def test_package_publishes_pep_561_type_information():
    marker = Path(bricks.__file__).with_name("py.typed")
    assert marker.exists()


def test_machine_entrypoint_errors_follow_the_0_3_lifecycle_contract():
    builder = GraphBuilder("contract", initial="ready")
    builder.action("ready", lambda context, event: None)
    builder.terminal("done")
    builder.transition("ready", "finish", "done")
    machine = Machine(builder.build())

    with pytest.raises(MachineNotStarted):
        machine.dispatch(Event("finish"))

    machine.start()
    machine.dispatch("finish")
    assert machine.status is Status.COMPLETED

    with pytest.raises(MachineNotRunnable):
        machine.dispatch("finish")


def test_custom_action_executor_stays_at_the_action_boundary():
    calls = []

    class Executor:
        def execute(self, action, context, event):
            calls.append(("sync", event.name))
            return action(context, event)

        async def execute_async(self, action, context, event):
            calls.append(("async", event.name))
            result = action(context, event)
            if hasattr(result, "__await__"):
                return await result
            return result

    builder = GraphBuilder("executor-contract", initial="ready")
    builder.action("ready", lambda context, event: None)
    builder.terminal("done")
    builder.transition("ready", "finish", "done")
    machine = Machine(builder.build(), executor=Executor())

    machine.start()
    machine.dispatch("finish")

    assert machine.status is Status.COMPLETED
    assert calls == [("sync", "__start__")]


def test_custom_transition_selector_changes_routing_without_changing_graph():
    class LastSelector:
        def select(self, graph, source, event, context):
            return tuple(graph.transitions_from(source, event.name))[-1]

        async def select_async(self, graph, source, event, context):
            return self.select(graph, source, event, context)

    builder = GraphBuilder("selector-contract", initial="ready")
    builder.action("ready")
    builder.terminal("first")
    builder.terminal("second")
    builder.transition("ready", "finish", "first")
    builder.transition("ready", "finish", "second", priority=10)
    machine = Machine(builder.build(), selector=LastSelector())

    machine.start()
    machine.dispatch("finish")

    assert machine.node_id == "second"


def test_custom_executor_handles_node_exit_transition_and_target_actions():
    calls = []

    def enter_ready(context, event):
        calls.append(("ready.enter", event.name))

    def exit_ready(context, event):
        calls.append(("ready.exit", event.name))

    def transition_action(context, event):
        calls.append(("transition", event.name))
        return {"transitioned": True}

    def enter_done(context, event):
        calls.append(("done.enter", event.name))

    class Executor:
        def __init__(self):
            self.actions = []

        def execute(self, action, context, event):
            self.actions.append((action.__name__, event.name))
            return action(context, event)

        async def execute_async(self, action, context, event):
            self.actions.append((action.__name__, event.name))
            return action(context, event)

    builder = GraphBuilder("executor-actions", initial="ready")
    builder.action("ready", enter_ready, on_exit=exit_ready)
    builder.terminal("done", enter_done)
    builder.transition("ready", "finish", "done", action=transition_action)
    executor = Executor()
    machine = Machine(builder.build(), executor=executor)

    machine.start()
    machine.dispatch("finish")

    assert calls == [
        ("ready.enter", "__start__"),
        ("ready.exit", "finish"),
        ("transition", "finish"),
        ("done.enter", "finish"),
    ]
    assert [name for name, _ in executor.actions] == [
        "enter_ready",
        "exit_ready",
        "transition_action",
        "enter_done",
    ]
    assert machine.context.data == {"transitioned": True}


def test_custom_executor_exception_is_not_wrapped_by_machine():
    class Executor:
        def execute(self, action, context, event):
            raise LookupError("executor unavailable")

        async def execute_async(self, action, context, event):
            raise LookupError("executor unavailable")

    builder = GraphBuilder("executor-error", initial="ready")
    builder.action("ready", lambda context, event: None)
    machine = Machine(builder.build(), executor=Executor())

    with pytest.raises(LookupError, match="executor unavailable"):
        machine.start()

    assert machine.status is Status.FAILED


def test_async_custom_executor_preserves_async_action_results():
    calls = []

    async def work(context, event):
        await asyncio.sleep(0)
        calls.append(event.name)
        return {"done": True}

    class Executor:
        async def execute_async(self, action, context, event):
            calls.append("executor")
            result = action(context, event)
            if hasattr(result, "__await__"):
                return await result
            return result

        def execute(self, action, context, event):
            return action(context, event)

    builder = GraphBuilder("async-executor", initial="ready")
    builder.terminal("ready", work)
    machine = Machine(builder.build(), executor=Executor())

    async def run():
        await machine.start_async()

    asyncio.run(run())

    assert calls == ["executor", "__start__"]
    assert machine.context.data == {"done": True}
