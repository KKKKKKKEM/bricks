"""Executor, storage and notification implementations share public contracts."""

import asyncio
import pickle
import sqlite3
from threading import Event as ThreadEvent

import pytest

from bricks import Context, Execution, ExecutionLimits, Graph, Node, Output, Runtime
from bricks.engine.errors import ExecutionCancelledError, ExecutionTimeoutError
from bricks.engine.execution_resources import LocalExecutionNotifier
from bricks.plugins import CAP_EXECUTION_FACTORY, PluginDescriptor
from bricks.runtime import LocalRuntimePlugin


class Produce(Node):
    def execute(self, inputs, context):
        return tuple(Output(index) for index in range(inputs["default"]))


def graph():
    return Graph(entrypoint="produce").add(produce=Produce())


class SyncExecutor:
    def __init__(self, *, fail=False, gate=None):
        self.fail = fail
        self.gate = gate
        self.closed = False

    def execute(self, name, graph, inputs, emit, plan=None, *, slot=None, execution):
        assert graph.frozen and len(graph.nodes) == 1
        assert graph.spec_for(graph.entrypoint).input_ports["default"] is object
        assert graph.outgoing_for(graph.entrypoint, "default") == ()
        if plan is not None:
            assert plan.graph is graph
        with execution.step(graph.entrypoint):
            outputs = graph.nodes[graph.entrypoint].execute(
                {"default": inputs},
                Context(emit, slot, checkpoint=execution.checkpoint),
            )
        for output in outputs:
            execution.publish_output(output)
            if self.gate is not None:
                while not self.gate.wait(0.005):
                    execution.checkpoint()
            if self.fail:
                raise ValueError("executor failed after output")

    def close(self):
        self.closed = True


class AsyncExecutor(SyncExecutor):
    async def execute(
        self, name, graph, inputs, emit, plan=None, *, slot=None, execution
    ):
        assert graph.frozen
        with execution.step(graph.entrypoint):
            outputs = graph.nodes[graph.entrypoint].execute(
                {"default": inputs},
                Context(emit, slot, checkpoint=execution.checkpoint),
            )
        for output in outputs:
            await execution.apublish_output(output)
            if self.gate is not None:
                while not self.gate.is_set():
                    await asyncio.sleep(0.005)
            if self.fail:
                raise ValueError("executor failed after output")


@pytest.mark.parametrize("executor_type", [SyncExecutor, AsyncExecutor])
@pytest.mark.parametrize("fail", [False, True])
def test_alternate_executor_publishes_before_completion_and_preserves_failure(
    executor_type, fail
):
    gate = ThreadEvent()
    executor = executor_type(fail=fail, gate=gate)
    with Runtime(plugins=(LocalRuntimePlugin(executor=executor),)) as runtime:
        runtime.register("work", graph())
        execution = runtime.start("work", 3, output_buffer=1)
        stream = iter(execution)
        try:
            assert next(stream) == Output(0)
            assert not execution.done
        finally:
            gate.set()
        if fail:
            with pytest.raises(ValueError, match="after output"):
                tuple(stream)
            with pytest.raises(ValueError, match="after output"):
                execution.result()
        else:
            assert tuple(stream) == (Output(1), Output(2))
            assert (
                execution.result()
                == tuple(execution)
                == tuple(Output(i) for i in range(3))
            )
    assert not executor.closed


@pytest.mark.parametrize("executor_type", [SyncExecutor, AsyncExecutor])
def test_custom_executor_async_stream_has_the_same_backpressure_contract(executor_type):
    with Runtime(plugins=(LocalRuntimePlugin(executor=executor_type()),)) as runtime:
        runtime.register("work", graph())

        async def consume():
            seen = []
            async for output in runtime.aiter("work", 30, output_buffer=1):
                seen.append(output)
                await asyncio.sleep(0)
            return tuple(seen)

        assert asyncio.run(consume()) == tuple(Output(i) for i in range(30))


@pytest.mark.parametrize("control", ["cancel", "timeout"])
def test_async_executor_cleanup_precedes_terminal_status(control):
    entered, cleaning, release = ThreadEvent(), ThreadEvent(), ThreadEvent()

    class Slow(AsyncExecutor):
        async def execute(
            self, name, graph, inputs, emit, plan=None, *, slot=None, execution
        ):
            with execution.step(graph.entrypoint):
                entered.set()
                try:
                    await asyncio.sleep(60)
                finally:
                    cleaning.set()
                    while not release.is_set():
                        await asyncio.sleep(0.005)

    with Runtime(plugins=(LocalRuntimePlugin(executor=Slow()),)) as runtime:
        runtime.register("work", graph())
        execution = runtime.start(
            "work", 1, timeout=0.05 if control == "timeout" else None
        )
        try:
            assert entered.wait(1)
            if control == "cancel":
                execution.cancel()
            assert cleaning.wait(1)
            assert not execution.wait(0.05)
        finally:
            release.set()
        with pytest.raises(
            ExecutionCancelledError if control == "cancel" else ExecutionTimeoutError
        ):
            execution.result(2)


class DiskStore:
    def __init__(self, path):
        self.path = path
        self.reads = 0
        with self.connect() as connection:
            connection.execute(
                "CREATE TABLE outputs (id INTEGER PRIMARY KEY, value BLOB)"
            )

    def connect(self):
        # Each operation owns its connection; retained Executions need no open handles.
        from contextlib import contextmanager

        @contextmanager
        def connection():
            conn = sqlite3.connect(self.path)
            try:
                with conn:
                    yield conn
            finally:
                conn.close()

        return connection()

    def __len__(self):
        with self.connect() as connection:
            return connection.execute("SELECT count(*) FROM outputs").fetchone()[0]

    def __getitem__(self, index):
        self.reads += 1
        with self.connect() as connection:
            row = connection.execute(
                "SELECT value FROM outputs WHERE id = ?", (index + 1,)
            ).fetchone()
        if row is None:
            raise IndexError(index)
        return pickle.loads(row[0])

    def append(self, output):
        with self.connect() as connection:
            connection.execute(
                "INSERT INTO outputs(value) VALUES (?)", (pickle.dumps(output),)
            )


class Notifier:
    def __init__(self):
        self.delegate = LocalExecutionNotifier()
        self.sync_waits = self.async_waits = 0

    @property
    def version(self):
        return self.delegate.version

    def notify(self):
        self.delegate.notify()

    def wait(self, version, timeout=None):
        self.sync_waits += 1
        self.delegate.wait(version, timeout)

    async def wait_async(self, version):
        self.async_waits += 1
        await self.delegate.wait_async(version)


def test_plugin_execution_factory_replaces_storage_for_direct_and_queued_work(tmp_path):
    stores = []

    def factory(name, *, limits, id=None, output_buffer=64):
        store = DiskStore(tmp_path / f"{len(stores)}.sqlite")
        stores.append(store)
        return Execution(
            name,
            limits=limits,
            id=id,
            output_buffer=output_buffer,
            output_store=store,
            notifier=Notifier(),
        )

    class Resources:
        descriptor = PluginDescriptor(
            "example/execution", "1", provides=(CAP_EXECUTION_FACTORY,)
        )

        def setup(self, context):
            context.provide(CAP_EXECUTION_FACTORY, factory)

        def start(self, context):
            pass

        def stop(self, context):
            pass

    with Runtime(plugins=(Resources(),)) as runtime:
        runtime.register("work", graph())
        execution = runtime.start("work", 20)
        assert execution.wait(3)
        assert stores[0].reads == 0
        assert tuple(execution) == tuple(Output(i) for i in range(20))
        assert stores[0].reads == 20
        assert execution.result() == tuple(Output(i) for i in range(20))
        runtime.on("run", graph="work", queue="queue")
        runtime.emit("run", 5)
        runtime.wait_idle(3)
        assert len(stores) == 2 and stores[1].reads == 0
        queued = runtime.executions()[-1]
        assert queued.result() == tuple(Output(i) for i in range(5))


def test_notifier_wakes_async_result_and_stream_without_waiting_threads(monkeypatch):
    async def forbidden(*args, **kwargs):
        raise AssertionError("async waits must not consume a worker thread")

    monkeypatch.setattr(asyncio, "to_thread", forbidden)
    notifier = Notifier()
    execution = Execution("work", notifier=notifier, output_buffer=1)
    execution.start(graph().freeze())

    async def scenario():
        stream = execution.__aiter__()

        async def result_waiter():
            return await execution

        result = asyncio.create_task(result_waiter())
        first = asyncio.create_task(stream.__anext__())
        await asyncio.sleep(0)
        await execution.apublish_output(Output(1))
        assert await first == Output(1)
        execution.succeed()
        with pytest.raises(StopAsyncIteration):
            await stream.__anext__()
        assert await result == (Output(1),)

    asyncio.run(scenario())
    assert notifier.async_waits >= 2


def test_notifier_does_not_lose_notification_before_wait():
    notifier = LocalExecutionNotifier()
    old = notifier.version
    notifier.notify()
    notifier.wait(old, 0)
    asyncio.run(asyncio.wait_for(notifier.wait_async(old), 0.1))


def test_public_execution_control_rejects_invalid_lifecycle_and_outputs():
    execution = Execution("work")
    with pytest.raises(RuntimeError, match="RUNNING"):
        execution.publish_output(Output(1))
    with pytest.raises(TypeError, match="frozen"):
        execution.start(graph())
    execution.start(graph().freeze())
    with pytest.raises(TypeError, match="only Output"):
        execution.publish_output(1)
    execution.publish_output(Output(2))
    execution.succeed()
    with pytest.raises(RuntimeError, match="RUNNING"):
        execution.publish_output(Output(3))
    assert execution.result() == (Output(2),)


def test_execution_factory_cannot_silently_change_limits():
    def broken(name, **kwargs):
        return Execution(name, limits=ExecutionLimits(max_steps=999))

    with Runtime(plugins=(LocalRuntimePlugin(execution_factory=broken),)) as runtime:
        runtime.register("work", graph())
        with pytest.raises(ValueError, match="changed the execution contract"):
            runtime.start("work", 1, max_steps=1)


def test_executor_cannot_return_a_second_result_channel():
    class Invalid(SyncExecutor):
        def execute(
            self, name, graph, inputs, emit, plan=None, *, slot=None, execution
        ):
            execution.publish_output(Output("accepted"))
            return (Output("not accepted"),)

    with Runtime(plugins=(LocalRuntimePlugin(executor=Invalid()),)) as runtime:
        runtime.register("work", graph())
        stream = runtime.iter("work", 1)
        assert next(stream) == Output("accepted")
        with pytest.raises(TypeError, match="return None"):
            next(stream)


def test_host_rejects_foreign_plan_before_calling_alternate_executor():
    class NeverCalled(SyncExecutor):
        def execute(self, *args, **kwargs):
            raise AssertionError("foreign plan reached executor")

    foreign = graph().plan(include={"produce"})
    with Runtime(plugins=(LocalRuntimePlugin(executor=NeverCalled()),)) as runtime:
        runtime.register("work", graph())
        with pytest.raises(ValueError, match="different Graph"):
            runtime.run("work", 1, plan=foreign)


def test_output_store_failure_preserves_previously_accepted_outputs():
    class FailingStore:
        def __init__(self):
            self.items = []

        def __len__(self):
            return len(self.items)

        def __getitem__(self, index):
            return self.items[index]

        def append(self, output):
            if self.items:
                raise OSError("storage unavailable")
            self.items.append(output)

    def factory(name, **kwargs):
        return Execution(name, **kwargs, output_store=FailingStore())

    with Runtime(plugins=(LocalRuntimePlugin(execution_factory=factory),)) as runtime:
        runtime.register("work", graph())
        stream = runtime.iter("work", 2)
        assert next(stream) == Output(0)
        with pytest.raises(OSError, match="storage unavailable"):
            next(stream)


def test_cancelling_a_notifier_wait_does_not_cancel_other_waiters():
    notifier = LocalExecutionNotifier()

    async def scenario():
        first = asyncio.create_task(notifier.wait_async(notifier.version))
        second = asyncio.create_task(notifier.wait_async(notifier.version))
        await asyncio.sleep(0)
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first
        assert not second.done()
        notifier.notify()
        await asyncio.wait_for(second, 1)

    asyncio.run(scenario())
