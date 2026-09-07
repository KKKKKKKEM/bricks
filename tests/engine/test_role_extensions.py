"""Structural role and pool replacements need no default-class inheritance."""

from collections import deque
from contextlib import contextmanager
from threading import Condition, RLock
import time

import pytest

from bricks import Graph, Node, Output, Runtime, Slot, SlotPool
from bricks.adapters import memory
from bricks.plugins import CAP_EVENT_ROUTER, CAP_GRAPH_WORKER, PluginDescriptor
from bricks.runtime import EventRouter, GraphWorker
from bricks.spi import RouterRole, SlotProvider, WorkerRole


class RouterWrapper:
    def __init__(self, router):
        self.router = router
        self.closed = False

    @property
    def idle(self):
        return self.router.idle

    def observe(self, *args, **kwargs):
        return self.router.observe(*args, **kwargs)

    def route(self, *args, **kwargs):
        return self.router.route(*args, **kwargs)

    def emit(self, *args, **kwargs):
        return self.router.emit(*args, **kwargs)

    def publish(self, event):
        self.router.publish(event)

    def observe_runtime(self, observer):
        return self.router.observe_runtime(observer)

    def wait_idle(self, timeout=None):
        self.router.wait_idle(timeout)

    def close(self):
        self.closed = True
        self.router.close()


class WorkerWrapper:
    def __init__(self, worker):
        self.worker = worker
        self.closed = False

    @property
    def idle(self):
        return self.worker.idle

    def register(self, *args, **kwargs):
        return self.worker.register(*args, **kwargs)

    def consume(self, *args, **kwargs):
        return self.worker.consume(*args, **kwargs)

    def start(self, *args, **kwargs):
        return self.worker.start(*args, **kwargs)

    def iter(self, *args, **kwargs):
        return self.worker.iter(*args, **kwargs)

    def aiter(self, *args, **kwargs):
        return self.worker.aiter(*args, **kwargs)

    def get_execution(self, execution_id):
        return self.worker.get_execution(execution_id)

    def executions(self):
        return self.worker.executions()

    def attach(self, *args, **kwargs):
        return self.worker.attach(*args, **kwargs)

    def contribute_hook(self, *args, **kwargs):
        return self.worker.contribute_hook(*args, **kwargs)

    def register_policy(self, *args, **kwargs):
        return self.worker.register_policy(*args, **kwargs)

    def observe_runtime(self, observer):
        return self.worker.observe_runtime(observer)

    def wait_idle(self, timeout=None):
        self.worker.wait_idle(timeout)

    def close(self):
        self.closed = True
        self.worker.close()


class Echo(Node):
    def execute(self, inputs, context):
        return Output(inputs["default"])


@pytest.mark.parametrize("wrapped", ["router", "worker", "both"])
@pytest.mark.parametrize("plugin", [False, True])
def test_roles_can_be_independently_decorated_and_composed_without_inheritance(
    wrapped, plugin
):
    tasks = memory.TaskBackend()
    router = EventRouter(publisher=tasks)
    worker = GraphWorker(
        consumer=tasks, emit=router.publish, emit_local=router.publish_local
    )
    selected_router = RouterWrapper(router) if wrapped in ("router", "both") else router
    selected_worker = WorkerWrapper(worker) if wrapped in ("worker", "both") else worker
    assert isinstance(selected_router, RouterRole)
    assert isinstance(selected_worker, WorkerRole)

    class Roles:
        descriptor = PluginDescriptor(
            "example/roles", "1", provides=(CAP_EVENT_ROUTER, CAP_GRAPH_WORKER)
        )

        def setup(self, context):
            context.provide(CAP_EVENT_ROUTER, selected_router)
            context.provide(CAP_GRAPH_WORKER, selected_worker)

        def start(self, context):
            pass

        def stop(self, context):
            selected_worker.close()
            selected_router.close()

    runtime = (
        Runtime(plugins=(Roles(),))
        if plugin
        else Runtime(router=selected_router, worker=selected_worker)
    )
    observations = []
    try:
        runtime.observe_runtime(observations.append)
        runtime.register("echo", Graph(entrypoint="node").add(node=Echo()))
        assert runtime.run("echo", 1) == (Output(1),)
        assert tuple(runtime.iter("echo", 2)) == (Output(2),)
        runtime.on("event", graph="echo", queue="work")
        runtime.emit("event", 3)
        runtime.wait_idle(2)
        assert runtime.executions()[-1].result() == (Output(3),)
        assert observations
    finally:
        runtime.close()
        tasks.close()
    if wrapped in ("router", "both"):
        assert selected_router.closed
    if wrapped in ("worker", "both"):
        assert selected_worker.closed


class ResourceLease:
    def __init__(self, provider, slot):
        self.provider = provider
        self.slot = slot
        self.references = 1
        self.lock = RLock()
        self.execution_lock = RLock()

    def retain(self):
        with self.lock:
            if not self.references:
                raise RuntimeError("released")
            self.references += 1

    def release(self):
        with self.lock:
            if not self.references:
                raise RuntimeError("released")
            self.references -= 1
            last = not self.references
        if last:
            self.provider.put(self.slot)

    @contextmanager
    def execution(self):
        self.retain()
        try:
            with self.execution_lock:
                yield self.slot
        finally:
            self.release()


class ResourcePool:
    def __init__(self, size):
        self.size = size
        self.slots = deque(Slot() for _ in range(size))
        self.condition = Condition()
        self.listeners = []
        self.closed = False

    @property
    def available(self):
        with self.condition:
            return len(self.slots)

    def try_acquire(self):
        with self.condition:
            if self.closed:
                raise RuntimeError("closed")
            return ResourceLease(self, self.slots.popleft()) if self.slots else None

    def acquire(self, timeout=None):
        deadline = None if timeout is None else time.monotonic() + timeout
        with self.condition:
            while True:
                lease = self.try_acquire()
                if lease is not None:
                    return lease
                remaining = None if deadline is None else deadline - time.monotonic()
                if remaining is not None and remaining <= 0:
                    raise TimeoutError()
                self.condition.wait(remaining)

    def subscribe_available(self, listener):
        self.listeners.append(listener)

        def detach():
            if listener in self.listeners:
                self.listeners.remove(listener)

        return detach

    def put(self, slot):
        with self.condition:
            self.slots.append(slot)
            self.condition.notify_all()
        for listener in tuple(self.listeners):
            listener()

    def close(self):
        with self.condition:
            self.closed = True
            self.condition.notify_all()


def test_non_default_slot_provider_and_lease_preserve_cross_graph_resources():
    pool = ResourcePool(1)
    assert isinstance(pool, SlotProvider) and not isinstance(pool, SlotPool)
    seen = []

    class Source(Node):
        def execute(self, inputs, context):
            context.slot["value"] = inputs["default"]
            seen.append(context.slot)
            context.emit("next")

    class Target(Node):
        def execute(self, inputs, context):
            seen.append(context.slot)
            return Output(context.slot["value"])

    with Runtime() as runtime:
        runtime.register("source", Graph(entrypoint="node").add(node=Source()))
        runtime.register("target", Graph(entrypoint="node").add(node=Target()))
        runtime.on("root", graph="source", queue="roots", slots=pool)
        runtime.on("next", graph="target", queue="targets", slots=pool)
        runtime.emit("root", 42)
        runtime.wait_idle(2)
        assert seen[0] is seen[1]
        assert runtime.executions()[-1].result() == (Output(42),)
        assert pool.available == 1
    assert not pool.closed
    pool.close()
