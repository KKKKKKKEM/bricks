"""A separately implemented local adapter uses only public Slot capabilities."""

from collections import deque
from contextlib import contextmanager

import pytest

from bricks import Event, Graph, Node, Runtime, SlotPool
from bricks.adapters import memory
from bricks.adapters.memory import EventBus
from bricks.runtime import EventRouter, GraphWorker
from bricks.spi import Delivery, DeliveryOutcome, SlotLease


def test_accepted_delivery_dispatch_failure_releases_only_transferred_reference(
    monkeypatch,
):
    class RejectedExecutor:
        def __init__(self, **kwargs):
            pass

        def submit(self, *args):
            raise RuntimeError("thread submission failed")

        def shutdown(self, **kwargs):
            pass

    monkeypatch.setattr(memory, "ThreadPoolExecutor", RejectedExecutor)
    pool = SlotPool(1)
    tasks = memory.TaskBackend()
    router = EventRouter(publisher=tasks)
    lease = pool.acquire()
    try:
        tasks.bind(
            "queue",
            lambda delivery: pytest.fail("must not execute"),
            concurrency=1,
            slots=pool,
        )
        router.route("start", graph="graph", queue="queue")
        router.publish_local(Event("start"), lease)
        with pytest.raises(RuntimeError, match="thread submission failed"):
            tasks.wait_idle(1)
        assert pool.available == 0
        assert lease.slot is not None
    finally:
        lease.release()
        assert pool.available == 1
        router.close()
        tasks.close()
        pool.close()


class LeaseView:
    """An independent protocol implementation with no core-private access."""

    def __init__(self, lease: SlotLease):
        self.lease = lease
        self.executions = 0
        self.retained = 0
        self.released = 0

    @property
    def slot(self):
        return self.lease.slot

    def retain(self):
        self.retained += 1
        self.lease.retain()

    def release(self):
        self.released += 1
        self.lease.release()

    @contextmanager
    def execution(self):
        with self.lease.execution() as slot:
            self.executions += 1
            yield slot


class PumpTasks:
    """Deterministic adapter; the test explicitly pumps queued deliveries."""

    def __init__(self):
        self.queued = deque()
        self.consumers = {}
        self.leases = []
        self.detachers = []
        self.notifications = 0
        self.failures = []
        self.reject_local = False

    @property
    def idle(self):
        return not self.queued

    def bind(self, queue, handler, *, concurrency, slots):
        assert concurrency == 1
        self.consumers[queue] = (handler, slots)
        self.detachers.append(slots.subscribe_available(self.available))

    def available(self):
        self.notifications += 1

    def submit(self, queue, work):
        self.queued.append((queue, Delivery(work)))

    def submit_local(self, queue, work, lease):
        if self.reject_local:
            raise ValueError("local publication rejected")
        self.queued.append((queue, Delivery(work, slot_lease=lease)))

    def pump(self):
        ordered = sorted(
            tuple(self.queued), key=lambda item: item[1].slot_lease is None
        )
        for queue, delivery in ordered:
            handler, slots = self.consumers[queue]
            lease = delivery.slot_lease
            if lease is None:
                acquired = slots.try_acquire()
                if acquired is None:
                    continue
                lease = LeaseView(acquired)
                self.leases.append(lease)
            self.queued.remove((queue, delivery))
            try:
                result = handler(Delivery(delivery.work, slot_lease=lease))
                if result.outcome is not DeliveryOutcome.ACK:
                    self.failures.append(result.error or RuntimeError("rejected"))
            finally:
                lease.release()
            return True
        return False

    def wait_idle(self, timeout=None):
        while not self.idle:
            if not self.pump():
                raise TimeoutError("no Slot available")
        if self.failures:
            error = self.failures.pop(0)
            raise error

    def close(self):
        self.wait_idle()
        for detach in self.detachers:
            detach()


@pytest.mark.parametrize("failure", [None, "graph", "publication"])
def test_independent_adapter_preserves_chain_and_recovers_pool(failure):
    tasks = PumpTasks()
    tasks.reject_local = failure == "publication"
    bus = EventBus()
    root_pool, other_pool = SlotPool(1), SlotPool(1)
    seen = []

    class Source(Node):
        def execute(self, inputs, context):
            context.slot["root"] = inputs["default"]
            seen.append(("source", context.slot, context.slot["root"]))
            if inputs["default"] == 1:
                context.emit("next")
                context.emit("next")

    class Target(Node):
        def execute(self, inputs, context):
            seen.append(("target", context.slot, context.slot["root"]))
            if failure == "graph":
                raise ValueError("target failed")

    router = EventRouter(events=bus, publisher=tasks)
    worker = GraphWorker(
        consumer=tasks, emit=router.publish, emit_local=router.publish_local
    )
    runtime = Runtime(router=router, worker=worker)
    try:
        runtime.register("source", Graph(entrypoint="node").add(node=Source()))
        runtime.register("target", Graph(entrypoint="node").add(node=Target()))
        runtime.on("root", graph="source", queue="roots", slots=root_pool)
        runtime.on("next", graph="target", queue="targets", slots=other_pool)
        runtime.emit("root", 1)
        runtime.emit("root", 2)
        assert tasks.pump()
        if failure != "publication":
            assert root_pool.available == 0
            assert other_pool.available == 1
            assert tasks.pump()
            assert root_pool.available == 0
            assert tasks.pump()
        assert root_pool.available == 1
        assert tasks.pump()
        while tasks.failures:
            with pytest.raises(
                Exception, match="target failed|local publication rejected"
            ):
                runtime.wait_idle(1)
        runtime.wait_idle(1)
        expected = (
            ["source", "source"]
            if failure == "publication"
            else ["source", "target", "target", "source"]
        )
        assert [item[0] for item in seen] == expected
        assert all(item[1] is seen[0][1] for item in seen)
        assert seen[-1][2] == 2
        assert sum(lease.executions for lease in tasks.leases) == len(seen)
        assert tasks.leases[0].released == tasks.leases[0].retained + 1
        assert tasks.notifications > 0
        assert root_pool.available == other_pool.available == 1
    finally:
        runtime.close()
        tasks.close()
        bus.close()
        root_pool.close()
        other_pool.close()
