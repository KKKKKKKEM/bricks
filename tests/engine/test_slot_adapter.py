"""独立本地适配器仅使用公共 Slot 能力的契约测试。"""

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
    """验证已接受投递分发失败时只释放接管的引用。

    Args:
        monkeypatch: 当前用例使用的 monkeypatch 夹具或参数化输入。
    """

    class RejectedExecutor:
        def __init__(self, **kwargs):
            """初始化实例及其依赖，建立当前对象独立维护的状态。

            Args:
                **kwargs: 传给目标接口的关键字参数。
            """

            pass

        def submit(self, *args):
            """向命名执行通道提交工作。

            Args:
                *args: 调用协议传入的位置参数。

            Raises:
                RuntimeError: 当前生命周期状态或操作顺序不允许此操作。
            """

            raise RuntimeError("thread submission failed")

        def shutdown(self, **kwargs):
            """记录测试线程池的关闭操作。

            Args:
                **kwargs: 传给目标接口的关键字参数。
            """

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
    """仅依赖公开能力、不访问核心私有状态的独立协议实现。

    Attributes:
        lease: 当前测试使用的资源引用句柄。
        executions: 测试记录的执行对象集合。
        retained: 测试记录的引用接管次数。
        released: 测试记录的引用释放次数。
    """

    def __init__(self, lease: SlotLease):
        """初始化实例及其依赖，建立当前对象独立维护的状态。

        Args:
            lease: 当前进程内执行槽的引用与串行执行能力。
        """

        self.lease = lease
        self.executions = 0
        self.retained = 0
        self.released = 0

    @property
    def slot(self):
        """返回当前 lease 关联的本地执行槽。

        Returns:
            当前逻辑链使用的本地 Slot。
        """

        return self.lease.slot

    def retain(self):
        """为同一逻辑链新增一个必须由接管方释放的引用。"""

        self.retained += 1
        self.lease.retain()

    def release(self):
        """释放当前持有的 lease 引用，最后一个引用结束时归还 Slot。"""

        self.released += 1
        self.lease.release()

    @contextmanager
    def execution(self):
        """在 lease 保护下串行使用同一个 Slot 的执行资源。

        Yields:
            在引用保护与串行锁内使用的测试 Slot。
        """

        with self.lease.execution() as slot:
            self.executions += 1
            yield slot


class PumpTasks:
    """由测试显式推进排队投递的确定性适配器。

    Attributes:
        queued: 等待执行的投递队列。
        consumers: 通道内按注册顺序排列的消费者。
        leases: 测试提供者创建的 lease 集合。
        detachers: 测试装配产生的卸载函数集合。
        notifications: 测试通知器记录的通知次数。
        failures: 测试适配器收集的失败记录。
        reject_local: 是否主动拒绝本地投递以验证失败引用归属。
    """

    def __init__(self):
        """初始化实例及其依赖，建立当前对象独立维护的状态。"""

        self.queued: deque[tuple[str, Delivery]] = deque()
        self.consumers = {}
        self.leases = []
        self.detachers = []
        self.notifications = 0
        self.failures = []
        self.reject_local = False

    @property
    def idle(self):
        """判断当前组件是否没有尚未完成的工作。

        Returns:
            没有未完成工作时为 True，否则为 False。
        """

        return not self.queued

    def bind(self, queue, handler, *, concurrency, slots=None):
        """绑定消费通道及其处理器和本地并发配置。

        Args:
            queue: 命名消费通道。
            handler: 接收事件或投递的处理函数。
            concurrency: 当前消费者允许并行执行的完整 Graph 数量。
            slots: 提供本地执行槽的资源池能力。
        """

        assert concurrency == 1
        assert slots is not None
        self.consumers[queue] = (handler, slots)
        self.detachers.append(slots.subscribe_available(self.available))

    def available(self):
        """返回当前可申请的执行槽数量。"""

        self.notifications += 1

    def submit(self, queue, work):
        """向命名执行通道提交工作。

        Args:
            queue: 命名消费通道。
            work: 需要投递或执行的工作请求。
        """

        self.queued.append((queue, Delivery(work)))

    def submit_local(self, queue, work, lease):
        """提交延续本地 Slot 链的工作，成功后接管传入的引用。

        Args:
            queue: 命名消费通道。
            work: 需要投递或执行的工作请求。
            lease: 当前进程内执行槽的引用与串行执行能力。

        Raises:
            ValueError: 参数值或字段组合不合法。
        """

        if self.reject_local:
            raise ValueError("local publication rejected")
        self.queued.append((queue, Delivery(work, slot_lease=lease)))

    def pump(self):
        """显式推进测试适配器的排队投递。

        Returns:
            当前测试回调收集或构造的结果。
        """

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
        """等待已接受的工作完成，并传播已记录的失败。

        Args:
            timeout: 等待或执行时限，单位秒；None 表示不设置时限。

        Raises:
            TimeoutError: 等待未在指定时限内完成。
        """

        while not self.idle:
            if not self.pump():
                raise TimeoutError("no Slot available")
        if self.failures:
            error = self.failures.pop(0)
            raise error

    def close(self):
        """结束当前组件的生命周期并释放其拥有的资源。"""

        self.wait_idle()
        for detach in self.detachers:
            detach()


@pytest.mark.parametrize("failure", [None, "graph", "publication"])
def test_independent_adapter_preserves_chain_and_recovers_pool(failure):
    """验证独立适配器保留执行链并最终归还资源池。

    Args:
        failure: 先前已经记录的失败，None 表示没有失败。
    """

    tasks = PumpTasks()
    tasks.reject_local = failure == "publication"
    bus = EventBus()
    root_pool, other_pool = SlotPool(1), SlotPool(1)
    seen = []

    class Source(Node):
        def execute(self, inputs, context):
            """执行当前测试场景的节点行为，供外层契约断言检查。

            Args:
                inputs: 入口数据或按端口名称组织的输入映射。
                context: 当前调用的执行或插件上下文。
            """

            assert context.slot is not None
            context.slot["root"] = inputs["default"]
            seen.append(("source", context.slot, context.slot["root"]))
            if inputs["default"] == 1:
                context.emit("next")
                context.emit("next")

    class Target(Node):
        def execute(self, inputs, context):
            """执行当前测试场景的节点行为，供外层契约断言检查。

            Args:
                inputs: 入口数据或按端口名称组织的输入映射。
                context: 当前调用的执行或插件上下文。

            Raises:
                ValueError: 参数值或字段组合不合法。
            """

            assert context.slot is not None
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
