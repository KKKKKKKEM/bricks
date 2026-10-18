"""本地与真实 Redis 的队列状态、租约和跨进程交付契约。"""

from __future__ import annotations

import json
import multiprocessing
import os
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from dataclasses import replace
from threading import Event as ThreadEvent
from typing import Any
from uuid import uuid4

import pytest
from interlace import Event, Graph, Node, Runtime
from interlace.engine import SlotPool
from interlace.runtime import LocalInfrastructurePlugin
from interlace.spi import Delivery, DeliveryResult, ExecutionLimits, Work

from bricks.parsers import MISSING
from bricks.queues import (
    LeaseLostError,
    MemoryTaskQueue,
    QueueBackend,
    TaskConflictError,
    make_work,
)
from bricks.queues.redis import RedisTaskQueue


@pytest.fixture(params=["memory", "redis"])
def queue(request):
    """为每个测试提供隔离队列，真实 Redis 通过显式环境变量启用。

    Args:
        request: 当前参数化测试请求。

    Yields:
        仅本测试拥有的队列。
    """
    name = uuid4().hex
    if request.param == "memory":
        yield MemoryTaskQueue(name)
        return
    url = os.environ.get("BRICKS_TEST_REDIS_URL")
    if not url:
        pytest.skip("set BRICKS_TEST_REDIS_URL for real Redis contracts")
    from redis import Redis

    client = Redis.from_url(url, socket_timeout=2, socket_connect_timeout=2)
    prefix = f"bricks-test:{name}"
    try:
        client.ping()
        yield RedisTaskQueue(client, name, prefix=prefix)
    finally:
        keys = list(client.scan_iter(match=f"{prefix}:*"))
        if keys:
            client.delete(*keys)
        client.close()


def test_atomic_submission_snapshot_and_deduplication(queue):
    """验证稳定 ID、内容冲突、独立副本和 JSON 无损传输。

    Args:
        queue: 本地或真实 Redis 队列。
    """
    payload = {"list": [1, None, False, {}, []], "large": 2**100, "text": "中文"}
    work = Work(
        "crawl@v1",
        payload,
        id="seed-1",
        trigger=Event("seeds", {"n": 1}),
        limits=ExecutionLimits(max_steps=12, timeout=10),
        options={"crawler.session.reuse": False},
    )
    assert queue.put(work)
    assert not queue.put(work)
    payload["list"].append(9)
    with pytest.raises(TaskConflictError):
        queue.put(work)
    lease = queue.claim("worker")
    assert lease.work.inputs["large"] == 2**100
    assert lease.work.inputs["list"] == [1, None, False, {}, []]
    assert lease.work.trigger == Event("seeds", {"n": 1})
    assert lease.work.options["crawler.session.reuse"] is False
    assert lease.work.limits == work.limits
    lease.work.inputs["list"].clear()
    assert queue.get("seed-1").work.inputs["list"] == [1, None, False, {}, []]
    queue.ack(lease)
    record = queue.get("seed-1")
    assert record.status == "acked" and record.result is None
    assert queue.counts() == {"pending": 0, "leased": 0, "acked": 1, "dead": 0}
    with pytest.raises(LeaseLostError):
        queue.ack(lease)
    assert queue.forget("seed-1")
    assert not queue.forget("seed-1")
    assert queue.put(work)


def test_content_id_deduplicates_independently_created_work(queue):
    """按内容构造的独立 Work 在两种队列中自动去重。

    Args:
        queue: 本地或真实 Redis 队列。
    """
    first = make_work("crawl@v1", {"page": 1, "filters": {"b": 2, "a": 1}})
    second = make_work("crawl@v1", {"filters": {"a": 1, "b": 2}, "page": 1})
    assert first.id == second.id
    assert first.id.startswith("sha256:") and len(first.id) == 71
    assert isinstance(first, Work)
    assert queue.put(first)
    assert not queue.put(second)
    assert queue.claim("worker").work.id == first.id
    explicit = make_work("crawl@v1", {"page": 2}, id="business-id")
    assert explicit.id == "business-id"
    assert queue.put(explicit)
    with pytest.raises(TaskConflictError):
        queue.put(make_work("crawl@v1", {"page": 3}, id="business-id"))


def test_put_claim_atomically_persists_specific_ready_work(queue):
    """指定 Work 的入队与领取原子完成，重复、延迟和终态不绕过状态。

    Args:
        queue: 本地或真实 Redis 任务队列。
    """
    work = make_work("crawl@v1", {"page": 2}, id="local-page")
    lease = queue.put_claim(work, "local-worker", lease_seconds=10)
    assert lease is not None
    assert lease.work == work and lease.attempt == 1
    assert queue.counts()["leased"] == 1
    assert queue.put_claim(work, "other-worker", lease_seconds=10) is None
    with pytest.raises(TaskConflictError):
        queue.put_claim(
            make_work("crawl@v1", {"page": 3}, id="local-page"),
            "other-worker",
        )
    queue.ack(lease)
    assert queue.put_claim(work, "other-worker") is None

    delayed = make_work("crawl@v1", {"page": 4}, id="delayed-page")
    delayed_lease = queue.put_claim(delayed, "local-worker")
    assert delayed_lease is not None
    queue.retry(delayed_lease, delay=60)
    assert queue.put_claim(delayed, "other-worker") is None


def test_put_claim_expiry_recovers_persisted_local_work():
    """本地优先 Worker 中断后，指定 Work 由过期租约恢复且拒绝旧凭据。"""
    now = [100.0]
    queue = MemoryTaskQueue("local-recovery", clock=lambda: now[0])
    work = make_work("crawl@v1", {"page": 2}, id="recover-page")
    original = queue.put_claim(work, "first", lease_seconds=5)
    assert original is not None
    now[0] += 6
    replacement = queue.put_claim(work, "second", lease_seconds=5)
    assert replacement is not None and replacement.attempt == 2
    with pytest.raises(LeaseLostError):
        queue.ack(original)
    queue.ack(replacement)
    assert queue.get(work.id).status == "acked"


def test_content_id_is_sensitive_to_execution_content_and_preserves_snapshots():
    """图、顺序、类型、执行配置和来源事件参与身份，输入副本独立。"""
    source = {"items": [1, 2]}
    baseline = make_work("crawl@v1", source)
    source["items"].append(3)
    assert baseline.inputs == {"items": [1, 2]}
    variations = [
        make_work("crawl@v2", baseline.inputs),
        make_work("crawl@v1", {"items": [2, 1]}),
        make_work("crawl@v1", {"items": [1.0, 2]}),
        make_work(
            "crawl@v1", baseline.inputs, options={"crawler.session.reuse": False}
        ),
        make_work("crawl@v1", baseline.inputs, limits=ExecutionLimits(timeout=3)),
        make_work("crawl@v1", baseline.inputs, trigger=Event("seed.created", {"n": 1})),
    ]
    assert len({baseline.id, *(work.id for work in variations)}) == 7
    assert (
        make_work("crawl@v1", baseline.inputs, options={}, limits=ExecutionLimits()).id
        == baseline.id
    )
    with pytest.raises(ValueError):
        make_work("crawl", {}, id="")
    with pytest.raises(TypeError):
        make_work("crawl", {"payload": b"not JSON"})


def test_content_id_is_stable_across_python_processes():
    """内容指纹不依赖 Python 随机哈希种子或字典插入顺序。"""
    expected = make_work("crawl", {"名称": "新闻", "id": 2**100}).id
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "from bricks.queues import make_work; print(make_work('crawl', {'id': 2**100, '名称': '新闻'}).id)",
        ],
        capture_output=True,
        text=True,
        check=True,
        env={**os.environ, "PYTHONHASHSEED": "17"},
    )
    assert result.stdout.strip() == expected


@pytest.mark.parametrize(
    "payload", [b"bytes", {1: "key"}, {"x": float("nan")}, (1, 2), object()]
)
def test_rejects_non_json_before_mutating(queue, payload):
    """本地与 Redis 均拒绝不可移植数据，不能只在远端失败。

    Args:
        queue: 隔离队列。
        payload: 非法任务输入。
    """
    with pytest.raises((TypeError, ValueError)):
        queue.put(Work("crawl", payload))
    assert sum(queue.counts().values()) == 0


def test_task_lease_expiry_fences_old_owner(queue):
    """任务接管后，旧凭据不能续租、确认、拒绝或重投。

    Args:
        queue: 隔离队列。
    """
    queue.put(Work("crawl", id="one"))
    first = queue.claim("old", lease_seconds=0.2)
    assert queue.claim("competitor") is None
    time.sleep(0.3)
    with pytest.raises(LeaseLostError):
        queue.ack(first)
    second = queue.claim("new")
    assert second.work.id == first.work.id and second.attempt == 2
    assert second.token != first.token
    for operation in (queue.renew, queue.ack, queue.retry, queue.reject):
        with pytest.raises(LeaseLostError):
            operation(first)
    with pytest.raises(LeaseLostError):
        queue.ack(replace(second, owner="forged"))
    with pytest.raises(LeaseLostError):
        queue.ack(replace(second, queue="another"))
    renewed = queue.renew(second, lease_seconds=60)
    assert renewed.token == second.token and renewed.deadline > second.deadline
    queue.ack(renewed, result="failure")
    assert queue.get("one").result == "failure"


def test_retry_delay_exhaustion_and_terminal_retention(queue):
    """延迟任务不可提前领取，耗尽次数后保留死信和去重身份。

    Args:
        queue: 隔离队列。
    """
    work = Work("crawl", id="one")
    queue.put(work)
    first = queue.claim("worker")
    with pytest.raises(ValueError):
        queue.forget("one")
    assert queue.retry(first, delay=0.2, error="temporary") == "pending"
    assert queue.claim("worker") is None
    time.sleep(0.3)
    second = queue.claim("worker")
    assert second.attempt == 2
    assert queue.retry(second) == "pending"
    third = queue.claim("worker")
    assert third.attempt == 3
    assert queue.retry(third, error="exhausted") == "dead"
    assert queue.claim("worker") is None
    assert queue.get("one").status == "dead"
    assert queue.get("one").error == "exhausted"
    assert not queue.put(work)
    assert queue.counts()["dead"] == 1


def test_crashed_final_attempt_becomes_dead(queue):
    """最后一次交付到期后不会无界重新领取。

    Args:
        queue: 隔离队列。
    """
    queue.put(Work("crawl", id="one"))
    for _ in range(2):
        queue.retry(queue.claim("worker"))
    queue.claim("crashed", lease_seconds=0.2)
    time.sleep(0.3)
    assert queue.claim("replacement") is None
    assert queue.get("one").status == "dead"
    assert queue.counts()["dead"] == 1


def test_init_lease_progress_and_fenced_submission(queue):
    """初始化租约接管保留断点，并拒绝旧生产者继续写入或宣布成功。

    Args:
        queue: 隔离队列。
    """
    assert queue.initialization() is None
    first = queue.acquire_init("old", lease_seconds=0.2)
    assert queue.acquire_init("other") is None
    progress = {"page": 2, "ids": [2**100]}
    queue.save_init(first, progress)
    progress["ids"].clear()
    queue.put(Work("crawl", id="page-1"), init=first)
    time.sleep(0.3)
    second = queue.acquire_init("new")
    assert second.fence == first.fence + 1
    assert queue.initialization().progress == {"page": 2, "ids": [2**100]}
    with pytest.raises(LeaseLostError):
        queue.put(Work("crawl", id="stale"), init=first)
    assert queue.get("stale") is None
    with pytest.raises(LeaseLostError):
        queue.save_init(first, {})
    with pytest.raises(LeaseLostError):
        queue.finish_init(first, success=True)
    renewed = queue.renew_init(second, lease_seconds=60)
    assert renewed.deadline > second.deadline
    assert type(renewed.deadline) is float
    queue.finish_init(renewed, success=False, error="producer failed")
    third = queue.acquire_init("retry")
    assert third.fence == second.fence + 1
    queue.finish_init(third, success=True)
    assert queue.acquire_init("later") is None
    assert queue.initialization().status == "succeeded"
    assert queue.counts()["pending"] == 1


def test_competing_claims_and_submission_are_atomic(queue):
    """多个线程对同一 ID 只新增一次，且交付凭据不会重复分配。

    Args:
        queue: 隔离队列。
    """
    work = Work("crawl", id="shared")
    with ThreadPoolExecutor(max_workers=8) as executor:
        assert sum(executor.map(queue.put, [work] * 16)) == 1
        claims = list(executor.map(queue.claim, ["worker"] * 16))
    assert sum(lease is not None for lease in claims) == 1
    lease = next(lease for lease in claims if lease is not None)
    queue.ack(lease, result="success")
    assert queue.get("shared").result == "success"


def test_queue_encode_names_parser_missing_sentinel(queue):
    """任务 JSON 编码在边界明确指出解析器缺失标记。"""
    with pytest.raises(TypeError, match="MISSING"):
        queue.put(Work("crawl", inputs={"value": MISSING}))


def test_claim_rejects_poison_envelope_without_stopping_queue(queue):
    """解码失败的已领取信封转为死信，后续任务仍可处理。"""
    queue._call(
        "put",
        id="poison",
        body='{"version":1,"id":"poison","graph":"crawl","inputs":{},'
        '"options":{},"limits":{"max_steps":null,"timeout":null},'
        '"trigger":null,"extra":true}',
        maximum=3,
        init=None,
    )
    assert queue.claim("worker") is None
    poisoned = queue._call("get", id="poison")
    assert poisoned["status"] == "dead"
    assert "ValueError" in poisoned["error"]
    work = Work("crawl", id="after-poison")
    queue.put(work)
    lease = queue.claim("worker")
    assert lease is not None and lease.work.id == work.id


def test_backend_retries_propagates_errors_and_preserves_unclaimed(queue):
    """适配器遵循 DeliveryResult，关闭保留未领取任务并拒绝新提交。

    Args:
        queue: 隔离队列。
    """
    attempts = []

    def handler(delivery: Delivery) -> DeliveryResult:
        """第一次要求重投，第二次接受。

        Args:
            delivery: 本地交付。

        Returns:
            明确的交付决定。
        """
        attempts.append(delivery.attempt)
        assert delivery.slot_lease is not None
        return DeliveryResult.retry() if delivery.attempt == 1 else DeliveryResult.ack()

    backend = QueueBackend({queue.name: queue}, poll_interval=0.005)
    unbind = backend.bind(queue.name, handler, concurrency=2)
    backend.submit(queue.name, Work("crawl", id="one"))
    backend.wait_idle(5)
    assert attempts == [1, 2]
    unbind()
    unbind()
    backend.submit(queue.name, Work("crawl", id="unclaimed"))
    backend.close()
    backend.close()
    assert queue.get("unclaimed").status == "pending"
    with pytest.raises(RuntimeError):
        backend.submit(queue.name, Work("crawl"))


def test_backend_wakeup_removes_fixed_polling_throughput_cap(queue):
    """交付结束立即唤醒领取器，快速任务不受固定轮询间隔限速。"""
    backend = QueueBackend({queue.name: queue}, poll_interval=0.05)
    backend.bind(queue.name, lambda delivery: DeliveryResult.ack(), concurrency=1)
    for index in range(40):
        backend.submit(queue.name, Work("crawl", id=f"fast-{index}"))
    started = time.monotonic()
    try:
        backend.wait_idle(4)
        assert time.monotonic() - started < 1.5
    finally:
        backend.close()


def test_handler_control_failure_is_retried_and_propagated(queue):
    """处理器控制异常有界重投并传播，不直接 reject 为死信。"""
    backend = QueueBackend({queue.name: queue}, poll_interval=0.005)
    retry_started, release = ThreadEvent(), ThreadEvent()

    def handler(delivery: Delivery) -> DeliveryResult:
        """首次抛出控制异常，第二次等待测试释放后确认。"""
        if delivery.attempt == 1:
            raise KeyboardInterrupt()
        retry_started.set()
        assert release.wait(3)
        return DeliveryResult.ack()

    backend.bind(queue.name, handler, concurrency=1)
    backend.submit(queue.name, Work("crawl", id="control"))
    try:
        with pytest.raises(KeyboardInterrupt):
            backend.wait_idle(3)
        assert retry_started.wait(1)
        assert queue._call("get", id="control")["status"] == "leased"
    finally:
        release.set()
        backend.close()


def test_backend_heartbeats_and_unbind_wait_for_slot_release(queue, renewals):
    """长执行持续续租，注销会等待执行结束和 Slot 引用释放。

    Args:
        queue: 隔离队列。
        renewals: 续租观察器工厂。
    """
    started, release = ThreadEvent(), ThreadEvent()
    pool = SlotPool(1)

    def handler(delivery: Delivery) -> DeliveryResult:
        """保持执行直到测试允许结束。

        Args:
            delivery: 带 Slot 的交付。

        Returns:
            ACK。
        """
        assert delivery.slot_lease is not None
        started.set()
        assert release.wait(5)
        return DeliveryResult.ack()

    watch = renewals(queue)
    backend = QueueBackend({queue.name: queue}, lease_seconds=0.6, poll_interval=0.01)
    unbind = backend.bind(queue.name, handler, concurrency=1, slots=pool)
    backend.submit(queue.name, Work("crawl", id="long"))
    try:
        assert started.wait(5)
        expiry = time.monotonic() + 0.6
        with ThreadPoolExecutor(max_workers=1) as executor:
            closing = executor.submit(unbind)
            watch.wait_past(expiry)
            assert not closing.done()
            assert queue.claim("competitor") is None
            assert not backend.idle
            release.set()
            closing.result(5)
        backend.wait_idle(5)
        lease = pool.try_acquire()
        assert lease is not None
        lease.release()
    finally:
        release.set()
        backend.close()
        pool.close()


def test_backend_local_work_persists_and_prefers_transferred_slot(queue):
    """本地 Work 持久化并优先使用转交 Slot，失败仍保留队列状态。

    Args:
        queue: 隔离的内存或 Redis 任务存储。
    """
    pool = SlotPool(2)
    backend = QueueBackend({queue.name: queue}, poll_interval=0.005)
    seen = []
    retries = []
    failure = TimeoutError("local business timeout")

    def handler(delivery: Delivery) -> DeliveryResult:
        """记录本地交付携带的 Slot。

        Args:
            delivery: 带调用方转交 lease 的本地 Work。

        Returns:
            ACK。
        """
        assert delivery.slot_lease is not None
        seen.append((delivery.work.inputs, id(delivery.slot_lease.slot)))
        if delivery.work.inputs == {"page": 3}:
            return DeliveryResult.reject(failure)
        if delivery.work.inputs == {"page": 4}:
            retries.append((delivery.attempt, id(delivery.slot_lease.slot)))
            if delivery.attempt == 1:
                return DeliveryResult.retry()
        return DeliveryResult.ack()

    backend.bind(queue.name, handler, concurrency=2, slots=pool)
    lease = pool.acquire()
    identity = id(lease.slot)
    try:
        backend.submit_local(queue.name, Work("crawl", {"page": 2}), lease)
        backend.wait_idle(5)
        assert seen == [({"page": 2}, identity)]
        assert queue.counts()["acked"] == 1
        assert pool.available == 2
        failed_lease = pool.acquire()
        backend.submit_local(queue.name, Work("crawl", {"page": 3}), failed_lease)
        with pytest.raises(TimeoutError) as caught:
            backend.wait_idle(5)
        assert caught.value is failure
        assert pool.available == 2
        retry_lease = pool.acquire()
        retry_identity = id(retry_lease.slot)
        backend.submit_local(queue.name, Work("crawl", {"page": 4}), retry_lease)
        backend.wait_idle(5)
        assert retries[0] == (1, retry_identity)
        assert [attempt for attempt, _ in retries] == [1, 2]
        assert pool.available == 2
    finally:
        backend.close()
        pool.close()


def test_backend_local_execution_uses_persisted_work_snapshot(queue):
    """本地提交执行存储领取快照而非调用方可变 Work。

    Args:
        queue: 隔离的内存或 Redis 任务存储。
    """
    original_put_claim = queue.put_claim
    observed = []

    def put_claim_then_mutate(work, owner, *, lease_seconds=30):
        """在原子领取完成后模拟生产者修改原 Work。

        Args:
            work: 正在持久化的任务。
            owner: 领取者身份。
            lease_seconds: 领取租约时长。

        Returns:
            存储创建的领取凭据。
        """
        lease = original_put_claim(work, owner, lease_seconds=lease_seconds)
        work.inputs["value"] = "changed"
        return lease

    queue.put_claim = put_claim_then_mutate
    pool = SlotPool(1)

    def handler(delivery: Delivery) -> DeliveryResult:
        """记录本地处理器实际收到的输入。

        Args:
            delivery: 已领取的本地任务。

        Returns:
            确认交付。
        """
        observed.append(delivery.work.inputs.copy())
        return DeliveryResult.ack()

    backend = QueueBackend({queue.name: queue}, poll_interval=0.005)
    backend.bind(queue.name, handler, concurrency=1, slots=pool)
    work = Work("crawl", {"value": "persisted"}, id="local-snapshot")
    slot_lease = pool.acquire()
    try:
        backend.submit_local(queue.name, work, slot_lease)
        backend.wait_idle(5)
        assert observed == [{"value": "persisted"}]
        record = queue.get(work.id)
        assert record is not None and record.work.inputs == observed[0]
    finally:
        backend.close()
        pool.close()


@pytest.mark.parametrize("mode", ["raise", "reject", "invalid", "exhaust"])
def test_backend_failure_is_observable(queue, mode):
    """处理器错误、非法结果及重投耗尽不能被报告为成功。

    Args:
        queue: 隔离队列。
        mode: 失败形式。
    """
    error = ValueError("broken")

    def handler(delivery: Delivery) -> Any:
        """产生指定失败。

        Args:
            delivery: 当前交付。

        Returns:
            交付决定或故意非法的值。

        Raises:
            ValueError: 测试的原始错误。
        """
        if mode == "raise":
            raise error
        if mode == "reject":
            return DeliveryResult.reject(error)
        if mode == "exhaust":
            return DeliveryResult.retry(error)
        return None

    backend = QueueBackend({queue.name: queue}, poll_interval=0.005)
    backend.bind(queue.name, handler, concurrency=1)
    backend.submit(queue.name, Work("crawl", id="broken"))
    try:
        with pytest.raises(TypeError if mode == "invalid" else ValueError) as caught:
            backend.wait_idle(5)
        if mode != "invalid":
            assert caught.value is error
            assert queue.get("broken").error == "ValueError: broken"
        assert queue.get("broken").status == "dead"
    finally:
        backend.close()


def test_runtime_graph_and_event_use_queue_backend(queue):
    """基础设施插件注入的队列后端可以经过真实队列执行并派生后续 Graph。

    Args:
        queue: 隔离队列。
    """
    received = []

    class Crawl(Node):
        """读取 JSON 种子并显式发出后续业务事件。"""

        inputs = {"seed": dict}  # 当前 JSON 种子。
        outputs = {}  # 业务事件显式发出，不返回终端数据。

        def execute(self, inputs, context):
            """记录身份、Slot 与独立执行配置。

            Args:
                inputs: 当前输入。
                context: 引擎提供的上下文。
            """
            received.append((inputs["seed"]["id"], context.slot, dict(context.options)))
            if inputs["seed"]["id"] == 1:
                context.emit("next", {"id": 2})

    backend = QueueBackend({queue.name: queue}, poll_interval=0.005)
    try:
        with Runtime(plugins=[LocalInfrastructurePlugin(tasks=backend)]) as runtime:
            runtime.register("crawl@v1", Graph(entrypoint="crawl").add(crawl=Crawl()))
            runtime.on("next", graph="crawl@v1", queue=queue.name, concurrency=2)
            backend.submit(
                queue.name,
                Work("crawl@v1", {"id": 1}, options={"crawler.session.reuse": False}),
            )
            runtime.wait_idle(5)
        assert [item[0] for item in received] == [1, 2]
        assert all(item[1] is not None for item in received)
        assert received[0][1] is received[1][1]
        assert received[0][2] == {"crawler.session.reuse": False}
        assert received[1][2] == {}
        assert queue.counts()["acked"] == 2
    finally:
        backend.close()


def _redis_process(url: str, name: str, prefix: str, barrier: Any) -> None:
    """在独立进程创建完整 Runtime 并竞争消费同一 Redis 队列。

    Args:
        url: 临时 Redis 地址。
        name: 隔离队列名称。
        prefix: 本测试的键前缀。
        barrier: 两个 Worker 同时开始消费的进程屏障。
    """
    from redis import Redis

    client = Redis.from_url(url, socket_timeout=2, socket_connect_timeout=2)
    queue = RedisTaskQueue(client, name, prefix=prefix)
    backend = QueueBackend({name: queue}, poll_interval=0.005)

    class Collect(Node):
        """把执行身份写入本测试拥有的 Redis 列表。"""

        inputs = {"seed": dict}  # 当前种子数据。
        outputs = {}  # 数据直接写入测试收集器。

        def execute(self, inputs, context):
            """记录当前进程执行的种子。

            Args:
                inputs: 当前 JSON 种子。
                context: 本地执行上下文。
            """
            assert context.slot is not None
            client.rpush(
                f"{prefix}:results", json.dumps([os.getpid(), inputs["seed"]["id"]])
            )
            time.sleep(0.03)

    try:
        with Runtime(plugins=[LocalInfrastructurePlugin(tasks=backend)]) as runtime:
            runtime.register(
                "crawl", Graph(entrypoint="collect").add(collect=Collect())
            )
            barrier.wait(10)
            runtime.consume(name, concurrency=1)
            runtime.wait_idle(10)
    finally:
        backend.close()
        client.close()


def test_real_redis_two_processes_compete_and_client_restart_preserves_tasks():
    """两个独立 Runtime 竞争消费，重连保留任务、交付结果与去重身份。"""
    url = os.environ.get("BRICKS_TEST_REDIS_URL")
    if not url:
        pytest.skip("set BRICKS_TEST_REDIS_URL for multiprocessing contracts")
    from redis import Redis

    name = uuid4().hex
    prefix = f"bricks-test:{name}"
    client = Redis.from_url(url, socket_timeout=2, socket_connect_timeout=2)
    queue = RedisTaskQueue(client, name, prefix=prefix)
    for index in range(12):
        queue.put(Work("crawl", {"id": index}, id=str(index)))
    client.close()
    process_context = multiprocessing.get_context("spawn")
    barrier = process_context.Barrier(2)
    processes = [
        process_context.Process(
            target=_redis_process, args=(url, name, prefix, barrier)
        )
        for _ in range(2)
    ]
    try:
        for process in processes:
            process.start()
        for process in processes:
            process.join(15)
            assert process.exitcode == 0
        results = [
            json.loads(value) for value in client.lrange(f"{prefix}:results", 0, -1)
        ]
        assert sorted(row[1] for row in results) == list(range(12))
        assert len({row[0] for row in results}) == 2
        assert queue.counts() == {"pending": 0, "leased": 0, "acked": 12, "dead": 0}
        assert not queue.put(Work("crawl", {"id": 0}, id="0"))
    finally:
        for process in processes:
            if process.is_alive():
                process.terminate()
                process.join(5)
        keys = list(client.scan_iter(match=f"{prefix}:*"))
        if keys:
            client.delete(*keys)
        client.close()


def test_queue_default_import_does_not_load_optional_redis():
    """公共默认导入路径不加载 Redis 或额外 HTTP 服务依赖。"""
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys; import bricks.queues; assert 'redis' not in sys.modules; assert 'fastapi' not in sys.modules",
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr


def test_scope_holds_existing_lease_and_defers_ack(queue, renewals):
    """已有租约在 with 内持续续租，显式确认直到退出才写入。

    Args:
        queue: 隔离队列。
        renewals: 续租观察器工厂。
    """
    watch = renewals(queue)
    queue.put(make_work("crawl", {"page": 1}, id="scoped"))
    lease = queue.claim("worker", lease_seconds=0.6)
    with queue.hold(lease, lease_seconds=0.6) as task:
        expiry = time.monotonic() + 0.6
        assert task.work.id == "scoped"
        work = task.work
        work.inputs["local_note"] = "kept during renewal"
        task.ack(result="success")
        watch.wait_past(expiry)
        task.check()
        assert task.work is work
        assert task.work.inputs["local_note"] == "kept during renewal"
        assert "local_note" not in queue.get("scoped").work.inputs
        assert queue.get("scoped").status == "leased"
        assert queue.claim("competitor") is None
    assert queue.get("scoped").status == "acked"
    assert queue.get("scoped").result == "success"
    with pytest.raises(RuntimeError):
        task.check()


def test_scopes_share_one_lazy_thread_and_stop_after_last_exit(queue, renewals):
    """多个同时占用共享一个线程；空队列不启动，最后退出后线程停止。

    Args:
        queue: 隔离队列。
        renewals: 续租观察器工厂。
    """
    watch = renewals(queue)
    before = set(threading.enumerate())
    with queue.reserve("empty") as empty:
        assert empty is None
    assert set(threading.enumerate()) == before
    for index in range(20):
        queue.put(make_work("crawl", {"page": index}, id=f"scoped:{index}"))
    with ExitStack() as stack:
        for _ in range(20):
            task = stack.enter_context(queue.reserve("worker", lease_seconds=0.6))
            assert task is not None
            task.ack()
        expiry = time.monotonic() + 0.6
        added = set(threading.enumerate()) - before
        assert len(added) == 1
        assert next(iter(added)).name == "bricks-queue-renewals"
        watch.wait_past(expiry, count=20)
        assert queue.claim("competitor") is None
        assert queue.counts()["leased"] == 20
    assert not next(iter(added)).is_alive()
    assert queue.counts()["acked"] == 20


def test_unacknowledged_scope_releases_and_respects_attempt_limit(queue):
    """未确认退出立即请求重投，仍受最大交付次数约束。

    Args:
        queue: 隔离队列。
    """
    queue.put(make_work("crawl", id="released"))
    context = queue.reserve("worker")
    assert queue.get("released").attempt == 0
    with context as task:
        assert task is not None
    assert queue.get("released").status == "pending"
    with pytest.raises(RuntimeError):
        with context:
            pass
    for _ in range(2):
        with queue.reserve("another-worker") as task:
            assert task.work.id == "released"
    assert queue.get("released").status == "dead"
    assert queue.get("released").attempt == 3


@pytest.mark.parametrize("error", [ValueError("business failed"), KeyboardInterrupt()])
def test_scope_exception_cancels_ack_and_propagates_original(queue, error):
    """业务错误与控制异常取消预定确认，释放占用并原样传播。

    Args:
        queue: 隔离队列。
        error: 块内抛出的原异常。
    """
    queue.put(make_work("crawl", id="exception"))
    with pytest.raises(type(error)) as caught:
        with queue.reserve("worker") as task:
            task.ack()
            raise error
    assert caught.value is error
    assert queue.get("exception").status == "pending"
    lease = queue.claim("replacement")
    assert lease.attempt == 2
    queue.ack(lease)


def test_scope_explicit_retry_reject_and_duplicate_decisions(queue):
    """退出决定明确区分延迟重投、拒绝和重复决定。

    Args:
        queue: 隔离队列。
    """
    queue.put(make_work("crawl", id="decisions"))
    with queue.reserve("worker") as task:
        task.retry(delay=0.2, error="later")
        with pytest.raises(RuntimeError):
            task.ack()
        assert queue.get("decisions").status == "leased"
    assert queue.claim("early") is None
    time.sleep(0.3)
    with queue.reserve("worker") as task:
        task.reject(error="invalid seed")
    assert queue.get("decisions").status == "dead"
    assert queue.get("decisions").error == "invalid seed"


def test_duplicate_scope_does_not_release_original_ownership(queue):
    """同一凭据不能被两个上下文管理，失败进入不释放原有上下文。

    Args:
        queue: 隔离队列。
    """
    queue.put(make_work("crawl", id="exclusive"))
    lease = queue.claim("worker")
    with queue.hold(lease) as first:
        with pytest.raises(RuntimeError, match="already has"):
            with queue.hold(lease):
                pass
        assert queue.get("exclusive").status == "leased"
        assert queue.claim("competitor") is None
        first.ack()
    assert queue.get("exclusive").status == "acked"


def test_scope_ownership_is_retained_until_exit_write_completes(queue, monkeypatch):
    """退出确认尚在执行时，同一 token 仍不能被其他上下文接管。

    Args:
        queue: 隔离队列。
        monkeypatch: 临时延迟确认的测试工具。
    """
    queue.put(make_work("crawl", id="settling"))
    lease = queue.claim("worker")
    entered, release = ThreadEvent(), ThreadEvent()
    original_ack = queue.ack

    def delayed_ack(current, *, result=None):
        """等待测试允许存储确认结束。

        Args:
            current: 待确认的最新凭据。
            result: 业务结果。
        """
        entered.set()
        assert release.wait(5)
        original_ack(current, result=result)

    monkeypatch.setattr(queue, "ack", delayed_ack)
    scope = queue.hold(lease)
    scope.__enter__()
    scope.ack()
    with ThreadPoolExecutor(max_workers=1) as executor:
        closing = executor.submit(scope.__exit__, None, None, None)
        try:
            assert entered.wait(2)
            with pytest.raises(RuntimeError, match="already has"):
                with queue.hold(lease):
                    pass
        finally:
            release.set()
        closing.result(5)
    assert queue.get("settling").status == "acked"


def test_scope_cannot_revive_expired_lease(queue):
    """旧凭据进入失败，不能影响重新领取后的交付。

    Args:
        queue: 隔离队列。
    """
    queue.put(make_work("crawl", id="stale-scope"))
    old = queue.claim("old", lease_seconds=0.2)
    time.sleep(0.3)
    new = queue.claim("new")
    with pytest.raises(LeaseLostError):
        with queue.hold(old):
            pass
    queue.ack(new)
    assert queue.get("stale-scope").status == "acked"


def test_shared_scope_renewal_failure_does_not_stop_other_tasks(renewals):
    """单项续租失败仍允许其他任务持续续租，并在退出时传播失败。

    Args:
        renewals: 续租观察器工厂。
    """
    failure = ConnectionError("renewal unavailable")
    failed = ThreadEvent()

    class PartialFailureQueue(MemoryTaskQueue):
        """只让指定任务的心跳失败。"""

        def renew(self, lease, *, lease_seconds=30):
            """对其他任务正常续租。

            Args:
                lease: 当前租约。
                lease_seconds: 续租秒数。

            Returns:
                正常任务的新租约。

            Raises:
                ConnectionError: 指定任务的模拟续租错误。
            """
            if lease.work.id == "bad":
                failed.set()
                raise failure
            return super().renew(lease, lease_seconds=lease_seconds)

    queue = PartialFailureQueue("partial-renewal")
    watch = renewals(queue)
    queue.put(make_work("crawl", id="bad"))
    queue.put(make_work("crawl", id="good"))
    with pytest.raises(ConnectionError) as caught:
        with queue.reserve("worker", lease_seconds=0.6) as bad:
            with queue.reserve("worker", lease_seconds=0.6) as good:
                expiry = time.monotonic() + 0.6
                assert bad.work.id == "bad" and good.work.id == "good"
                bad.ack()
                assert failed.wait(2)
                # 暂时存储错误在本地租约到期前继续重试，到期后才登记为失败。
                _eventually(lambda: _raises(bad.check, ConnectionError))
                watch.wait_past(expiry)
                good.check()
                good.ack()
    assert caught.value is failure
    assert queue.get("good").status == "acked"
    recovered = queue.claim("replacement")
    assert recovered.work.id == "bad" and recovered.attempt == 2
    queue.ack(recovered)


def test_scope_cleanup_failure_preserves_body_error():
    """释放失败通过 cause 保留，原业务异常仍是调用方捕获的异常。"""
    cleanup = ConnectionError("release unavailable")
    body = ValueError("body failed")

    class ReleaseFailureQueue(MemoryTaskQueue):
        """模拟退出时的存储错误。"""

        def retry(self, lease, *, delay=0, error=""):
            """拒绝释放占用。

            Args:
                lease: 当前凭据。
                delay: 延迟秒数。
                error: 原因文本。

            Raises:
                ConnectionError: 模拟的写回失败。
            """
            raise cleanup

    queue = ReleaseFailureQueue("release-failure")
    queue.put(make_work("crawl", id="release-failure"))
    with pytest.raises(ValueError) as caught:
        with queue.reserve("worker"):
            raise body
    assert caught.value is body
    assert caught.value.__cause__ is cleanup
    assert queue.get("release-failure").status == "leased"


def test_scope_cleanup_failure_keeps_existing_body_cause():
    """块内异常已有 cause 时，释放失败不覆盖真实根因。"""
    root = KeyError("root")

    class ReleaseFailureQueue(MemoryTaskQueue):
        """模拟退出时的存储错误。"""

        def retry(self, lease, *, delay=0, error=""):
            """拒绝释放占用。

            Args:
                lease: 当前凭据。
                delay: 延迟秒数。
                error: 原因文本。

            Raises:
                ConnectionError: 模拟的写回失败。
            """
            raise ConnectionError("release unavailable")

    queue = ReleaseFailureQueue("release-cause")
    queue.put(make_work("crawl", id="release-cause"))
    with pytest.raises(ValueError) as caught:
        with queue.reserve("worker"):
            raise ValueError("body failed") from root
    assert caught.value.__cause__ is root


def test_scope_transient_renewal_error_retries_until_local_expiry(renewals):
    """单次暂时存储错误不终止续租，退出仍写入已登记的确认。

    Args:
        renewals: 续租观察器工厂。
    """
    failed = ThreadEvent()

    class FlakyQueue(MemoryTaskQueue):
        """第一次续租模拟连接抖动，之后恢复。"""

        def renew(self, lease, *, lease_seconds=30):
            """首次抛出连接错误，其余正常续租。

            Args:
                lease: 当前租约。
                lease_seconds: 续租秒数。

            Returns:
                新租约。

            Raises:
                ConnectionError: 首次模拟的续租错误。
            """
            if not failed.is_set():
                failed.set()
                raise ConnectionError("renewal flapped")
            return super().renew(lease, lease_seconds=lease_seconds)

    queue = FlakyQueue("flaky-renewal")
    watch = renewals(queue)
    queue.put(make_work("crawl", id="flaky"))
    with queue.reserve("worker", lease_seconds=0.6) as scope:
        expiry = time.monotonic() + 0.6
        scope.ack()
        assert failed.wait(2)
        watch.wait_past(expiry)
        scope.check()
    assert queue.get("flaky").status == "acked"


def test_scope_check_failure_propagates_without_self_cause(queue, monkeypatch):
    """块内 check 抛出的后台错误在退出时保持原对象，不形成自身 cause。

    Args:
        queue: 隔离队列。
        monkeypatch: 替换续租操作的工具。
    """
    failure = ConnectionError("heartbeat failed")
    observed = ThreadEvent()

    def fail_renew(lease, *, lease_seconds=30):
        """记录心跳并抛出同一个模拟错误。

        Args:
            lease: 当前凭据。
            lease_seconds: 有效秒数。

        Raises:
            ConnectionError: 模拟的续租错误。
        """
        observed.set()
        raise failure

    queue.put(make_work("crawl", id="check-error"))
    monkeypatch.setattr(queue, "renew", fail_renew)
    with pytest.raises(ConnectionError) as caught:
        with queue.reserve("worker", lease_seconds=0.6) as task:
            assert observed.wait(2)
            # 暂时存储错误在本地租约到期后才登记，随后 check 传播原对象。
            _eventually(lambda: _raises(task.check, ConnectionError))
            task.check()
    assert caught.value is failure
    assert caught.value.__cause__ is not failure
    recovered = []
    _eventually(lambda: recovered.append(queue.claim("replacement")) or recovered[-1])
    assert recovered[-1].work.id == "check-error"
    assert recovered[-1].attempt == 2


def test_scope_thread_start_failure_rolls_back_and_can_restart(monkeypatch):
    """首次调度线程启动失败释放已领取任务，下一次进入可以重新启动。

    Args:
        monkeypatch: 临时替换线程启动方法的测试工具。
    """
    queue = MemoryTaskQueue("thread-start")
    queue.put(make_work("crawl", id="restart"))

    def fail_start(thread):
        """模拟线程资源耗尽。

        Args:
            thread: 尚未启动的线程。

        Raises:
            RuntimeError: 模拟启动失败。
        """
        raise RuntimeError("thread unavailable")

    with monkeypatch.context() as patch:
        patch.setattr("bricks.queues.scope.Thread.start", fail_start)
        with pytest.raises(RuntimeError, match="thread unavailable"):
            with queue.reserve("worker"):
                pass
    assert queue.get("restart").status == "pending"
    with queue.reserve("worker") as task:
        task.ack()
    assert queue.get("restart").status == "acked"


def test_queue_validation_does_not_change_existing_lease(queue):
    """非法时间和业务结果不会改变当前租约。

    Args:
        queue: 隔离队列。
    """
    queue.put(Work("crawl", id="validation"))
    lease = queue.claim("worker")
    for invalid in (0, -1, True, float("inf"), float("nan")):
        with pytest.raises(ValueError):
            queue.renew(lease, lease_seconds=invalid)
        with pytest.raises(ValueError):
            queue.acquire_init("producer", lease_seconds=invalid)
    with pytest.raises(ValueError):
        queue.ack(lease, result="unknown")
    with pytest.raises(TypeError):
        queue.reject(lease, error=ValueError("not text"))
    queue.ack(lease)
    assert queue.get("validation").status == "acked"


def test_simultaneous_close_calls_wait_for_execution(queue):
    """两个并发关闭调用都必须等到已领取任务完成。

    Args:
        queue: 隔离队列。
    """
    started, release = ThreadEvent(), ThreadEvent()

    def handler(delivery: Delivery) -> DeliveryResult:
        """等待测试结束当前交付。

        Args:
            delivery: 当前交付。

        Returns:
            ACK。
        """
        started.set()
        assert release.wait(5)
        return DeliveryResult.ack()

    backend = QueueBackend({queue.name: queue}, poll_interval=0.005)
    backend.bind(queue.name, handler, concurrency=1)
    backend.submit(queue.name, Work("crawl", id="closing"))
    try:
        assert started.wait(5)
        with ThreadPoolExecutor(max_workers=2) as executor:
            first = executor.submit(backend.close)
            second = executor.submit(backend.close)
            time.sleep(0.1)
            assert not first.done() and not second.done()
            release.set()
            first.result(5)
            second.result(5)
        assert queue.get("closing").status == "acked"
    finally:
        release.set()
        backend.close()


def test_binding_failure_rolls_back_and_allows_rebind(queue):
    """装配失败不遗留消费者，同一后端可重新绑定。

    Args:
        queue: 隔离队列。
    """
    backend = QueueBackend({queue.name: queue}, poll_interval=0.005)

    def handler(delivery: Delivery) -> DeliveryResult:
        """接受测试任务。

        Args:
            delivery: 当前交付。

        Returns:
            ACK。
        """
        return DeliveryResult.ack()

    with pytest.raises(ValueError):
        backend.bind(queue.name, handler, concurrency=0)
    with pytest.raises(TypeError):
        backend.bind(queue.name, handler, concurrency=1, slots=object())
    with pytest.raises(TypeError):
        backend.bind(queue.name, None, concurrency=1)
    with pytest.raises(ValueError):
        backend.submit("not-configured", Work("crawl"))
    first = backend.bind(queue.name, handler, concurrency=1)
    first()
    second = backend.bind(queue.name, handler, concurrency=1)
    backend.submit(queue.name, Work("crawl", id="rebound"))
    backend.wait_idle(5)
    second()
    backend.close()


def test_background_storage_failure_reaches_wait_and_close_cleans_threads():
    """后台领取失败会向调用方传播，关闭仍完成线程清理。"""

    class BrokenQueue(MemoryTaskQueue):
        """模拟 Redis 不可用的领取失败。"""

        def claim(self, owner, *, lease_seconds=30):
            """报告存储断开。

            Args:
                owner: Worker 身份。
                lease_seconds: 请求的租约秒数。

            Raises:
                ConnectionError: 模拟的连接错误。
            """
            raise ConnectionError("storage unavailable")

    queue = BrokenQueue("broken")
    queue.put(Work("crawl", id="waiting"))
    backend = QueueBackend({queue.name: queue}, lease_seconds=0.08, poll_interval=0.005)
    backend.bind(queue.name, lambda delivery: DeliveryResult.ack(), concurrency=1)
    try:
        with pytest.raises(ConnectionError, match="storage unavailable"):
            backend.wait_idle(5)
    finally:
        backend.close()


def _eventually(predicate, timeout=5):
    """在截止时间内轮询条件成立，不依赖紧贴租约的固定等待。

    Args:
        predicate: 无参条件函数。
        timeout: 最长等待秒数。
    """
    deadline = time.monotonic() + timeout
    while not predicate():
        assert time.monotonic() < deadline, "condition was not reached in time"
        time.sleep(0.005)


def _raises(call, error):
    """判断无参调用是否抛出指定异常。

    Args:
        call: 无参调用。
        error: 期望的异常类型。

    Returns:
        抛出该异常时为 True，正常返回时为 False。
    """
    try:
        call()
    except error:
        return True
    return False


def test_backend_renewal_outage_uses_local_clock_and_stops_binding():
    """存储时钟不是本机单调时钟时，持续续租失败仍在本地租约到期后传播并停止绑定。"""
    release = ThreadEvent()

    class EpochQueue(MemoryTaskQueue):
        """使用纪元时钟模拟 Redis 服务端时间，续租始终连接失败。"""

        def renew(self, lease, *, lease_seconds=30):
            """报告存储断开。

            Args:
                lease: 当前租约。
                lease_seconds: 续租秒数。

            Raises:
                ConnectionError: 模拟的连接错误。
            """
            raise ConnectionError("renewal unavailable")

    def handler(delivery: Delivery) -> DeliveryResult:
        """保持执行直到测试释放。

        Args:
            delivery: 本地交付。

        Returns:
            ACK。
        """
        assert release.wait(5)
        return DeliveryResult.ack()

    queue = EpochQueue("epoch-renewal", clock=time.time)
    queue.put(Work("crawl", id="outage"))
    backend = QueueBackend({queue.name: queue}, lease_seconds=0.3, poll_interval=0.01)
    backend.bind(queue.name, handler, concurrency=1)
    try:
        with pytest.raises(ConnectionError, match="renewal unavailable"):
            backend.wait_idle(5)
    finally:
        release.set()
        with pytest.raises(LeaseLostError):
            backend.close()
    assert queue.get("outage").attempt == 1


def test_backend_defers_reclaimed_lease_until_local_delivery_finishes():
    """租约过期后本绑定重新领到仍在执行的任务时，不并发执行也不空耗交付次数。"""
    now = [100.0]
    queue = MemoryTaskQueue("reclaimed", clock=lambda: now[0])
    queue.put(Work("crawl", id="slow"))
    started, release = ThreadEvent(), ThreadEvent()
    attempts = []

    def handler(delivery: Delivery) -> DeliveryResult:
        """首次交付阻塞直到释放，之后立即确认。

        Args:
            delivery: 本地交付。

        Returns:
            ACK。
        """
        attempts.append(delivery.attempt)
        if delivery.attempt == 1:
            started.set()
            assert release.wait(5)
        return DeliveryResult.ack()

    backend = QueueBackend({queue.name: queue}, lease_seconds=0.3, poll_interval=0.01)
    backend.bind(queue.name, handler, concurrency=2)
    try:
        assert started.wait(5)
        now[0] += 60
        _eventually(lambda: queue.get("slow").attempt == 2)
        # 新租约保持续租，旧交付结束前不会被重投耗尽。
        record = queue.get("slow")
        assert (record.status, record.attempt) == ("leased", 2)
        assert attempts == [1]
        release.set()
        _eventually(lambda: queue.get("slow").status == "acked")
        assert attempts == [1, 2]
        with pytest.raises(LeaseLostError):
            backend.wait_idle(5)
    finally:
        release.set()
        backend.close()


def test_backend_renewal_racing_ack_is_not_reported_lost():
    """续租往返期间交付已确认时，续租返回的失效不能报告为失败。"""
    renewing, acked = ThreadEvent(), ThreadEvent()

    class SlowRenewQueue(MemoryTaskQueue):
        """首次续租等待确认完成后才访问存储。"""

        def renew(self, lease, *, lease_seconds=30):
            """模拟网络往返中交付被确认。

            Args:
                lease: 当前租约。
                lease_seconds: 续租秒数。

            Returns:
                新租约。
            """
            if not renewing.is_set():
                renewing.set()
                assert acked.wait(5)
            return super().renew(lease, lease_seconds=lease_seconds)

        def ack(self, lease, *, result=None):
            """确认后放行等待中的续租。

            Args:
                lease: 当前租约。
                result: 业务结果。
            """
            super().ack(lease, result=result)
            acked.set()

    def handler(delivery: Delivery) -> DeliveryResult:
        """等到续租已发出再确认。

        Args:
            delivery: 本地交付。

        Returns:
            ACK。
        """
        assert renewing.wait(5)
        return DeliveryResult.ack()

    queue = SlowRenewQueue("renew-race")
    queue.put(Work("crawl", id="race"))
    backend = QueueBackend({queue.name: queue}, lease_seconds=0.3, poll_interval=0.01)
    backend.bind(queue.name, handler, concurrency=1)
    try:
        backend.wait_idle(5)
        assert queue.get("race").status == "acked"
    finally:
        acked.set()
        backend.close()


@pytest.mark.parametrize("stop", ["unbind", "close"])
def test_backend_accepts_local_continuation_while_draining(queue, stop):
    """注销或关闭排空期间，在途交付发起的本地延续仍被持久化并执行。

    Args:
        queue: 隔离的内存或 Redis 任务存储。
        stop: 停止领取的方式。
    """
    pool = SlotPool(2)
    started, proceed = ThreadEvent(), ThreadEvent()
    backend = QueueBackend({queue.name: queue}, poll_interval=0.005)

    def handler(delivery: Delivery) -> DeliveryResult:
        """父交付在停止领取后提交本地延续。

        Args:
            delivery: 本地交付。

        Returns:
            ACK。
        """
        if delivery.work.id == "parent":
            started.set()
            assert proceed.wait(5)
            backend.submit_local(
                queue.name, Work("crawl", {"child": 1}, id="child"), pool.acquire()
            )
        return DeliveryResult.ack()

    unbind = backend.bind(queue.name, handler, concurrency=1, slots=pool)
    # 绑定对象只用于观察停止领取的时刻。
    binding = backend._bindings[-1]
    backend.submit(queue.name, Work("crawl", id="parent"))
    try:
        assert started.wait(5)
        with ThreadPoolExecutor(max_workers=1) as executor:
            closing = executor.submit(unbind if stop == "unbind" else backend.close)
            _eventually(binding.stop.is_set)
            proceed.set()
            closing.result(5)
        assert queue.get("parent").status == "acked"
        assert queue.get("child").status == "acked"
        assert pool.available == 2
    finally:
        proceed.set()
        backend.close()
        pool.close()


def test_backend_failure_aggregation_keeps_existing_causes(queue):
    """多个交付失败聚合传播时不覆盖各自已有的根因。

    Args:
        queue: 隔离的内存或 Redis 任务存储。
    """

    def handler(delivery: Delivery) -> DeliveryResult:
        """抛出带根因的业务错误。

        Args:
            delivery: 本地交付。

        Raises:
            ValueError: 以 KeyError 为 cause 的业务错误。
        """
        raise ValueError(delivery.work.id) from KeyError(delivery.work.id)

    backend = QueueBackend({queue.name: queue}, poll_interval=0.005)
    backend.bind(queue.name, handler, concurrency=2)
    backend.submit(queue.name, Work("crawl", id="first"))
    backend.submit(queue.name, Work("crawl", id="second"))
    try:
        _eventually(lambda: queue.counts()["dead"] == 2)
        with pytest.raises(ValueError) as caught:
            backend.wait_idle(5)
        assert isinstance(caught.value.__cause__, KeyError)
        assert caught.value.__cause__.args == caught.value.args
    finally:
        backend.close()


def _claim_then_exit(url: str, name: str, prefix: str, connection: Any) -> None:
    """领取后直接退出进程，模拟未确认的 Worker 崩溃。

    Args:
        url: 测试 Redis 地址。
        name: 测试队列名称。
        prefix: 隔离键前缀。
        connection: 向父进程发送交付凭据的管道。
    """
    from redis import Redis

    client = Redis.from_url(url, socket_timeout=2, socket_connect_timeout=2)
    queue = RedisTaskQueue(client, name, prefix=prefix)
    lease = queue.claim("crashing-worker", lease_seconds=0.3)
    connection.send((lease.token, lease.owner, lease.attempt, lease.deadline))
    connection.close()
    os._exit(0)


def test_real_redis_recovers_after_worker_process_exit():
    """已领取任务在 Worker 进程退出后恢复，旧 token 无法确认新交付。"""
    url = os.environ.get("BRICKS_TEST_REDIS_URL")
    if not url:
        pytest.skip("set BRICKS_TEST_REDIS_URL for process recovery")
    from redis import Redis

    from bricks.queues import TaskLease

    name = uuid4().hex
    prefix = f"bricks-test:{name}"
    client = Redis.from_url(url, socket_timeout=2, socket_connect_timeout=2)
    queue = RedisTaskQueue(client, name, prefix=prefix)
    work = Work("crawl", {"id": 1}, id="recover")
    queue.put(work)
    process_context = multiprocessing.get_context("spawn")
    parent, child = process_context.Pipe(duplex=False)
    process = process_context.Process(
        target=_claim_then_exit, args=(url, name, prefix, child)
    )
    try:
        process.start()
        assert parent.poll(10)
        token, owner, attempt, deadline = parent.recv()
        process.join(5)
        assert process.exitcode == 0
        time.sleep(0.4)
        recovered = queue.claim("replacement")
        assert recovered.attempt == 2 and recovered.work == work
        old = TaskLease(name, work, token, owner, attempt, deadline)
        with pytest.raises(LeaseLostError):
            queue.ack(old)
        queue.ack(recovered, result="success")
        assert queue.get("recover").result == "success"
    finally:
        if process.is_alive():
            process.terminate()
            process.join(5)
        parent.close()
        child.close()
        keys = list(client.scan_iter(match=f"{prefix}:*"))
        if keys:
            client.delete(*keys)
        client.close()
