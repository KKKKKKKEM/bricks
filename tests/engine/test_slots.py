"""面向适配器作者的公共 Slot 资源能力契约测试。"""

import pickle
from typing import Any, cast
from threading import Event as ThreadEvent, Thread

import pytest

import bricks
from bricks import SlotPool
from bricks.spi import Delivery, SlotLease, Work


def test_slot_lease_is_an_extension_protocol_only() -> None:
    """验证 Slot lease 仅作为扩展协议暴露。"""

    assert not hasattr(bricks, "SlotLease")
    pool = SlotPool(1)
    lease = pool.acquire(0)
    assert isinstance(lease, SlotLease)
    assert Delivery(Work("graph"), slot_lease=lease).slot_lease is lease
    lease.release()
    pool.close()
    with pytest.raises(TypeError, match="SlotLease"):
        Delivery(Work("graph"), slot_lease=cast(Any, object()))  # 验证非法 lease。


def test_pool_acquisition_reference_ownership_and_exhaustion() -> None:
    """验证资源池申请、引用归属与耗尽行为。"""

    pool = SlotPool(1)
    lease = pool.try_acquire()
    assert lease is not None
    slot = lease.slot
    assert pool.try_acquire() is None
    with pytest.raises(TimeoutError):
        pool.acquire(0)
    lease.retain()
    lease.release()
    assert pool.available == 0
    lease.release()
    assert pool.available == 1
    for operation in (lease.retain, lease.release, lambda: lease.slot):
        with pytest.raises(RuntimeError, match="released"):
            operation()
    with pytest.raises(RuntimeError, match="released"):
        with lease.execution():
            pass
    next_lease = pool.acquire(0)
    assert next_lease.slot is slot
    next_lease.release()
    pool.close()


def test_slot_resources_cannot_cross_serialization_boundary() -> None:
    """验证本地 Slot 资源不可跨序列化边界。"""

    pool = SlotPool(1)
    lease = pool.acquire()
    try:
        for value in (
            pool,
            lease,
            lease.slot,
            Delivery(Work("graph"), slot_lease=lease),
        ):
            with pytest.raises(TypeError):
                pickle.dumps(value)
        work = Work("graph", {"input": 1})
        assert pickle.loads(pickle.dumps(work)) == work
    finally:
        lease.release()
        pool.close()


def test_execution_keeps_slot_alive_until_exception_unwinds() -> None:
    """验证异常展开完成前执行仍持有 Slot。

    Raises:
        ValueError: 参数值或字段组合不合法。
    """

    pool = SlotPool(1)
    lease = pool.acquire()
    slot = lease.slot
    with pytest.raises(ValueError, match="business failure"):
        with lease.execution() as active:
            assert active is slot
            lease.release()
            assert pool.available == 0
            raise ValueError("business failure")
    assert pool.available == 1
    pool.close()


def test_lease_serializes_executions_across_threads() -> None:
    """验证 lease 保证跨线程的同槽执行串行化。"""

    pool = SlotPool(1)
    lease = pool.acquire()
    attempted, entered = ThreadEvent(), ThreadEvent()

    def branch():
        """在独立分支持有 lease，并验证同槽执行串行化。"""

        attempted.set()
        with lease.execution():
            entered.set()

    with lease.execution():
        thread = Thread(target=branch)
        thread.start()
        assert attempted.wait(1)
        assert not entered.wait(0.05)
    thread.join(1)
    assert not thread.is_alive()
    assert entered.is_set()
    lease.release()
    assert pool.available == 1
    pool.close()


def test_release_notifies_all_subscribers_despite_failure_and_allows_detach(
    caplog,
) -> None:
    """验证资源归还通知不被单个订阅者失败中断且支持卸载。

    Args:
        caplog: 当前用例使用的 caplog 夹具或参数化输入。
    """

    pool = SlotPool(1)
    notifications = []

    def broken():
        """抛出测试指定的异常以验证失败传播与清理。

        Raises:
            ValueError: 参数值或字段组合不合法。
        """

        raise ValueError("listener failed")

    detach_broken = pool.subscribe_available(broken)
    detach = pool.subscribe_available(lambda: notifications.append(pool.available))
    pool.acquire().release()
    assert notifications == [1]
    assert "listener failed" in caplog.text
    detach()
    detach()
    detach_broken()
    pool.acquire().release()
    assert notifications == [1]
    pool.close()


def test_pool_close_wakes_waiters_and_allows_outstanding_references_to_return() -> None:
    """验证池关闭唤醒等待方并允许在途引用归还。"""

    pool = SlotPool(1)
    lease = pool.acquire()
    waiting, rejected = ThreadEvent(), ThreadEvent()

    def waiter():
        """在独立等待方申请资源并记录唤醒结果。"""

        waiting.set()
        try:
            pool.acquire()
        except RuntimeError:
            rejected.set()

    thread = Thread(target=waiter)
    thread.start()
    assert waiting.wait(1)
    pool.close()
    thread.join(1)
    assert not thread.is_alive()
    assert rejected.is_set()
    for operation in (
        pool.acquire,
        pool.try_acquire,
        lambda: pool.subscribe_available(lambda: None),
    ):
        with pytest.raises(RuntimeError, match="closed"):
            operation()
    lease.release()
    assert pool.available == 1
