"""可跨 Work 传递并长期复用状态的逻辑执行槽。"""

from __future__ import annotations

import logging
import time
from collections import deque
from collections.abc import Callable, Iterable, Iterator, Mapping, MutableMapping
from contextlib import AbstractContextManager, contextmanager
from threading import Condition, RLock
from typing import Any, Protocol, runtime_checkable
from uuid import uuid4

from .core import _validate_timeout, require_non_empty_string

_LOGGER = logging.getLogger(__name__)


@runtime_checkable
class SlotLease(Protocol):
    """适配器持有的进程内 Slot 能力，由 SlotPool 分配并从 bricks.spi 导出。"""

    @property
    def slot(self) -> Slot:
        """返回当前执行链使用的 Slot；不转移引用所有权。

        Returns:
            当前逻辑链使用的本地 Slot。
        """
        ...

    def retain(self) -> None:
        """为分支或后续投递增加一个引用，必须匹配一次 release。"""

    def release(self) -> None:
        """释放当前引用；最后一个引用归还 Slot，归还后不可再次使用。"""

    def execution(self) -> AbstractContextManager[Slot]:
        """串行占用 Slot 执行 Graph；退出前保留资源，不释放调用方引用。

        Returns:
            保护 lease 引用并保证同槽串行执行的上下文管理器。
        """
        ...


@runtime_checkable
class SlotProvider(Protocol):
    """Consumer 使用的本地 Slot 池能力，不要求继承默认 SlotPool。"""

    @property
    def size(self) -> int:
        """返回执行槽池的固定容量。

        Returns:
            资源池的固定容量。
        """
        ...

    @property
    def available(self) -> int:
        """返回当前可申请的执行槽数量。

        Returns:
            当前可供申请的空闲 Slot 数量。
        """
        ...

    def try_acquire(self) -> SlotLease | None:
        """尝试立即取得 Slot lease，无可用资源时返回 None。

        Returns:
            调用方负责释放的 Slot lease；没有空闲资源时返回 None。
        """
        ...

    def acquire(self, timeout: float | None = None) -> SlotLease:
        """等待可用的本地 Slot，并返回调用方负责释放的 lease。

        Args:
            timeout: 等待或执行时限，单位秒；None 表示不设置时限。

        Returns:
            调用方负责释放的 Slot lease。
        """
        ...

    def subscribe_available(self, listener: Callable[[], None]) -> Callable[[], None]:
        """注册资源可用回调并返回取消订阅函数。

        Args:
            listener: 资源可用时调用的通知函数。

        Returns:
            用于取消当前可用通知订阅的函数。
        """
        ...

    def close(self) -> None:
        """结束当前组件的生命周期并释放其拥有的资源。"""
        ...


class Slot(MutableMapping[str, Any]):
    """保存一条逻辑执行链可复用的状态，与线程和 Worker 无关。

    Attributes:
        id: 当前对象的唯一标识。
        _values: 当前容器拥有的数据映射。
        _execution_lock: 保证同一 Slot 的 Graph 执行串行化的锁。
    """

    def __init__(
        self,
        values: Mapping[str, Any] | None = None,
        *,
        id: str | None = None,
    ) -> None:
        """创建可沿逻辑执行链共享的本地资源映射与串行执行锁。

        Args:
            values: 当前操作处理的值集合或字段映射。
            id: 对象标识，允许缺省时由实现生成。
        """

        self.id = require_non_empty_string(
            str(uuid4()) if id is None else id,
            "slot id",
        )
        self._values: dict[str, Any] = {} if values is None else dict(values)
        self._execution_lock = RLock()

    def __getitem__(self, key: str) -> Any:
        """读取当前 Slot 中按名称保存的资源。

        Args:
            key: 关联记录或容器条目的键。

        Returns:
            指定名称或索引对应的容器条目。
        """

        return self._values[key]

    def __setitem__(self, key: str, value: Any) -> None:
        """按名称保存资源，供同一逻辑执行链继续使用。

        Args:
            key: 关联记录或容器条目的键。
            value: 当前操作处理的输入值。
        """

        self._values[key] = value

    def __delitem__(self, key: str) -> None:
        """移除当前 Slot 中指定名称的资源。

        Args:
            key: 关联记录或容器条目的键。
        """

        del self._values[key]

    def __iter__(self) -> Iterator[str]:
        """迭代当前 Slot 的资源名称。

        Returns:
            遍历当前对象内容的独立迭代入口。
        """

        return iter(self._values)

    def __len__(self) -> int:
        """返回当前容器中保存的条目数量。

        Returns:
            当前容器条目数量。
        """

        return len(self._values)

    def __repr__(self) -> str:
        """返回包含当前内容的调试表示。

        Returns:
            包含当前对象内容的调试字符串。
        """

        return f"Slot(id={self.id!r})"


class SlotPool:
    """管理一组同构 Slot；同一个池可以被多个 Consumer 共享。

    Attributes:
        _slots: 资源池拥有的全部 Slot。
        _available: 尚未租出的执行槽集合。
        _condition: 协调共享状态访问及同步等待的锁或条件变量。
        _listeners: 资源归还时触发的可用通知函数。
        _closed: 当前组件是否已停止接受新工作。
    """

    def __init__(
        self,
        size: int | None = None,
        *,
        slots: Iterable[Slot] | None = None,
        factory: Callable[[], Slot] | None = None,
    ) -> None:
        """创建固定容量的本地执行资源池。

        Args:
            size: 资源池容量或本次断言使用的预期字节数。
            slots: 提供本地执行槽的资源池能力。
            factory: 创建默认值或可替换资源的工厂。

        Raises:
            TypeError: 参数类型或接口实现不符合当前契约。
            ValueError: 参数值或字段组合不合法。
        """

        if slots is not None and size is not None:
            raise TypeError("size and slots are mutually exclusive")
        if slots is not None and factory is not None:
            raise TypeError("factory cannot be combined with slots")
        if factory is not None and not callable(factory):
            raise TypeError("slot factory must be callable")
        if slots is None:
            if type(size) is not int:
                raise TypeError("slot pool size must be an integer")
            if size < 1:
                raise ValueError("slot pool size must be at least 1")
            if factory is None:
                factory = Slot
            created = tuple(factory() for _ in range(size))
        else:
            created = tuple(slots)
            if not created:
                raise ValueError("slot pool must contain at least one Slot")
        if any(not isinstance(slot, Slot) for slot in created):
            raise TypeError("slot pool accepts only Slot instances")
        if len({id(slot) for slot in created}) != len(created):
            raise ValueError("slot pool must not contain duplicate Slot instances")
        if len({slot.id for slot in created}) != len(created):
            raise ValueError("slot pool must not contain duplicate Slot ids")

        self._slots = created
        self._available = deque(created)
        self._condition = Condition(RLock())
        self._listeners: list[Callable[[], None]] = []
        self._closed = False

    @property
    def size(self) -> int:
        """返回执行槽池的固定容量。

        Returns:
            资源池的固定容量。
        """

        return len(self._slots)

    @property
    def available(self) -> int:
        """返回当前可申请的执行槽数量。

        Returns:
            当前可供申请的空闲 Slot 数量。
        """

        with self._condition:
            return len(self._available)

    def close(self) -> None:
        """关闭资源池并唤醒等待方，允许在途引用稍后归还。"""

        with self._condition:
            if self._closed:
                return
            self._closed = True
            self._condition.notify_all()

    def try_acquire(self) -> SlotLease | None:
        """立即申请一个根执行链引用；池耗尽返回 None，关闭后抛错。

        Returns:
            调用方负责释放的 Slot lease；没有空闲资源时返回 None。

        Raises:
            RuntimeError: 当前生命周期状态或操作顺序不允许此操作。
        """

        with self._condition:
            if self._closed:
                raise RuntimeError("slot pool is closed")
            if not self._available:
                return None
            return _SlotLease(self, self._available.popleft())

    def subscribe_available(self, listener: Callable[[], None]) -> Callable[[], None]:
        """订阅归还通知并返回幂等取消函数；通知不预留 Slot。

        Args:
            listener: 资源可用时调用的通知函数。

        Returns:
            用于取消当前可用通知订阅的函数。

        Raises:
            RuntimeError: 当前生命周期状态或操作顺序不允许此操作。
            TypeError: 参数类型或接口实现不符合当前契约。
        """

        if not callable(listener):
            raise TypeError("slot pool listener must be callable")
        with self._condition:
            if self._closed:
                raise RuntimeError("slot pool is closed")
            self._listeners.append(listener)

        def unsubscribe() -> None:
            """移除当前资源可用回调，重复取消不会产生额外作用。"""

            with self._condition:
                if listener in self._listeners:
                    self._listeners.remove(listener)

        return unsubscribe

    def acquire(self, timeout: float | None = None) -> SlotLease:
        """等待一个根执行链引用；不得在 Consumer 执行线程内等待。

        Args:
            timeout: 等待或执行时限，单位秒；None 表示不设置时限。

        Returns:
            调用方负责释放的 Slot lease。

        Raises:
            RuntimeError: 当前生命周期状态或操作顺序不允许此操作。
            TimeoutError: 等待未在指定时限内完成。
        """

        _validate_timeout(timeout)
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._condition:
            while not self._available:
                if self._closed:
                    raise RuntimeError("slot pool is closed")
                remaining = None if deadline is None else deadline - time.monotonic()
                if remaining is not None and remaining <= 0:
                    raise TimeoutError("slot pool did not provide a Slot")
                self._condition.wait(remaining)
            if self._closed:
                raise RuntimeError("slot pool is closed")
            return _SlotLease(self, self._available.popleft())

    def _release(self, slot: Slot) -> None:
        """归还池中的执行槽，并通知等待资源的调用方。

        Args:
            slot: 当前逻辑执行链使用的本地执行槽。

        Raises:
            RuntimeError: 当前生命周期状态或操作顺序不允许此操作。
            ValueError: 参数值或字段组合不合法。
        """

        with self._condition:
            if all(slot is not item for item in self._slots):
                raise ValueError("Slot does not belong to this pool")
            if any(slot is item for item in self._available):
                raise RuntimeError("Slot is already available")
            self._available.append(slot)
            self._condition.notify()
            listeners = tuple(self._listeners)
        for listener in listeners:
            try:
                listener()
            except Exception:
                _LOGGER.exception("slot availability listener failed")


class _SlotLease:
    """对执行链所持 Slot 做内部引用计数，最后一个 Work 结束时归还。

    Attributes:
        __slots__: 实例允许保存的字段名称，限制动态增加属性。
        _slot: 当前逻辑执行链的本地执行资源。
        _pool: lease 归还资源时对应的 SlotPool。
        _references: 当前 lease 尚未释放的引用数量。
        _lock: 保护当前组件共享状态的进程内互斥锁。
    """

    __slots__ = ("_lock", "_pool", "_references", "_slot")

    def __init__(self, pool: SlotPool, slot: Slot) -> None:
        """为指定池中的 Slot 创建拥有一个引用的本地 lease。

        Args:
            pool: 负责管理 Slot 生命周期的本地资源池。
            slot: 当前逻辑执行链使用的本地执行槽。
        """

        self._slot = slot
        self._pool = pool
        self._references = 1
        self._lock = RLock()

    @property
    def slot(self) -> Slot:
        """返回当前 lease 关联的本地执行槽。

        Returns:
            当前逻辑链使用的本地 Slot。

        Raises:
            RuntimeError: 当前生命周期状态或操作顺序不允许此操作。
        """

        with self._lock:
            if self._references == 0:
                raise RuntimeError("slot lease is already released")
            return self._slot

    @contextmanager
    def execution(self) -> Iterator[Slot]:
        """在 lease 保护下串行使用同一个 Slot 的执行资源。

        Yields:
            当前 lease 的 Slot；作用域内持有引用并串行访问其执行资源。
        """

        self.retain()
        try:
            with self._slot._execution_lock:
                yield self._slot
        finally:
            self.release()

    def retain(self) -> None:
        """为同一逻辑链新增一个必须由接管方释放的引用。

        Raises:
            RuntimeError: 当前生命周期状态或操作顺序不允许此操作。
        """

        with self._lock:
            if self._references == 0:
                raise RuntimeError("slot lease is already released")
            self._references += 1

    def release(self) -> None:
        """释放当前持有的 lease 引用，最后一个引用结束时归还 Slot。

        Raises:
            RuntimeError: 当前生命周期状态或操作顺序不允许此操作。
        """

        slot: Slot | None = None
        with self._lock:
            if self._references == 0:
                raise RuntimeError("slot lease is already released")
            self._references -= 1
            if self._references == 0:
                slot = self._slot
        if slot is not None:
            self._pool._release(slot)
