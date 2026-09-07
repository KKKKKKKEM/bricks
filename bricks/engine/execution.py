"""单次 Graph execution 的限制、状态和协作式控制。"""

from __future__ import annotations

import asyncio
import enum
import itertools
import math
import time
from collections.abc import AsyncIterator, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from threading import RLock
from uuid import uuid4

from .core import Output, _validate_timeout, require_non_empty_string
from .errors import (
    ExecutionCancelledError,
    ExecutionTimeoutError,
    NodeTimeoutError,
    StepLimitExceededError,
)
from .execution_resources import (
    ExecutionNotifier,
    LocalExecutionNotifier,
    MemoryOutputStore,
    OutputStore,
)
from .graph import Graph


def _validate_duration(value: float | None, label: str) -> None:
    """校验正数秒数或表示不限时的 None。

    Args:
        value: 待校验的超时秒数，None 表示不限制。
        label: 校验失败时用于指明字段的说明名称。

    Raises:
        TypeError: 参数类型或接口实现不符合当前契约。
        ValueError: 参数值或字段组合不合法。
    """

    if value is None:
        return
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{label} must be a number or None")
    if not math.isfinite(value) or value <= 0:
        raise ValueError(f"{label} must be a finite number greater than zero")


@dataclass(frozen=True, slots=True)
class ExecutionLimits:
    """控制一次 Graph execution；零步数和空 timeout 表示无限制。

    Attributes:
        max_steps: 节点触发次数上限，0 表示不限制。
        timeout: 超时秒数，None 表示不限制。
    """

    max_steps: int = 0
    timeout: float | None = None

    def __post_init__(self) -> None:
        """校验构造字段并固定需要保持不变的数据。

        Raises:
            TypeError: 参数类型或接口实现不符合当前契约。
            ValueError: 参数值或字段组合不合法。
        """

        if type(self.max_steps) is not int:
            raise TypeError("max_steps must be an integer")
        if self.max_steps < 0:
            raise ValueError("max_steps must not be negative")
        _validate_duration(self.timeout, "timeout")


class ExecutionStatus(str, enum.Enum):
    """一项 Execution 对调用方可见的生命周期状态。

    Attributes:
        PENDING: 执行已创建但尚未开始。
        RUNNING: Graph 正在执行。
        SUCCEEDED: 执行成功完成。
        FAILED: 执行因业务或基础设施异常失败。
        CANCELLED: 执行已取消。
        TIMED_OUT: 执行或节点超时。
        STEP_LIMITED: 执行达到步数限制。
    """

    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    TIMED_OUT = "timed_out"
    STEP_LIMITED = "step_limited"

    @property
    def terminal(self) -> bool:
        """判断执行状态是否属于不再继续运行的终态。

        Returns:
            当前状态为终态时返回 True。
        """

        return self not in (ExecutionStatus.PENDING, ExecutionStatus.RUNNING)


class _OutputIterator(Iterator[Output]):
    """同步输出订阅，独立维护读取位置与关闭状态。

    Attributes:
        _execution: 输出迭代器关联的 Execution 句柄。
        _stream_id: 当前输出订阅的独立游标标识。
        _closed: 当前组件是否已停止接受新工作。
    """

    def __init__(self, execution: Execution, stream_id: int) -> None:
        """绑定执行句柄与同步输出流游标，初始状态为未关闭。

        Args:
            execution: 记录当前执行状态、控制限制及输出的句柄。
            stream_id: 当前输出流的独立游标标识。
        """

        self._execution = execution
        self._stream_id = stream_id
        self._closed = False

    def __next__(self) -> Output:
        """读取下一项输出，流结束或失败时解除当前订阅。

        Returns:
            当前游标处的下一项 Output。
        """

        if self._closed:
            raise StopIteration
        try:
            output = self._execution._next_output(self._stream_id)
        except BaseException:
            self.close()
            raise
        if output is None:
            self.close()
            raise StopIteration
        return output

    def close(self) -> None:
        """结束当前组件的生命周期并释放其拥有的资源。"""

        if not self._closed:
            self._closed = True
            self._execution._detach_stream(self._stream_id)

    def __del__(self) -> None:
        """回收对象时解除仍然存在的输出流订阅。"""

        self.close()


class _AsyncOutputIterator(AsyncIterator[Output]):
    """异步输出订阅，独立维护读取位置与关闭状态。

    Attributes:
        _execution: 输出迭代器关联的 Execution 句柄。
        _stream_id: 当前输出订阅的独立游标标识。
        _closed: 当前组件是否已停止接受新工作。
    """

    def __init__(self, execution: Execution, stream_id: int) -> None:
        """绑定执行句柄与异步输出流游标，初始状态为未关闭。

        Args:
            execution: 记录当前执行状态、控制限制及输出的句柄。
            stream_id: 当前输出流的独立游标标识。
        """

        self._execution = execution
        self._stream_id = stream_id
        self._closed = False

    def __aiter__(self) -> _AsyncOutputIterator:
        """返回当前对象的异步输出迭代入口。

        Returns:
            异步读取当前执行输出的迭代入口。
        """

        return self

    async def __anext__(self) -> Output:
        """异步读取下一项输出，结束或失败时解除当前订阅。

        Returns:
            当前游标处的下一项 Output。
        """

        if self._closed:
            raise StopAsyncIteration
        try:
            output = await self._execution._next_output_async(self._stream_id)
            if output is None:
                self.close()
                raise StopAsyncIteration
            return output
        except BaseException:
            self.close()
            raise

    def close(self) -> None:
        """结束当前组件的生命周期并释放其拥有的资源。"""

        if not self._closed:
            self._closed = True
            self._execution._detach_stream(self._stream_id)

    async def aclose(self) -> None:
        """异步关闭输出迭代器并解除背压订阅。"""

        self.close()

    def __del__(self) -> None:
        """回收对象时解除仍然存在的输出流订阅。"""

        self.close()


class Execution:
    """保存单次执行的身份、进度、结果，并提供协作式取消。

    Attributes:
        id: 当前对象的唯一标识。
        graph: 关联的 Graph 定义或注册名称。
        limits: 单次执行的步数与超时限制。
        output_buffer: 每个活跃输出流允许积压的最大条数。
        created_at: 执行句柄创建的 UTC 时间。
        _condition: 协调共享状态访问及同步等待的锁或条件变量。
        _store: 按追加顺序保存全部输出的可替换存储。
        _notifier: 唤醒同步与异步等待方的可替换通知器。
        _status: 当前执行的生命周期状态。
        _started_at: 执行开始的 UTC 时间，未开始时为 None。
        _finished_at: 执行结束的 UTC 时间，未结束时为 None。
        _started_monotonic: 执行开始的单调时钟读数，单位秒。
        _node_started_monotonic: 当前节点开始执行的单调时钟读数，单位秒。
        _step_timeout: 当前节点的超时秒数，None 表示不限制。
        _node_timeouts: 冻结 Graph 绑定的各节点超时秒数。
        _current_node: 当前执行中的节点 ID，空闲时为 None。
        _steps: 已经开始的节点触发次数。
        _error: 执行终止时保留的原始异常。
        _cancel_requested: 是否已收到协作式取消请求。
        _stream_counter: 分配独立输出订阅 ID 的计数器。
        _stream_cursors: 每个活跃输出流下次读取的位置，参与背压计算。
    """

    def __init__(
        self,
        graph: str,
        *,
        limits: ExecutionLimits | None = None,
        id: str | None = None,
        output_buffer: int = 64,
        output_store: OutputStore | None = None,
        notifier: ExecutionNotifier | None = None,
    ) -> None:
        """创建独立执行句柄并装配输出存储、通知器和控制限制。

        Args:
            graph: 目标 Graph 定义或其注册名称，以类型声明为准。
            limits: 本次执行独立使用的步数和时长限制。
            id: 对象标识，允许缺省时由实现生成。
            output_buffer: 每个活跃输出流允许积压的输出条数。
            output_store: 可替换的输出存储，构造执行时必须为空。
            notifier: 通知同步和异步等待方的可替换实现。

        Raises:
            TypeError: 参数类型或接口实现不符合当前契约。
            ValueError: 参数值或字段组合不合法。
        """

        self.id = require_non_empty_string(
            str(uuid4()) if id is None else id,
            "execution id",
        )
        self.graph = require_non_empty_string(graph, "execution graph")
        if limits is None:
            limits = ExecutionLimits()
        if not isinstance(limits, ExecutionLimits):
            raise TypeError("limits must be ExecutionLimits or None")
        self.limits = limits
        if type(output_buffer) is not int or output_buffer < 1:
            raise ValueError("output_buffer must be an integer greater than zero")
        self.output_buffer = output_buffer
        self.created_at = datetime.now(timezone.utc)

        self._condition = RLock()
        self._store = MemoryOutputStore() if output_store is None else output_store
        self._notifier = LocalExecutionNotifier() if notifier is None else notifier
        if not isinstance(self._store, OutputStore) or len(self._store):
            raise TypeError("output_store must be an empty OutputStore")
        if not isinstance(self._notifier, ExecutionNotifier):
            raise TypeError("notifier must implement ExecutionNotifier")
        self._status = ExecutionStatus.PENDING
        self._started_at: datetime | None = None
        self._finished_at: datetime | None = None
        self._started_monotonic: float | None = None
        self._node_started_monotonic: float | None = None
        self._step_timeout: float | None = None
        self._node_timeouts: dict[str, float | None] | None = None
        self._current_node: str | None = None
        self._steps = 0
        self._error: BaseException | None = None
        self._cancel_requested = False
        self._stream_counter = itertools.count()
        self._stream_cursors: dict[int, int] = {}

    @property
    def status(self) -> ExecutionStatus:
        """在线程安全边界内读取当前执行状态。

        Returns:
            当前执行的生命周期状态。
        """

        with self._condition:
            return self._status

    @property
    def started_at(self) -> datetime | None:
        """返回执行开始的 UTC 时间，尚未开始时为 None。

        Returns:
            执行开始的 UTC 时间，未开始时为 None。
        """

        with self._condition:
            return self._started_at

    @property
    def finished_at(self) -> datetime | None:
        """返回执行结束的 UTC 时间，未结束时为 None。

        Returns:
            执行结束的 UTC 时间，未结束时为 None。
        """

        with self._condition:
            return self._finished_at

    @property
    def current_node(self) -> str | None:
        """返回当前正在执行的节点 ID，空闲时为 None。

        Returns:
            当前执行中的节点 ID，空闲时为 None。
        """

        with self._condition:
            return self._current_node

    @property
    def steps(self) -> int:
        """返回已经开始的节点触发次数。

        Returns:
            已经开始的节点触发次数。
        """

        with self._condition:
            return self._steps

    @property
    def outputs(self) -> tuple[Output, ...] | None:
        """返回成功执行的完整输出，尚未成功时为 None。

        Returns:
            成功执行的全部输出；未成功时为 None。
        """

        with self._condition:
            if self._status is not ExecutionStatus.SUCCEEDED:
                return None
            return tuple(self._store[index] for index in range(len(self._store)))

    @property
    def error(self) -> BaseException | None:
        """在错误阶段执行回调，未恢复的异常继续传播。

        Returns:
            执行终止时保留的原始异常。
        """

        with self._condition:
            return self._error

    @property
    def cancel_requested(self) -> bool:
        """返回当前执行是否已收到取消请求。

        Returns:
            当前执行已收到取消请求时返回 True。
        """

        with self._condition:
            return self._cancel_requested

    @property
    def done(self) -> bool:
        """判断当前执行是否已经到达终态。

        Returns:
            当前执行已进入终态时返回 True。
        """

        return self.status.terminal

    def cancel(self) -> bool:
        """请求取消；尚未开始时立即取消，运行中在最近检查点生效。

        Returns:
            本次取消请求被接受时返回 True，执行已经结束时返回 False。
        """

        with self._condition:
            if self._status.terminal:
                return False
            self._cancel_requested = True
            if self._status is ExecutionStatus.PENDING:
                error = ExecutionCancelledError(
                    f"execution {self.id!r} was cancelled before it started",
                    graph=self.graph,
                )
                self._finish_locked(ExecutionStatus.CANCELLED, error=error)
            self._notifier.notify()
            return True

    def wait(self, timeout: float | None = None) -> bool:
        """等待终态；超时返回 False，不改变 Execution。

        Args:
            timeout: 等待或执行时限，单位秒；None 表示不设置时限。

        Returns:
            满足当前操作的判断条件时返回 True，否则返回 False。
        """

        _validate_timeout(timeout)
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            with self._condition:
                if self._status.terminal:
                    return True
                version = self._notifier.version
                remaining = None if deadline is None else deadline - time.monotonic()
                if remaining is not None and remaining <= 0:
                    return False
            self._notifier.wait(version, remaining)

    def result(self, timeout: float | None = None) -> tuple[Output, ...]:
        """等待并返回终端 Output，失败时重新抛出原始执行异常。

        Args:
            timeout: 等待或执行时限，单位秒；None 表示不设置时限。

        Returns:
            按产生顺序排列的全部终端 Output。

        Raises:
            TimeoutError: 等待未在指定时限内完成。
        """

        if not self.wait(timeout):
            raise TimeoutError(f"execution {self.id!r} did not finish in time")
        with self._condition:
            if self._error is not None:
                raise self._error
            outputs = self.outputs
            assert outputs is not None
            return outputs

    def __await__(self):
        """异步等待最终结果；取消等待方会协作式取消 execution。

        Returns:
            等待最终执行结果的协程迭代器。
        """

        return self._await_result().__await__()

    def __iter__(self) -> Iterator[Output]:
        """按产生顺序迭代 terminal Output；结束时传播执行异常。

        Returns:
            遍历当前对象内容的独立迭代入口。
        """

        stream_id = next(self._stream_counter)
        with self._condition:
            self._stream_cursors[stream_id] = 0
            self._notifier.notify()
        return _OutputIterator(self, stream_id)

    def __aiter__(self) -> AsyncIterator[Output]:
        """异步迭代 terminal Output，语义与同步迭代一致。

        Returns:
            异步读取当前执行输出的迭代入口。
        """

        stream_id = next(self._stream_counter)
        with self._condition:
            self._stream_cursors[stream_id] = 0
            self._notifier.notify()
        return _AsyncOutputIterator(self, stream_id)

    def checkpoint(self) -> None:
        """协作式检查取消、Graph timeout 和当前 Node timeout。"""

        self._checkpoint()

    @contextmanager
    def step(self, node_id: str) -> Iterator[None]:
        """记录一次 Node firing，并在其完整生命周期内应用控制限制。

        Args:
            node_id: Graph 内绑定的节点 ID。

        Yields:
            None；上下文内部的一次节点触发计入步数并受执行时限约束。

        Raises:
            ValueError: 参数值或字段组合不合法。
        """

        node_id = require_non_empty_string(node_id, "execution node")
        with self._condition:
            try:
                timeout = (
                    None
                    if self._node_timeouts is None
                    else self._node_timeouts[node_id]
                )
            except KeyError as exc:
                raise ValueError(
                    f"node {node_id!r} does not belong to this execution"
                ) from exc
        self._begin_step(node_id, timeout)
        try:
            yield
        finally:
            self._end_step()

    def start(self, graph: Graph) -> bool:
        """执行宿主绑定冻结 Graph 并开始计时；已取消时返回 False。

        Args:
            graph: 目标 Graph 定义或其注册名称，以类型声明为准。

        Returns:
            满足当前操作的判断条件时返回 True，否则返回 False。

        Raises:
            RuntimeError: 当前生命周期状态或操作顺序不允许此操作。
            TypeError: 参数类型或接口实现不符合当前契约。
        """

        if not isinstance(graph, Graph) or not graph.frozen:
            raise TypeError("execution requires a frozen Graph")
        self._bind_node_timeouts(graph._execution_timeouts())
        with self._condition:
            if self._status is ExecutionStatus.CANCELLED:
                return False
            if self._status is not ExecutionStatus.PENDING:
                raise RuntimeError(f"execution {self.id!r} has already started")
            now = datetime.now(timezone.utc)
            self._status = ExecutionStatus.RUNNING
            self._started_at = now
            self._started_monotonic = time.monotonic()
            self._notifier.notify()
            return True

    def _bind_node_timeouts(
        self,
        timeouts: Mapping[str, float | None],
    ) -> None:
        """在开始执行前绑定冻结 Graph 的 Node timeout 快照。

        Args:
            timeouts: Graph 冻结时保存的节点超时映射，单位秒。

        Raises:
            RuntimeError: 当前生命周期状态或操作顺序不允许此操作。
        """

        normalized: dict[str, float | None] = {}
        for node_id, timeout in timeouts.items():
            node_id = require_non_empty_string(node_id, "execution node")
            _validate_duration(timeout, f"node {node_id!r} timeout")
            normalized[node_id] = timeout
        with self._condition:
            if self._steps:
                raise RuntimeError("cannot bind node timeouts after execution steps")
            if self._node_timeouts is None:
                self._node_timeouts = normalized
            elif self._node_timeouts != normalized:
                raise RuntimeError("execution is bound to a different Graph definition")

    def _begin_step(self, node_id: str, timeout: float | None) -> None:
        """开始一次节点触发，检查执行状态与步数并记录节点计时。

        Args:
            node_id: Graph 内绑定的节点 ID。
            timeout: 等待或执行时限，单位秒；None 表示不设置时限。

        Raises:
            RuntimeError: 当前生命周期状态或操作顺序不允许此操作。
            StepLimitExceededError: 节点触发次数达到执行上限。
        """

        self._checkpoint()
        with self._condition:
            if self._status is not ExecutionStatus.RUNNING:
                raise RuntimeError("execution step requires RUNNING status")
            if self.limits.max_steps and self._steps >= self.limits.max_steps:
                raise StepLimitExceededError(
                    f"execution exceeded max_steps={self.limits.max_steps}",
                    graph=self.graph,
                    node=node_id,
                )
            self._steps += 1
            self._current_node = node_id
            self._node_started_monotonic = time.monotonic()
            self._step_timeout = timeout

    def _end_step(self) -> None:
        """结束节点计时并检查控制条件，始终清理当前节点状态。"""

        try:
            self._checkpoint()
        finally:
            with self._condition:
                self._current_node = None
                self._node_started_monotonic = None
                self._step_timeout = None

    def _checkpoint(self) -> None:
        """检查取消和执行时限，抛出最先到期的控制异常。

        Raises:
            ExecutionCancelledError: 当前执行已请求取消。
        """

        with self._condition:
            if self._cancel_requested:
                raise ExecutionCancelledError(
                    f"execution {self.id!r} was cancelled",
                    graph=self.graph,
                    node=self._current_node,
                )
            now = time.monotonic()
            expired: list[tuple[float, BaseException]] = []
            if self.limits.timeout is not None and self._started_monotonic is not None:
                deadline = self._started_monotonic + self.limits.timeout
                if now >= deadline:
                    expired.append(
                        (
                            deadline,
                            ExecutionTimeoutError(
                                f"execution exceeded timeout={self.limits.timeout}",
                                graph=self.graph,
                                node=self._current_node,
                            ),
                        )
                    )
            if (
                self._step_timeout is not None
                and self._node_started_monotonic is not None
            ):
                deadline = self._node_started_monotonic + self._step_timeout
                if now >= deadline:
                    expired.append(
                        (
                            deadline,
                            NodeTimeoutError(
                                f"node {self._current_node!r} exceeded "
                                f"timeout={self._step_timeout}",
                                graph=self.graph,
                                node=self._current_node,
                            ),
                        )
                    )
            if expired:
                raise min(expired, key=lambda item: item[0])[1]

    def wait_timeout(self) -> float | None:
        """返回当前异步等待预算；调用前也会解释取消和过期原因。

        Returns:
            剩余等待秒数；没有有效时限时返回 None。
        """

        self._checkpoint()
        with self._condition:
            now = time.monotonic()
            remaining: list[float] = []
            if self.limits.timeout is not None and self._started_monotonic is not None:
                remaining.append(self.limits.timeout - (now - self._started_monotonic))
            if (
                self._step_timeout is not None
                and self._node_started_monotonic is not None
            ):
                remaining.append(
                    self._step_timeout - (now - self._node_started_monotonic)
                )
            return None if not remaining else max(0.0, min(remaining))

    def succeed(self) -> None:
        """执行宿主在执行器完成后结束执行；结果来自已交付输出。

        Raises:
            RuntimeError: 当前生命周期状态或操作顺序不允许此操作。
        """

        with self._condition:
            if self._status.terminal:
                return
            self.checkpoint()
            if self._status is not ExecutionStatus.RUNNING:
                raise RuntimeError("execution must be running before success")
            self._finish_locked(ExecutionStatus.SUCCEEDED)

    def fail(self, error: BaseException) -> None:
        """执行宿主记录失败；已发布输出仍可由流消费者读取。

        Args:
            error: 需要传播、记录或用于恢复的异常。

        Raises:
            TypeError: 参数类型或接口实现不符合当前契约。
        """
        if not isinstance(error, BaseException):
            raise TypeError("execution failure must be a BaseException")
        with self._condition:
            if self._status.terminal:
                return
            if isinstance(error, ExecutionCancelledError):
                status = ExecutionStatus.CANCELLED
            elif isinstance(error, (ExecutionTimeoutError, NodeTimeoutError)):
                status = ExecutionStatus.TIMED_OUT
            elif isinstance(error, StepLimitExceededError):
                status = ExecutionStatus.STEP_LIMITED
            else:
                status = ExecutionStatus.FAILED
            self._finish_locked(status, error=error)

    def _finish_locked(
        self,
        status: ExecutionStatus,
        *,
        error: BaseException | None = None,
    ) -> None:
        """在已持有执行锁的条件下写入终态并通知等待方。

        Args:
            status: 需要记录或验证的状态值。
            error: 需要传播、记录或用于恢复的异常。
        """

        self._status = status
        self._error = error
        self._current_node = None
        self._node_started_monotonic = None
        self._step_timeout = None
        self._finished_at = datetime.now(timezone.utc)
        self._notifier.notify()

    def _try_publish(self, output: Output) -> bool:
        """尝试追加输出；存在背压时返回未提交状态。

        Args:
            output: 需要校验、保存或交付的一项 Output。

        Returns:
            输出已被存储接受时返回 True，受背压阻塞时返回 False。

        Raises:
            RuntimeError: 当前生命周期状态或操作顺序不允许此操作。
            TypeError: 参数类型或接口实现不符合当前契约。
        """

        if not isinstance(output, Output):
            raise TypeError("execution can publish only Output")
        with self._condition:
            self.checkpoint()
            if self._status is not ExecutionStatus.RUNNING:
                raise RuntimeError("output publication requires RUNNING status")
            if any(
                len(self._store) - cursor >= self.output_buffer
                for cursor in self._stream_cursors.values()
            ):
                return False
            self._store.append(output)
            self._notifier.notify()
            return True

    def publish_output(self, output: Output) -> None:
        """同步交付 terminal Output；应用取消、超时和流消费者背压。

        Args:
            output: 需要校验、保存或交付的一项 Output。
        """

        while True:
            with self._condition:
                version = self._notifier.version
                if self._try_publish(output):
                    return
                timeout = self.wait_timeout()
            self._notifier.wait(version, timeout)

    async def apublish_output(self, output: Output) -> None:
        """异步交付 terminal Output，等待背压时不阻塞事件循环。

        Args:
            output: 需要校验、保存或交付的一项 Output。
        """

        while True:
            with self._condition:
                version = self._notifier.version
                if self._try_publish(output):
                    return
                timeout = self.wait_timeout()
            try:
                await asyncio.wait_for(self._notifier.wait_async(version), timeout)
            except asyncio.TimeoutError:
                self.checkpoint()

    async def _await_result(self) -> tuple[Output, ...]:
        """异步等待最终结果，取消等待方时同步请求取消底层执行。

        Returns:
            按产生顺序排列的全部终端 Output。
        """

        try:
            while True:
                with self._condition:
                    version = self._notifier.version
                    if self._status.terminal:
                        return self.result(0)
                await self._notifier.wait_async(version)
        except asyncio.CancelledError:
            self.cancel()
            raise

    def _next_output(self, stream_id: int) -> Output | None:
        """同步等待当前流的下一项输出或终态。

        Args:
            stream_id: 当前输出流的独立游标标识。

        Returns:
            符合声明端口契约的 Output。
        """

        while True:
            with self._condition:
                version = self._notifier.version
                output, pending = self._poll_output(stream_id)
                if output is not None or not pending:
                    return output
            self._notifier.wait(version)

    async def _next_output_async(self, stream_id: int) -> Output | None:
        """异步等待当前流的下一项输出或终态。

        Args:
            stream_id: 当前输出流的独立游标标识。

        Returns:
            符合声明端口契约的 Output。
        """

        while True:
            with self._condition:
                version = self._notifier.version
                output, pending = self._poll_output(stream_id)
                if output is not None or not pending:
                    return output
            await self._notifier.wait_async(version)

    def _poll_output(self, stream_id: int) -> tuple[Output | None, bool]:
        """读取当前流游标处的输出，或报告需要继续等待。

        Args:
            stream_id: 当前输出流的独立游标标识。

        Returns:
            符合声明端口契约的 Output 集合。
        """

        with self._condition:
            cursor = self._stream_cursors[stream_id]
            if cursor < len(self._store):
                output = self._store[cursor]
                self._stream_cursors[stream_id] = cursor + 1
                self._notifier.notify()
                return output, True
            if self.done:
                if self._error is not None:
                    raise self._error
                return None, False
            return None, True

    def _detach_stream(self, stream_id: int) -> None:
        """移除输出流游标并唤醒因背压等待的生产者。

        Args:
            stream_id: 当前输出流的独立游标标识。
        """

        with self._condition:
            self._stream_cursors.pop(stream_id, None)
            self._notifier.notify()
