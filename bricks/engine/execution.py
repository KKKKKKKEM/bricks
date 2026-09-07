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
    if value is None:
        return
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{label} must be a number or None")
    if not math.isfinite(value) or value <= 0:
        raise ValueError(f"{label} must be a finite number greater than zero")


@dataclass(frozen=True, slots=True)
class ExecutionLimits:
    """控制一次 Graph execution；零步数和空 timeout 表示无限制。"""

    max_steps: int = 0
    timeout: float | None = None

    def __post_init__(self) -> None:
        if type(self.max_steps) is not int:
            raise TypeError("max_steps must be an integer")
        if self.max_steps < 0:
            raise ValueError("max_steps must not be negative")
        _validate_duration(self.timeout, "timeout")


class ExecutionStatus(str, enum.Enum):
    """一项 Execution 对调用方可见的生命周期状态。"""

    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    TIMED_OUT = "timed_out"
    STEP_LIMITED = "step_limited"

    @property
    def terminal(self) -> bool:
        return self not in (ExecutionStatus.PENDING, ExecutionStatus.RUNNING)


class _OutputIterator(Iterator[Output]):
    def __init__(self, execution: Execution, stream_id: int) -> None:
        self._execution = execution
        self._stream_id = stream_id
        self._closed = False

    def __next__(self) -> Output:
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
        if not self._closed:
            self._closed = True
            self._execution._detach_stream(self._stream_id)

    def __del__(self) -> None:
        self.close()


class _AsyncOutputIterator(AsyncIterator[Output]):
    def __init__(self, execution: Execution, stream_id: int) -> None:
        self._execution = execution
        self._stream_id = stream_id
        self._closed = False

    def __aiter__(self) -> _AsyncOutputIterator:
        return self

    async def __anext__(self) -> Output:
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
        if not self._closed:
            self._closed = True
            self._execution._detach_stream(self._stream_id)

    async def aclose(self) -> None:
        self.close()

    def __del__(self) -> None:
        self.close()


class Execution:
    """保存单次执行的身份、进度、结果，并提供协作式取消。"""

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
        with self._condition:
            return self._status

    @property
    def started_at(self) -> datetime | None:
        with self._condition:
            return self._started_at

    @property
    def finished_at(self) -> datetime | None:
        with self._condition:
            return self._finished_at

    @property
    def current_node(self) -> str | None:
        with self._condition:
            return self._current_node

    @property
    def steps(self) -> int:
        with self._condition:
            return self._steps

    @property
    def outputs(self) -> tuple[Output, ...] | None:
        with self._condition:
            if self._status is not ExecutionStatus.SUCCEEDED:
                return None
            return tuple(self._store[index] for index in range(len(self._store)))

    @property
    def error(self) -> BaseException | None:
        with self._condition:
            return self._error

    @property
    def cancel_requested(self) -> bool:
        with self._condition:
            return self._cancel_requested

    @property
    def done(self) -> bool:
        return self.status.terminal

    def cancel(self) -> bool:
        """请求取消；尚未开始时立即取消，运行中在最近检查点生效。"""

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
        """等待终态；超时返回 False，不改变 Execution。"""

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
        """等待并返回终端 Output，失败时重新抛出原始执行异常。"""

        if not self.wait(timeout):
            raise TimeoutError(f"execution {self.id!r} did not finish in time")
        with self._condition:
            if self._error is not None:
                raise self._error
            outputs = self.outputs
            assert outputs is not None
            return outputs

    def __await__(self):
        """异步等待最终结果；取消等待方会协作式取消 execution。"""

        return self._await_result().__await__()

    def __iter__(self) -> Iterator[Output]:
        """按产生顺序迭代 terminal Output；结束时传播执行异常。"""

        stream_id = next(self._stream_counter)
        with self._condition:
            self._stream_cursors[stream_id] = 0
            self._notifier.notify()
        return _OutputIterator(self, stream_id)

    def __aiter__(self) -> AsyncIterator[Output]:
        """异步迭代 terminal Output，语义与同步迭代一致。"""

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
        """记录一次 Node firing，并在其完整生命周期内应用控制限制。"""

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
        """执行宿主绑定冻结 Graph 并开始计时；已取消时返回 False。"""

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
        """在开始执行前绑定冻结 Graph 的 Node timeout 快照。"""

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
        try:
            self._checkpoint()
        finally:
            with self._condition:
                self._current_node = None
                self._node_started_monotonic = None
                self._step_timeout = None

    def _checkpoint(self) -> None:
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
        """返回当前异步等待预算；调用前也会解释取消和过期原因。"""

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
        """执行宿主在执行器完成后结束执行；结果来自已交付输出。"""

        with self._condition:
            if self._status.terminal:
                return
            self.checkpoint()
            if self._status is not ExecutionStatus.RUNNING:
                raise RuntimeError("execution must be running before success")
            self._finish_locked(ExecutionStatus.SUCCEEDED)

    def fail(self, error: BaseException) -> None:
        """执行宿主记录失败；已发布输出仍可由流消费者读取。"""
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
        self._status = status
        self._error = error
        self._current_node = None
        self._node_started_monotonic = None
        self._step_timeout = None
        self._finished_at = datetime.now(timezone.utc)
        self._notifier.notify()

    def _try_publish(self, output: Output) -> bool:
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
        """同步交付 terminal Output；应用取消、超时和流消费者背压。"""

        while True:
            with self._condition:
                version = self._notifier.version
                if self._try_publish(output):
                    return
                timeout = self.wait_timeout()
            self._notifier.wait(version, timeout)

    async def apublish_output(self, output: Output) -> None:
        """异步交付 terminal Output，等待背压时不阻塞事件循环。"""

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
        while True:
            with self._condition:
                version = self._notifier.version
                output, pending = self._poll_output(stream_id)
                if output is not None or not pending:
                    return output
            self._notifier.wait(version)

    async def _next_output_async(self, stream_id: int) -> Output | None:
        while True:
            with self._condition:
                version = self._notifier.version
                output, pending = self._poll_output(stream_id)
                if output is not None or not pending:
                    return output
            await self._notifier.wait_async(version)

    def _poll_output(self, stream_id: int) -> tuple[Output | None, bool]:
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
        with self._condition:
            self._stream_cursors.pop(stream_id, None)
            self._notifier.notify()
