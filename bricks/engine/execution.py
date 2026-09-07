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
from threading import Condition, RLock
from typing import TYPE_CHECKING
from uuid import uuid4

from .core import Output, _validate_timeout, require_non_empty_string
from .errors import (
    ExecutionCancelledError,
    ExecutionTimeoutError,
    NodeTimeoutError,
    StepLimitExceededError,
)

if TYPE_CHECKING:
    from collections.abc import Callable


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
            while True:
                output, pending = self._execution._poll_output(self._stream_id)
                if output is not None:
                    return output
                if not pending:
                    self.close()
                    raise StopAsyncIteration
                await asyncio.sleep(0.01)
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

        self._condition = Condition(RLock())
        self._status = ExecutionStatus.PENDING
        self._started_at: datetime | None = None
        self._finished_at: datetime | None = None
        self._started_monotonic: float | None = None
        self._node_started_monotonic: float | None = None
        self._step_timeout: float | None = None
        self._node_timeouts: dict[str, float | None] | None = None
        self._current_node: str | None = None
        self._steps = 0
        self._outputs: tuple[Output, ...] | None = None
        self._error: BaseException | None = None
        self._cancel_requested = False
        self._published_outputs: list[Output] = []
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
            return self._outputs

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
            self._condition.notify_all()
            return True

    def wait(self, timeout: float | None = None) -> bool:
        """等待终态；超时返回 False，不改变 Execution。"""

        _validate_timeout(timeout)
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._condition:
            while not self._status.terminal:
                remaining = None if deadline is None else deadline - time.monotonic()
                if remaining is not None and remaining <= 0:
                    return False
                self._condition.wait(remaining)
            return True

    def result(self, timeout: float | None = None) -> tuple[Output, ...]:
        """等待并返回终端 Output，失败时重新抛出原始执行异常。"""

        if not self.wait(timeout):
            raise TimeoutError(f"execution {self.id!r} did not finish in time")
        with self._condition:
            if self._error is not None:
                raise self._error
            assert self._outputs is not None
            return self._outputs

    def __await__(self):
        """异步等待最终结果；取消等待方会协作式取消 execution。"""

        return self._await_result().__await__()

    def __iter__(self) -> Iterator[Output]:
        """按产生顺序迭代 terminal Output；结束时传播执行异常。"""

        stream_id = next(self._stream_counter)
        with self._condition:
            self._stream_cursors[stream_id] = 0
            self._condition.notify_all()
        return _OutputIterator(self, stream_id)

    def __aiter__(self) -> AsyncIterator[Output]:
        """异步迭代 terminal Output，语义与同步迭代一致。"""

        stream_id = next(self._stream_counter)
        with self._condition:
            self._stream_cursors[stream_id] = 0
            self._condition.notify_all()
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

    def _start(self) -> bool:
        with self._condition:
            if self._status is ExecutionStatus.CANCELLED:
                return False
            if self._status is not ExecutionStatus.PENDING:
                raise RuntimeError(f"execution {self.id!r} has already started")
            now = datetime.now(timezone.utc)
            self._status = ExecutionStatus.RUNNING
            self._started_at = now
            self._started_monotonic = time.monotonic()
            self._condition.notify_all()
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

    def _wait_timeout(self) -> float | None:
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

    def _succeed(self, outputs: tuple[Output, ...]) -> None:
        with self._condition:
            if self._status.terminal:
                return
            if self._cancel_requested:
                error = ExecutionCancelledError(
                    f"execution {self.id!r} was cancelled",
                    graph=self.graph,
                )
                self._finish_locked(ExecutionStatus.CANCELLED, error=error)
                return
            self._finish_locked(ExecutionStatus.SUCCEEDED, outputs=outputs)

    def _fail(self, error: BaseException) -> None:
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
        outputs: tuple[Output, ...] | None = None,
        error: BaseException | None = None,
    ) -> None:
        self._status = status
        self._outputs = outputs
        self._error = error
        self._current_node = None
        self._node_started_monotonic = None
        self._step_timeout = None
        self._finished_at = datetime.now(timezone.utc)
        self._condition.notify_all()

    def _publish_output(self, output: Output) -> None:
        """发布 terminal Output；活跃消费者落后时施加有界背压。"""

        if not isinstance(output, Output):
            raise TypeError("execution can publish only Output")
        with self._condition:
            while self._stream_cursors and any(
                len(self._published_outputs) - cursor >= self.output_buffer
                for cursor in self._stream_cursors.values()
            ):
                self._checkpoint()
                self._condition.wait(0.05)
            self._published_outputs.append(output)
            self._condition.notify_all()

    def _complete_outputs(self, outputs: tuple[Output, ...]) -> None:
        """为不支持增量 sink 的 GraphExecutor 补发尚未发布的最终结果。"""

        with self._condition:
            published = len(self._published_outputs)
        for output in outputs[published:]:
            self._publish_output(output)

    async def _await_result(self) -> tuple[Output, ...]:
        try:
            return await asyncio.to_thread(self.result)
        except asyncio.CancelledError:
            self.cancel()
            raise

    def _next_output(self, stream_id: int) -> Output | None:
        with self._condition:
            while True:
                cursor = self._stream_cursors[stream_id]
                if cursor < len(self._published_outputs):
                    output = self._published_outputs[cursor]
                    self._stream_cursors[stream_id] = cursor + 1
                    self._condition.notify_all()
                    return output
                if self.done:
                    self.result(0)
                    return None
                self._condition.wait()

    def _poll_output(self, stream_id: int) -> tuple[Output | None, bool]:
        with self._condition:
            cursor = self._stream_cursors[stream_id]
            if cursor < len(self._published_outputs):
                output = self._published_outputs[cursor]
                self._stream_cursors[stream_id] = cursor + 1
                self._condition.notify_all()
                return output, True
            if self.done:
                self.result(0)
                return None, False
            return None, True

    def _detach_stream(self, stream_id: int) -> None:
        with self._condition:
            self._stream_cursors.pop(stream_id, None)
            self._condition.notify_all()

    def _callbacks(self) -> tuple[Callable[[], None], Callable[[], float | None]]:
        return self.checkpoint, self._wait_timeout
