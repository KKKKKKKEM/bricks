"""图的一次运行。

Machine 只负责三件事：保存 Context、消费 Event、执行 Transition。
持久化、响应式路由和 Fork/Join 都通过独立对象接入。
"""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import AsyncIterator, Iterable, Iterator, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Optional

from ..errors import (
    CancellationError,
    DuplicateEvent,
    InternalStepLimitExceeded,
    MachineNotRunnable,
    MachineNotStarted,
    NoTransition,
)
from ..events.bus import EventBus
from ..events.hooks import HookContext, HookRegistry
from ..events.messages import Event, RuntimeEvent
from ..graph.graph import Graph
from ..graph.nodes import BaseNode
from ..graph.transitions import Transition
from ..policies.cancellation import CancellationToken
from ..policies.idempotency import IdempotencyStore
from ..policies.retry import RetryPolicy
from ..policies.timeout import TimeoutPolicy
from .context import Context
from .executor import ActionExecutor, InlineExecutor, PolicyExecutor
from .effects import StagedEffect
from .fork import ForkController, ForkGroup, ForkRuntime, ForkRuntimeFactory
from .interpreter import (
    OutcomeDirective,
    OutcomeInterpreter,
    default_outcome_interpreter,
)
from .lifecycle import LifecycleEvent, Status
from .outcomes import Next, Outcome, Retry, Update
from .outcome_runtime import MachineOutcomeRuntime
from .selector import DefaultTransitionSelector, TransitionSelector


def _now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True, slots=True)
class TransitionResult:
    """一次事件迁移的结果。"""

    event: Event
    source: str
    target: str
    transition: Transition
    outcome: Optional[Outcome]
    status: Status
    children: tuple["Machine", ...] = ()


@dataclass(slots=True)
class _TransitionFrame:
    """Shared state machine for the sync and async transition pipelines."""

    source: str
    target: str
    declared_target: str
    outcome: Optional[Outcome] = None
    stopped: bool = False

    def accept(
        self,
        outcome: Optional[Outcome],
        directive: OutcomeDirective,
    ) -> None:
        if outcome is not None:
            self.outcome = outcome
        self.stopped = OutcomeDirective(directive) is OutcomeDirective.STOP


class Machine:
    """使用显式事件运行一张图的一个实例。"""

    def __init__(
        self,
        graph: Graph,
        *,
        context: Optional[Context] = None,
        executor: Optional[ActionExecutor] = None,
        events: Optional[EventBus] = None,
        hooks: Optional[HookRegistry] = None,
        retry_policy: Optional[RetryPolicy] = None,
        cancellation: Optional[CancellationToken] = None,
        timeout: Optional[TimeoutPolicy] = None,
        idempotency: Optional[IdempotencyStore] = None,
        graph_resolver: Optional[Callable[[str], Graph]] = None,
        selector: Optional[TransitionSelector] = None,
        outcome_handlers: Optional[
            Mapping[type[Outcome], Callable[["Machine", Outcome], Any]]
        ] = None,
        outcome_interpreter: Optional[OutcomeInterpreter] = None,
        fork_runtime_factory: Optional[ForkRuntimeFactory] = None,
        clock: Optional[Callable[[], datetime]] = None,
        max_internal_steps: int = 100,
        _runtime_observer: Optional[Callable[[RuntimeEvent], None]] = None,
    ) -> None:
        self.graph = graph
        self.context = context or Context(
            graph_id=graph.id,
            graph_version=graph.version,
        )
        if self.context.graph_id != graph.id:
            raise ValueError("context graph_id does not match graph id")
        if str(self.context.graph_version) != graph.version:
            raise ValueError(
                f"context graph_version {self.context.graph_version!r} does not "
                f"match graph {graph.version!r}"
            )
        self._validate_context()
        if max_internal_steps < 1:
            raise ValueError("max_internal_steps must be positive")

        self.executor = executor or InlineExecutor()
        self.events = events or EventBus()
        self.hooks = hooks or HookRegistry()
        self.retry_policy = retry_policy or RetryPolicy()
        self.cancellation = cancellation
        self.timeout = timeout
        self.clock = clock or _now
        self.idempotency = idempotency
        self.graph_resolver = graph_resolver
        self.selector = selector or DefaultTransitionSelector()
        if outcome_interpreter is not None and outcome_handlers:
            raise ValueError(
                "outcome_interpreter and outcome_handlers cannot be combined"
            )
        self.outcome_handlers = dict(outcome_handlers or {})
        self.outcome_interpreter = outcome_interpreter or default_outcome_interpreter(
            self.outcome_handlers
        )
        self.action_executor = PolicyExecutor(
            self.executor,
            cancellation=cancellation,
            timeout=timeout,
        )
        self.max_internal_steps = max_internal_steps
        self._runtime_observer = _runtime_observer
        self._runtime_sequence = int(
            self.context.metadata.get("runtime_sequence", 0)
        )
        self._last_runtime_event: Optional[RuntimeEvent] = None
        self._staged_effects: dict[str, StagedEffect] = {}
        self._deferred_emitted_events: list[Event] = []
        self.fork_runtime_factory: ForkRuntimeFactory = (
            fork_runtime_factory or ForkController
        )
        self._forks: ForkRuntime = self.fork_runtime_factory(self)
        self._outcome_runtime = MachineOutcomeRuntime(self)

    def _validate_context(self) -> None:
        if self.context.status is Status.CREATED:
            if self.context.node_id is not None or self.context.waiting is not None:
                raise ValueError("created context cannot have a node or waiting state")
            return
        if self.context.node_id not in self.graph.nodes:
            raise ValueError(
                f"context node_id {self.context.node_id!r} is not defined by graph"
            )
        if self.context.status is Status.WAITING and self.context.waiting is None:
            raise ValueError("waiting context must include waiting state")
        if (
            self.context.status is not Status.WAITING
            and self.context.waiting is not None
        ):
            raise ValueError(
                f"{self.context.status.value} context cannot include waiting state"
            )

    @property
    def node_id(self) -> Optional[str]:
        return self.context.node_id

    @property
    def status(self) -> Status:
        return self.context.status

    @property
    def fork_group(self) -> Optional[ForkGroup]:
        """返回待 Join 的 Fork；并行能力的实现由 ForkController 持有。"""
        return self._forks.group

    @property
    def runtime_sequence(self) -> int:
        """返回最近一次运行事件序号。"""
        return self._runtime_sequence

    @property
    def staged_effects(self) -> tuple[StagedEffect, ...]:
        """Return uncommitted effect intents without exposing mutable storage."""
        return tuple(self._staged_effects.values())

    def start(self) -> Context:
        self._require(Status.CREATED)
        self._check_cancelled()
        event = Event("__start__", source=self.context.run_id)
        self._hook(LifecycleEvent.BEFORE_START.value, event=event)
        self.context.status = Status.RUNNING
        self.context.node_id = self.graph.initial
        try:
            outcome = self._enter(self.graph.node(self._active_node_id()), event)
            self._interpret(outcome)
            self._drain_next(outcome)
            self._complete_if_terminal()
        except Exception:
            self._discard_staged_effects()
            self._discard_emitted_events()
            self.context.status = Status.FAILED
            raise
        self._flush_emitted_events()
        # 启动已经完成后，观察 Hook 的失败不能把已完成的运行伪装成执行失败。
        self._hook(LifecycleEvent.AFTER_START.value, event=event)
        return self.context

    async def start_async(self) -> Context:
        self._require(Status.CREATED)
        self._check_cancelled()
        event = Event("__start__", source=self.context.run_id)
        await self._hook_async(LifecycleEvent.BEFORE_START.value, event=event)
        self.context.status = Status.RUNNING
        self.context.node_id = self.graph.initial
        try:
            outcome = await self._enter_async(self.graph.node(self.context.node_id), event)
            await self._interpret_async(outcome)
            await self._drain_next_async(outcome)
            self._complete_if_terminal()
        except asyncio.CancelledError:
            self._mark_task_cancelled()
            raise
        except Exception:
            self._discard_staged_effects()
            self._discard_emitted_events()
            self.context.status = Status.FAILED
            raise
        await self._flush_emitted_events_async()
        # 启动已经完成后，观察 Hook 的失败不能把已完成的运行伪装成执行失败。
        try:
            await self._hook_async(LifecycleEvent.AFTER_START.value, event=event)
        except asyncio.CancelledError:
            self._mark_task_cancelled()
            raise
        return self.context

    def dispatch(self, event: Event | str, payload: Any = None) -> TransitionResult:
        self._ensure_runnable()
        return self._dispatch_event(self._event(event, payload))

    async def dispatch_async(
        self, event: Event | str, payload: Any = None
    ) -> TransitionResult:
        self._ensure_runnable()
        return await self._dispatch_event_async(self._event(event, payload))

    def update_context(
        self,
        values: Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> Context:
        """受控地更新业务数据，不触发 Graph 迁移。

        这个入口适合外部人工输入、恢复驱动或控制面更新；需要让更新被 Hook 和
        PersistenceBinding 观察时，应使用它，而不是直接修改 ``context.data``。
        """
        updates = self._normalize_context_update(values, kwargs)
        self.context.update(updates)
        event = Event(
            "__context_update__",
            payload=updates,
            source=self.context.run_id,
        )
        self._hook(LifecycleEvent.CONTEXT_UPDATED.value, event=event)
        return self.context

    async def update_context_async(
        self,
        values: Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> Context:
        """异步版本的 ``update_context``。"""
        updates = self._normalize_context_update(values, kwargs)
        self.context.update(updates)
        event = Event(
            "__context_update__",
            payload=updates,
            source=self.context.run_id,
        )
        await self._hook_async(LifecycleEvent.CONTEXT_UPDATED.value, event=event)
        return self.context

    def invoke(
        self,
        events: Iterable[Event | str | tuple[str, Any]] = (),
        *,
        auto_start: bool = True,
    ) -> Context:
        """启动（可选）并按顺序消费一组输入，返回最终 Context。"""
        if auto_start and self.status is Status.CREATED:
            self.start()
        for item in events:
            self._consume(item)
        return self.context

    async def ainvoke(
        self,
        events: Iterable[Event | str | tuple[str, Any]] = (),
        *,
        auto_start: bool = True,
    ) -> Context:
        """异步版本的 ``invoke``。"""
        if auto_start and self.status is Status.CREATED:
            await self.start_async()
        for item in events:
            await self._consume_async(item)
        return self.context

    def stream(
        self,
        events: Iterable[Event | str | tuple[str, Any]] = (),
        *,
        auto_start: bool = True,
    ) -> Iterator[TransitionResult]:
        """按输入顺序产出每次外部事件的迁移结果。

        节点内部的 ``Next`` 迁移会在对应的外部结果返回前排空，不单独产生重复输出。
        """
        if auto_start and self.status is Status.CREATED:
            self.start()
        for item in events:
            yield self._consume(item)

    async def astream(
        self,
        events: Iterable[Event | str | tuple[str, Any]] = (),
        *,
        auto_start: bool = True,
    ) -> AsyncIterator[TransitionResult]:
        """异步版本的 ``stream``。"""
        if auto_start and self.status is Status.CREATED:
            await self.start_async()
        for item in events:
            yield await self._consume_async(item)

    def stream_events(
        self,
        events: Iterable[Event | str | tuple[str, Any]] = (),
        *,
        auto_start: bool = True,
        match: Optional[Callable[[RuntimeEvent], bool]] = None,
    ) -> Iterator[RuntimeEvent]:
        """按运行生命周期产出细粒度事件。

        ``stream()`` 继续只产出迁移结果；这个入口用于调试、追踪和编排 UI。事件
        在当前操作完成后按发生顺序排出，子运行共享同一观察器。传入 ``match``
        可以只观察感兴趣的 RuntimeEvent；未匹配的事件仍会交给已有观察器。
        """
        buffer: list[RuntimeEvent] = []
        previous = self._runtime_observer

        def collect(runtime_event: RuntimeEvent) -> None:
            buffer.append(runtime_event)
            if previous is not None:
                previous(runtime_event)

        def pending(start: int) -> Iterator[RuntimeEvent]:
            for runtime_event in buffer[start:]:
                if match is None or match(runtime_event):
                    yield runtime_event

        self._runtime_observer = collect
        cursor = 0
        try:
            if auto_start and self.status is Status.CREATED:
                try:
                    self.start()
                except BaseException:
                    yield from pending(cursor)
                    raise
                yield from pending(cursor)
                cursor = len(buffer)
            for item in events:
                try:
                    self._consume(item)
                except BaseException:
                    yield from pending(cursor)
                    raise
                yield from pending(cursor)
                cursor = len(buffer)
        finally:
            self._runtime_observer = previous

    async def astream_events(
        self,
        events: Iterable[Event | str | tuple[str, Any]] = (),
        *,
        auto_start: bool = True,
        match: Optional[Callable[[RuntimeEvent], bool]] = None,
    ) -> AsyncIterator[RuntimeEvent]:
        """异步版本的 ``stream_events``。"""
        queue: asyncio.Queue[RuntimeEvent] = asyncio.Queue()
        previous = self._runtime_observer

        def collect(runtime_event: RuntimeEvent) -> None:
            queue.put_nowait(runtime_event)
            if previous is not None:
                previous(runtime_event)

        self._runtime_observer = collect

        async def execute(awaitable: Any) -> AsyncIterator[RuntimeEvent]:
            operation = asyncio.create_task(awaitable)
            try:
                while not operation.done():
                    event_waiter = asyncio.create_task(queue.get())
                    done, _ = await asyncio.wait(
                        {operation, event_waiter},
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    if event_waiter in done:
                        runtime_event = event_waiter.result()
                        if match is None or match(runtime_event):
                            yield runtime_event
                    else:
                        event_waiter.cancel()
                        await asyncio.gather(event_waiter, return_exceptions=True)
                while not queue.empty():
                    runtime_event = queue.get_nowait()
                    if match is None or match(runtime_event):
                        yield runtime_event
                await operation
            finally:
                if not operation.done():
                    operation.cancel()
                    await asyncio.gather(operation, return_exceptions=True)

        try:
            if auto_start and self.status is Status.CREATED:
                async for runtime_event in execute(self.start_async()):
                    yield runtime_event
            for item in events:
                async for runtime_event in execute(self._consume_async(item)):
                    yield runtime_event
        finally:
            self._runtime_observer = previous

    def resume(self, event: Event | str, payload: Any = None) -> TransitionResult:
        self._ensure_waiting("event")
        received = self._event(event, payload)
        claim = self._claim_event(received)
        commit_state = [False]
        expected = (self.context.waiting or {}).get("resume_event")
        if expected is not None and received.name != expected:
            self._release_event(claim)
            raise MachineNotRunnable(
                f"run is waiting for {expected!r}, received {received.name!r}"
            )
        try:
            self._check_cancelled()
            transition = self._resolve(received)
            self._resume(received)
            result = self._dispatch_event(
                received,
                claimed=True,
                transition=transition,
                on_commit=lambda: commit_state.__setitem__(0, True),
            )
        except BaseException:
            if not commit_state[0]:
                self._release_event(claim)
            raise
        self._hook(
            LifecycleEvent.AFTER_RESUME.value,
            event=received,
            result=result,
        )
        return result

    async def resume_async(
        self, event: Event | str, payload: Any = None
    ) -> TransitionResult:
        self._ensure_waiting("event")
        received = self._event(event, payload)
        claim = self._claim_event(received)
        commit_state = [False]
        expected = (self.context.waiting or {}).get("resume_event")
        if expected is not None and received.name != expected:
            self._release_event(claim)
            raise MachineNotRunnable(
                f"run is waiting for {expected!r}, received {received.name!r}"
            )
        try:
            self._check_cancelled()
            transition = await self._resolve_async(received)
            await self._resume_async(received)
            result = await self._dispatch_event_async(
                received,
                claimed=True,
                transition=transition,
                on_commit=lambda: commit_state.__setitem__(0, True),
            )
        except asyncio.CancelledError:
            if not commit_state[0]:
                self._mark_task_cancelled()
                self._release_event(claim)
            raise
        except BaseException:
            if not commit_state[0]:
                self._release_event(claim)
            raise
        try:
            await self._hook_async(
                LifecycleEvent.AFTER_RESUME.value,
                event=received,
                result=result,
            )
        except asyncio.CancelledError:
            self._mark_task_cancelled()
            raise
        return result

    def resume_retry(
        self,
        payload: Any = None,
        *,
        event: Event | str | None = None,
    ) -> Context:
        self._ensure_waiting("retry")
        expected = (self.context.waiting or {}).get("resume_event") or "__retry__"
        received = (
            Event(expected, payload=payload, source=self.context.run_id)
            if event is None
            else self._event(event, payload)
        )
        if received.name != expected:
            raise MachineNotRunnable(
                f"run is waiting for retry event {expected!r}, "
                f"received {received.name!r}"
            )
        claim = self._claim_event(received)
        committed = False
        try:
            self._check_cancelled()
            self._resume(received)
            outcome = self._enter(self.graph.node(self._active_node_id()), received)
            self._interpret(outcome)
            self._drain_next(outcome)
            self._complete_if_terminal()
            committed = True
            self._flush_emitted_events()
        except Exception:
            if not committed:
                self._discard_staged_effects()
                self._discard_emitted_events()
                self._release_event(claim)
                self.context.status = Status.FAILED
            raise
        self._hook(LifecycleEvent.AFTER_RESUME.value, event=received)
        return self.context

    async def resume_retry_async(
        self,
        payload: Any = None,
        *,
        event: Event | str | None = None,
    ) -> Context:
        self._ensure_waiting("retry")
        expected = (self.context.waiting or {}).get("resume_event") or "__retry__"
        received = (
            Event(expected, payload=payload, source=self.context.run_id)
            if event is None
            else self._event(event, payload)
        )
        if received.name != expected:
            raise MachineNotRunnable(
                f"run is waiting for retry event {expected!r}, "
                f"received {received.name!r}"
            )
        claim = self._claim_event(received)
        committed = False
        try:
            self._check_cancelled()
            await self._resume_async(received)
            outcome = await self._enter_async(
                self.graph.node(self._active_node_id()), received
            )
            await self._interpret_async(outcome)
            await self._drain_next_async(outcome)
            self._complete_if_terminal()
            committed = True
            await self._flush_emitted_events_async()
        except asyncio.CancelledError:
            if not committed:
                self._mark_task_cancelled()
                self._release_event(claim)
            raise
        except Exception:
            if not committed:
                self._discard_staged_effects()
                self._discard_emitted_events()
                self._release_event(claim)
                self.context.status = Status.FAILED
            raise
        try:
            await self._hook_async(
                LifecycleEvent.AFTER_RESUME.value,
                event=received,
            )
        except asyncio.CancelledError:
            self._mark_task_cancelled()
            raise
        return self.context

    def pause(self) -> None:
        self._require(Status.RUNNING)
        self.hooks.ensure_sync(LifecycleEvent.RUN_PAUSED.value)
        self.context.status = Status.PAUSED
        self._hook(LifecycleEvent.RUN_PAUSED.value)

    async def pause_async(self) -> None:
        """Pause a run whose lifecycle observers may be asynchronous."""
        self._require(Status.RUNNING)
        self.context.status = Status.PAUSED
        await self._hook_async(LifecycleEvent.RUN_PAUSED.value)

    def resume_run(self) -> Context:
        self._require(Status.PAUSED)
        self.hooks.ensure_sync(
            LifecycleEvent.RUN_RESUMED.value,
            LifecycleEvent.AFTER_RESUME.value,
        )
        self.context.status = Status.RUNNING
        self._hook(LifecycleEvent.RUN_RESUMED.value)
        self._hook(LifecycleEvent.AFTER_RESUME.value)
        return self.context

    async def resume_run_async(self) -> Context:
        """Resume a paused run whose lifecycle observers may be asynchronous."""
        self._require(Status.PAUSED)
        self.context.status = Status.RUNNING
        await self._hook_async(LifecycleEvent.RUN_RESUMED.value)
        await self._hook_async(LifecycleEvent.AFTER_RESUME.value)
        return self.context

    def join(self, run_id: Optional[str] = None) -> Optional[TransitionResult]:
        """Join 当前 Fork；传入 run_id 时递归 Join 指定的嵌套子运行。"""
        if run_id is None:
            result = self._forks.join()
            event = self._forks.last_join_event
        else:
            result = self._forks.join_child(run_id)
            event = None
        if result is None:
            self._hook(LifecycleEvent.AFTER_JOIN.value, event=event)
        return result

    async def join_async(self, run_id: Optional[str] = None) -> Optional[TransitionResult]:
        if run_id is None:
            result = await self._forks.join_async()
            event = self._forks.last_join_event
        else:
            result = await self._forks.join_child_async(run_id)
            event = None
        if result is None:
            await self._hook_async(LifecycleEvent.AFTER_JOIN.value, event=event)
        return result

    def route(
        self,
        run_id: str,
        event: Event | str,
        payload: Any = None,
    ) -> TransitionResult:
        """将事件路由到当前运行或任意嵌套子运行。"""
        if run_id == self.context.run_id:
            return self._route_local(event, payload)
        return self._forks.route(run_id, event, payload)

    async def route_async(
        self,
        run_id: str,
        event: Event | str,
        payload: Any = None,
    ) -> TransitionResult:
        """异步将事件路由到当前运行或任意嵌套子运行。"""
        if run_id == self.context.run_id:
            return await self._route_local_async(event, payload)
        return await self._forks.route_async(run_id, event, payload)

    def _route_local(
        self,
        event: Event | str,
        payload: Any = None,
    ) -> TransitionResult:
        received = self._event(event, payload)
        waiting_kind = (self.context.waiting or {}).get("kind")
        if waiting_kind == "fork":
            raise MachineNotRunnable("当前运行正在等待 Fork，请先 Join 子运行")
        if waiting_kind == "retry":
            raise MachineNotRunnable("当前运行正在等待 Retry，请使用 resume_retry()")
        if self.status is Status.WAITING:
            return self.resume(received)
        return self.dispatch(received)

    async def _route_local_async(
        self,
        event: Event | str,
        payload: Any = None,
    ) -> TransitionResult:
        received = self._event(event, payload)
        waiting_kind = (self.context.waiting or {}).get("kind")
        if waiting_kind == "fork":
            raise MachineNotRunnable("当前运行正在等待 Fork，请先 Join 子运行")
        if waiting_kind == "retry":
            raise MachineNotRunnable(
                "当前运行正在等待 Retry，请使用 resume_retry_async()"
            )
        if self.status is Status.WAITING:
            return await self.resume_async(received)
        return await self.dispatch_async(received)

    def snapshot(self) -> dict[str, Any]:
        """返回运行快照；存储介质由 persistence 模块负责。"""
        snapshot: dict[str, Any] = {
            "graph_id": self.graph.id,
            "graph_version": self.graph.version,
            "context": self.context.snapshot(),
        }
        fork = self._forks.snapshot()
        if fork is not None:
            snapshot["fork_group"] = fork
        return snapshot

    @classmethod
    def from_snapshot(
        cls,
        graph: Graph,
        snapshot: Any,
        **options: Any,
    ) -> "Machine":
        snapshot_graph_id = getattr(snapshot, "graph_id", None)
        if snapshot_graph_id is not None and snapshot_graph_id != graph.id:
            raise ValueError(
                f"snapshot graph_id {snapshot_graph_id!r} does not match graph {graph.id!r}"
            )
        if hasattr(snapshot, "to_machine_snapshot"):
            snapshot_graph_version = getattr(snapshot, "graph_version", None)
            if (
                snapshot_graph_version is not None
                and str(snapshot_graph_version) != graph.version
            ):
                raise ValueError(
                    f"snapshot graph_version {snapshot_graph_version!r} does not "
                    f"match graph {graph.version!r}"
                )
            snapshot = snapshot.to_machine_snapshot()
        snapshot_graph_id = snapshot.get("graph_id")
        if snapshot_graph_id is not None and snapshot_graph_id != graph.id:
            raise ValueError(
                f"snapshot graph_id {snapshot_graph_id!r} does not match graph {graph.id!r}"
            )
        snapshot_graph_version = snapshot.get("graph_version")
        if (
            snapshot_graph_version is not None
            and str(snapshot_graph_version) != graph.version
        ):
            raise ValueError(
                f"snapshot graph_version {snapshot_graph_version!r} does not "
                f"match graph {graph.version!r}"
            )
        context_snapshot = dict(snapshot["context"])
        context_snapshot.setdefault(
            "graph_version",
            str(snapshot_graph_version or "1"),
        )
        if context_snapshot.get("graph_id") != graph.id:
            raise ValueError(
                f"context graph_id {context_snapshot.get('graph_id')!r} does not match graph {graph.id!r}"
            )
        machine = cls(graph, context=Context.from_snapshot(context_snapshot), **options)
        fork = snapshot.get("fork_group")
        if fork is not None:
            machine._forks.restore(fork)
        return machine

    def _dispatch_event(
        self,
        event: Event,
        *,
        claimed: bool = False,
        transition: Optional[Transition] = None,
        on_commit: Optional[Callable[[], None]] = None,
        on_abort: Optional[Callable[[], None]] = None,
    ) -> TransitionResult:
        result = self._dispatch_event_once(
            event,
            claimed=claimed,
            transition=transition,
            on_commit=on_commit,
            on_abort=on_abort,
        )
        return self._drain_result(result)

    async def _dispatch_event_async(
        self,
        event: Event,
        *,
        claimed: bool = False,
        transition: Optional[Transition] = None,
        on_commit: Optional[Callable[[], None]] = None,
        on_abort: Optional[Callable[[], None]] = None,
    ) -> TransitionResult:
        result = await self._dispatch_event_once_async(
            event,
            claimed=claimed,
            transition=transition,
            on_commit=on_commit,
            on_abort=on_abort,
        )
        return await self._drain_result_async(result)

    def _dispatch_event_once(
        self,
        event: Event,
        *,
        claimed: bool = False,
        transition: Optional[Transition] = None,
        on_commit: Optional[Callable[[], None]] = None,
        on_abort: Optional[Callable[[], None]] = None,
    ) -> TransitionResult:
        claim = None if claimed else self._claim_event(event)
        committed = False
        try:
            transition = transition or self._resolve(event)
            self._hook(
                LifecycleEvent.BEFORE_DISPATCH.value,
                event=event,
                transition=transition,
            )
            result = self._transition(event, transition, on_abort=on_abort)
            committed = True
            if on_commit is not None:
                on_commit()
            self._flush_emitted_events()
            self._hook(
                LifecycleEvent.AFTER_TRANSITION.value,
                event=event,
                transition=transition,
                result=result,
            )
            self._hook(
                LifecycleEvent.AFTER_DISPATCH.value,
                event=event,
                transition=transition,
                result=result,
            )
            return result
        except BaseException:
            if not claimed and not committed:
                self._release_event(claim)
            raise

    async def _dispatch_event_once_async(
        self,
        event: Event,
        *,
        claimed: bool = False,
        transition: Optional[Transition] = None,
        on_commit: Optional[Callable[[], None]] = None,
        on_abort: Optional[Callable[[], None]] = None,
    ) -> TransitionResult:
        claim = None if claimed else self._claim_event(event)
        committed = False
        try:
            transition = transition or await self._resolve_async(event)
            await self._hook_async(
                LifecycleEvent.BEFORE_DISPATCH.value,
                event=event,
                transition=transition,
            )
            result = await self._transition_async(
                event,
                transition,
                on_abort=on_abort,
            )
            committed = True
            if on_commit is not None:
                on_commit()
            await self._flush_emitted_events_async()
            await self._hook_async(
                LifecycleEvent.AFTER_TRANSITION.value,
                event=event,
                transition=transition,
                result=result,
            )
            await self._hook_async(
                LifecycleEvent.AFTER_DISPATCH.value,
                event=event,
                transition=transition,
                result=result,
            )
            return result
        except asyncio.CancelledError:
            self._mark_task_cancelled()
            if not claimed and not committed:
                self._release_event(claim)
            raise
        except BaseException:
            if not claimed and not committed:
                self._release_event(claim)
            raise

    def _resolve(self, event: Event) -> Transition:
        source = self._active_node_id()
        transition = self.selector.select(self.graph, source, event, self.context)
        if transition is not None:
            return transition
        error = NoTransition(source, event.name)
        self._hook(LifecycleEvent.EVENT_UNHANDLED.value, event=event, error=error)
        raise error

    async def _resolve_async(self, event: Event) -> Transition:
        source = self._active_node_id()
        transition = await self.selector.select_async(
            self.graph,
            source,
            event,
            self.context,
        )
        if transition is not None:
            return transition
        error = NoTransition(source, event.name)
        await self._hook_async(
            LifecycleEvent.EVENT_UNHANDLED.value,
            event=event,
            error=error,
        )
        raise error

    def _transition(
        self,
        event: Event,
        transition: Transition,
        *,
        on_abort: Optional[Callable[[], None]] = None,
    ) -> TransitionResult:
        source = self._active_node_id()
        frame = _TransitionFrame(source, source, transition.target or source)
        self.context.last_event = event
        self._hook(
            LifecycleEvent.BEFORE_TRANSITION.value,
            event=event,
            transition=transition,
        )
        try:
            outcome = self._exit(self.graph.node(source), event)
            self._accept_stage(frame, outcome)
            if not frame.stopped and transition.action is not None:
                transition_outcome = self._as_outcome(
                    self.action_executor.execute(transition.action, self.context, event)
                )
                self._accept_stage(frame, transition_outcome)
            if not frame.stopped:
                frame.target = frame.declared_target
                self.context.node_id = frame.target
                enter_outcome = self._enter(self.graph.node(frame.target), event)
                self._accept_stage(frame, enter_outcome)
            if not frame.stopped:
                self.context.attempt = 0
                self._complete_if_terminal()
        except Exception as error:
            self._discard_staged_effects()
            self._discard_emitted_events()
            self.context.status = Status.FAILED
            if on_abort is not None:
                on_abort()
            self._hook(
                LifecycleEvent.TRANSITION_ERROR.value,
                event=event,
                transition=transition,
                error=error,
            )
            raise

        return self._transition_result(event, transition, frame)

    async def _transition_async(
        self,
        event: Event,
        transition: Transition,
        *,
        on_abort: Optional[Callable[[], None]] = None,
    ) -> TransitionResult:
        source = self._active_node_id()
        frame = _TransitionFrame(source, source, transition.target or source)
        self.context.last_event = event
        try:
            await self._hook_async(
                LifecycleEvent.BEFORE_TRANSITION.value,
                event=event,
                transition=transition,
            )
        except asyncio.CancelledError:
            self._mark_task_cancelled()
            raise
        try:
            outcome = await self._exit_async(self.graph.node(source), event)
            await self._accept_stage_async(frame, outcome)
            if not frame.stopped and transition.action is not None:
                transition_outcome = self._as_outcome(
                    await self.action_executor.execute_async(
                        transition.action, self.context, event
                    )
                )
                await self._accept_stage_async(frame, transition_outcome)
            if not frame.stopped:
                frame.target = frame.declared_target
                self.context.node_id = frame.target
                enter_outcome = await self._enter_async(
                    self.graph.node(frame.target), event
                )
                await self._accept_stage_async(frame, enter_outcome)
            if not frame.stopped:
                self.context.attempt = 0
                self._complete_if_terminal()
        except asyncio.CancelledError:
            self._mark_task_cancelled()
            raise
        except Exception as error:
            self._discard_staged_effects()
            self._discard_emitted_events()
            self.context.status = Status.FAILED
            if on_abort is not None:
                on_abort()
            await self._hook_async(
                LifecycleEvent.TRANSITION_ERROR.value,
                event=event,
                transition=transition,
                error=error,
            )
            raise

        return self._transition_result(event, transition, frame)

    def _enter(self, node: BaseNode, event: Event) -> Optional[Outcome]:
        self._hook(LifecycleEvent.NODE_ENTER.value, event=event)
        try:
            value = node.enter(self.context, event, self.action_executor)
        except Exception as error:
            retry = self._retry_for_error(error)
            if retry is None:
                raise
            return retry
        return self._as_outcome(value)

    async def _enter_async(self, node: BaseNode, event: Event) -> Optional[Outcome]:
        await self._hook_async(LifecycleEvent.NODE_ENTER.value, event=event)
        try:
            # 兼容只覆盖同步 enter() 的自定义节点；内置节点提供了自己的
            # enter_async()，仍然走异步 Action 执行路径。
            if (
                type(node).enter_async is BaseNode.enter_async
                and type(node).enter is not BaseNode.enter
            ):
                value = node.enter(self.context, event, self.action_executor)
                if inspect.isawaitable(value):
                    value = await value
            else:
                value = await node.enter_async(
                    self.context,
                    event,
                    self.action_executor,
                )
        except Exception as error:
            retry = self._retry_for_error(error)
            if retry is None:
                raise
            return retry
        return self._as_outcome(value)

    def _exit(self, node: BaseNode, event: Event) -> Optional[Outcome]:
        self._hook(LifecycleEvent.NODE_EXIT.value, event=event)
        return self._as_outcome(node.exit(self.context, event, self.action_executor))

    async def _exit_async(self, node: BaseNode, event: Event) -> Optional[Outcome]:
        await self._hook_async(LifecycleEvent.NODE_EXIT.value, event=event)
        if (
            type(node).exit_async is BaseNode.exit_async
            and type(node).exit is not BaseNode.exit
        ):
            value = node.exit(self.context, event, self.action_executor)
            if inspect.isawaitable(value):
                value = await value
        else:
            value = await node.exit_async(
                self.context,
                event,
                self.action_executor,
            )
        return self._as_outcome(value)

    def _interpret(self, outcome: Optional[Outcome]) -> OutcomeDirective:
        if outcome is None:
            self.context.attempt = 0
            return OutcomeDirective.CONTINUE
        return self.outcome_interpreter.apply(self._outcome_runtime, outcome)

    async def _interpret_async(
        self, outcome: Optional[Outcome]
    ) -> OutcomeDirective:
        if outcome is None:
            self.context.attempt = 0
            return OutcomeDirective.CONTINUE
        return await self.outcome_interpreter.apply_async(
            self._outcome_runtime, outcome
        )

    def _accept_stage(
        self, frame: _TransitionFrame, outcome: Optional[Outcome]
    ) -> None:
        directive = (
            OutcomeDirective.CONTINUE
            if outcome is None
            else self._interpret(outcome)
        )
        frame.accept(outcome, directive)

    async def _accept_stage_async(
        self, frame: _TransitionFrame, outcome: Optional[Outcome]
    ) -> None:
        directive = (
            OutcomeDirective.CONTINUE
            if outcome is None
            else await self._interpret_async(outcome)
        )
        frame.accept(outcome, directive)

    def _transition_result(
        self,
        event: Event,
        transition: Transition,
        frame: _TransitionFrame,
    ) -> TransitionResult:
        group = self.fork_group
        children = group.children if group is not None else ()
        return TransitionResult(
            event,
            frame.source,
            frame.target,
            transition,
            frame.outcome,
            self.status,
            children,
        )

    def _drain_next(self, outcome: Optional[Outcome]) -> None:
        steps = 0
        while isinstance(outcome, Next):
            if self.status is not Status.RUNNING:
                return
            steps = self._next_step(steps)
            event = Event(outcome.event, outcome.payload, self.context.run_id)
            outcome = self._dispatch_event_once(event).outcome

    async def _drain_next_async(self, outcome: Optional[Outcome]) -> None:
        steps = 0
        while isinstance(outcome, Next):
            if self.status is not Status.RUNNING:
                return
            steps = self._next_step(steps)
            event = Event(outcome.event, outcome.payload, self.context.run_id)
            outcome = (await self._dispatch_event_once_async(event)).outcome

    def _drain_result(self, result: TransitionResult) -> TransitionResult:
        steps = 0
        while isinstance(result.outcome, Next):
            if self.status is not Status.RUNNING:
                return result
            steps = self._next_step(steps)
            outcome = result.outcome
            result = self._dispatch_event_once(
                Event(outcome.event, outcome.payload, self.context.run_id)
            )
        return result

    async def _drain_result_async(self, result: TransitionResult) -> TransitionResult:
        steps = 0
        while isinstance(result.outcome, Next):
            if self.status is not Status.RUNNING:
                return result
            steps = self._next_step(steps)
            outcome = result.outcome
            result = await self._dispatch_event_once_async(
                Event(outcome.event, outcome.payload, self.context.run_id)
            )
        return result

    def _next_step(self, steps: int) -> int:
        if steps >= self.max_internal_steps:
            self.context.status = Status.FAILED
            raise InternalStepLimitExceeded(
                f"内部 Next 事件超过 {self.max_internal_steps} 步"
            )
        return steps + 1

    def _complete_if_terminal(self) -> None:
        node = self.graph.node(self._active_node_id())
        if node.terminal and self.status is Status.RUNNING:
            self.context.status = Status.COMPLETED

    def _ensure_waiting(self, kind: str) -> None:
        if self.status is not Status.WAITING:
            raise MachineNotRunnable(f"run is not waiting: {self.status.value}")
        actual = (self.context.waiting or {}).get("kind")
        if actual == "fork":
            raise MachineNotRunnable("Fork 等待请调用 join()")
        if kind == "retry" and actual != "retry":
            raise MachineNotRunnable("run is not waiting for a retry")
        if kind == "event" and actual == "retry":
            raise MachineNotRunnable("重试等待请调用 resume_retry()")

    def _active_node_id(self) -> str:
        node_id = self.context.node_id
        if node_id is None:
            raise MachineNotStarted("run has no active node")
        return node_id

    def _resume(self, event: Optional[Event] = None) -> None:
        self.context.waiting = None
        self.context.status = Status.RUNNING
        self._hook(LifecycleEvent.RUN_RESUMED.value, event=event)

    async def _resume_async(self, event: Optional[Event] = None) -> None:
        self.context.waiting = None
        self.context.status = Status.RUNNING
        await self._hook_async(LifecycleEvent.RUN_RESUMED.value, event=event)

    def _require(self, status: Status) -> None:
        if self.status is not status:
            raise MachineNotRunnable(
                f"run requires {status.value}, current status is {self.status.value}"
            )

    def _ensure_runnable(self) -> None:
        if self.status is Status.CREATED:
            raise MachineNotStarted("call start() before dispatching events")
        if self.status is not Status.RUNNING:
            raise MachineNotRunnable(f"run is not runnable: {self.status.value}")
        self._check_cancelled()

    def _check_cancelled(self) -> None:
        if self.cancellation is not None:
            self.cancellation.raise_if_cancelled()

    def _retry_for_error(self, error: Exception) -> Optional[Retry]:
        """把显式允许的节点异常接入已有 Retry 生命周期。"""
        # 取消是运行控制信号，不能被配置为普通业务异常后吞成 Retry。
        if isinstance(error, CancellationError) or not self.retry_policy.matches(
            error
        ):
            return None
        return Retry(
            reason={
                "type": type(error).__name__,
                "message": str(error),
            }
        )

    def _mark_task_cancelled(self) -> None:
        self._discard_staged_effects()
        if self.status not in {Status.COMPLETED, Status.FAILED, Status.STOPPED}:
            self.context.status = Status.STOPPED
            self.context.metadata["stop_reason"] = "task_cancelled"

    def _event_key(self, event: Event) -> str:
        """将事件键限定在运行实例内，避免不同运行共享事件对象时互相影响。"""
        return f"{self.context.run_id}:{event.event_id}"

    def _stage_effect(self, effect: StagedEffect) -> None:
        existing = self._staged_effects.get(effect.id)
        if existing is not None and existing != effect:
            raise ValueError(f"staged effect id already exists: {effect.id!r}")
        self._staged_effects[effect.id] = effect

    def _defer_emitted_event(self, event: Event) -> None:
        """Stage Outcome.emit delivery until the current state change commits."""
        self.context.attempt = 0
        self._deferred_emitted_events.append(event)

    def _flush_emitted_events(self) -> None:
        while self._deferred_emitted_events:
            event = self._deferred_emitted_events.pop(0)
            self._record_runtime_event(
                LifecycleEvent.EVENT_EMITTED.value,
                event=event,
            )
            self.events.publish(event)

    async def _flush_emitted_events_async(self) -> None:
        while self._deferred_emitted_events:
            event = self._deferred_emitted_events.pop(0)
            self._record_runtime_event(
                LifecycleEvent.EVENT_EMITTED.value,
                event=event,
            )
            await self.events.publish_async(event)

    def _discard_emitted_events(self) -> None:
        self._deferred_emitted_events.clear()

    def _ack_staged_effects(self, effect_ids: set[str]) -> None:
        for effect_id in effect_ids:
            self._staged_effects.pop(effect_id, None)

    def _discard_staged_effects(self) -> None:
        self._staged_effects.clear()

    def _consume(
        self, item: Event | str | tuple[str, Any]
    ) -> TransitionResult:
        event, payload = self._split_input(item)
        waiting_kind = (self.context.waiting or {}).get("kind")
        if self.status is Status.WAITING and waiting_kind != "retry":
            return self.resume(event, payload)
        return self.dispatch(event, payload)

    async def _consume_async(
        self, item: Event | str | tuple[str, Any]
    ) -> TransitionResult:
        event, payload = self._split_input(item)
        waiting_kind = (self.context.waiting or {}).get("kind")
        if self.status is Status.WAITING and waiting_kind != "retry":
            return await self.resume_async(event, payload)
        return await self.dispatch_async(event, payload)

    @staticmethod
    def _split_input(
        item: Event | str | tuple[str, Any]
    ) -> tuple[Event | str, Any]:
        if isinstance(item, tuple):
            if len(item) != 2:
                raise ValueError("event tuple must contain (name, payload)")
            return item
        return item, None

    @staticmethod
    def _normalize_context_update(
        values: Mapping[str, Any] | None,
        kwargs: Mapping[str, Any],
    ) -> dict[str, Any]:
        if values is not None and not isinstance(values, Mapping):
            raise TypeError("context update values must be a mapping")
        updates = dict(values or {})
        updates.update(kwargs)
        return updates

    def _claim_event(self, event: Event) -> Optional[str]:
        if self.idempotency is None:
            return None
        key = self._event_key(event)
        if not self.idempotency.claim(key):
            raise DuplicateEvent(key)
        return key

    def _release_event(self, key: Optional[str]) -> None:
        if key is not None and self.idempotency is not None:
            self.idempotency.release(key)

    def _hook(self, name: str, **kwargs) -> None:
        runtime_event = self._record_runtime_event(name, **kwargs)
        self.hooks.emit(
            name,
            HookContext(
                name=name,
                machine=self,
                context=self.context,
                runtime_event=runtime_event,
                **kwargs,
            ),
        )

    async def _hook_async(self, name: str, **kwargs) -> None:
        runtime_event = self._record_runtime_event(name, **kwargs)
        await self.hooks.emit_async(
            name,
            HookContext(
                name=name,
                machine=self,
                context=self.context,
                runtime_event=runtime_event,
                **kwargs,
            ),
        )

    def _record_runtime_event(self, name: str, **kwargs: Any) -> RuntimeEvent:
        observer = self._runtime_observer
        self._runtime_sequence += 1
        self.context.metadata["runtime_sequence"] = self._runtime_sequence
        event = kwargs.get("event")
        transition = kwargs.get("transition")
        error = kwargs.get("error")
        result = kwargs.get("result")
        outcome = getattr(result, "outcome", None)
        runtime_event = RuntimeEvent(
            name=name,
            run_id=self.context.run_id,
            graph_id=self.graph.id,
            sequence=self._runtime_sequence,
            parent_run_id=self.context.metadata.get("parent_run_id"),
            node_id=self.context.node_id,
            status=self.context.status.value,
            event_name=getattr(event, "name", None),
            event_id=getattr(event, "event_id", None),
            event_source=getattr(event, "source", None),
            target_run_id=getattr(event, "target_run_id", None),
            payload=getattr(event, "payload", None),
            transition_id=getattr(transition, "id", None),
            outcome=type(outcome).__name__ if outcome is not None else None,
            error_type=type(error).__name__ if error is not None else None,
            error_message=str(error) if error is not None else None,
        )
        self._last_runtime_event = runtime_event
        if observer is None:
            return runtime_event
        try:
            observer(runtime_event)
        except Exception:
            # 观测器不能改变图执行语义；需要失败隔离时应使用 Hook。
            pass
        return runtime_event

    @staticmethod
    def _as_outcome(value: Any) -> Optional[Outcome]:
        if isinstance(value, Outcome):
            return value
        if isinstance(value, Mapping):
            return Update(dict(value))
        return None

    @staticmethod
    def _event(event: Event | str, payload: Any = None) -> Event:
        return event if isinstance(event, Event) else Event(event, payload)
