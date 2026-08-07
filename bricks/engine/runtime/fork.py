"""Fork/Join 的运行时实现。

Fork 是可选的组合能力。它依赖 Machine，但不应该把子运行管理逻辑塞进
Machine 本身。
"""

from __future__ import annotations

import asyncio
import copy
import uuid
from dataclasses import dataclass
from typing import Any, TYPE_CHECKING, Callable, Optional, Protocol, cast

from ..errors import MachineNotRunnable, PersistenceError
from ..events.messages import Event
from ..runtime.context import Context, ROOT_RUN_ID_METADATA
from ..runtime.lifecycle import Status
from .outcomes import Fork, ForkBranch

if TYPE_CHECKING:
    from .machine import Machine, TransitionResult


_TERMINAL = frozenset({Status.COMPLETED, Status.FAILED, Status.STOPPED})


class ForkRuntime(Protocol):
    """Replaceable boundary for local or externally coordinated Fork semantics."""

    group: Optional["ForkGroup"]
    last_join_event: Optional[Event]

    def start(self, outcome: Fork) -> "ForkGroup": ...
    async def start_async(self, outcome: Fork) -> "ForkGroup": ...
    def join(self) -> Optional["TransitionResult"]: ...
    async def join_async(self) -> Optional["TransitionResult"]: ...
    def join_child(self, run_id: str) -> Optional["TransitionResult"]: ...
    async def join_child_async(
        self, run_id: str
    ) -> Optional["TransitionResult"]: ...
    def route(
        self, run_id: str, event: "Event | str", payload: object = None
    ) -> "TransitionResult": ...
    async def route_async(
        self, run_id: str, event: "Event | str", payload: object = None
    ) -> "TransitionResult": ...
    def snapshot(self) -> Optional[dict]: ...
    def restore(self, snapshot: dict) -> "ForkGroup": ...


ForkRuntimeFactory = Callable[["Machine"], ForkRuntime]


@dataclass
class ForkGroup:
    """一次 Fork 创建的子运行集合。"""

    id: str
    parent: "Machine"
    children: tuple["Machine", ...]
    join_event: Optional[str]
    policy: str
    failure_policy: str = "fail"
    max_concurrency: Optional[int] = None
    kind: str = "fork"
    joined: bool = False

    @property
    def finished(self) -> bool:
        if self.failure_policy == "fail_fast" and self.failed:
            return True
        if self.policy == "any":
            # any 表示任一分支成功；失败分支不能阻止其它分支继续竞争。
            return any(child.status is Status.COMPLETED for child in self.children) or all(
                child.status in _TERMINAL for child in self.children
            )
        return all(child.status in _TERMINAL for child in self.children)

    @property
    def successful(self) -> bool:
        if self.failure_policy == "continue":
            return self.finished
        if self.policy == "any":
            return any(child.status is Status.COMPLETED for child in self.children)
        return all(child.status is Status.COMPLETED for child in self.children)

    @property
    def failed(self) -> bool:
        """判断是否已有分支进入失败状态。"""
        return any(child.status is Status.FAILED for child in self.children)

    def contains(self, run_id: str) -> bool:
        """判断当前分支树是否包含运行 ID。"""
        return any(
            child.context.run_id == run_id
            or (child.fork_group is not None and child.fork_group.contains(run_id))
            for child in self.children
        )

    def find(self, run_id: str) -> Optional["Machine"]:
        """递归查找运行实例本身，而不是只判断它是否存在。"""
        for child in self.children:
            if child.context.run_id == run_id:
                return child
            if child.fork_group is not None:
                found = child.fork_group.find(run_id)
                if found is not None:
                    return found
        return None

    def route(
        self,
        run_id: str,
        event: "Event | str",
        payload: object = None,
    ) -> "TransitionResult":
        """将事件递归路由到指定子运行。"""
        for child in self.children:
            if child.context.run_id == run_id:
                return child._route_local(event, payload)
            if child.fork_group is not None and child.fork_group.contains(run_id):
                return child.route(run_id, event, payload)
        raise MachineNotRunnable(f"Fork 中不存在子运行 {run_id!r}")

    async def route_async(
        self,
        run_id: str,
        event: "Event | str",
        payload: object = None,
    ) -> "TransitionResult":
        """异步递归路由事件到指定子运行。"""
        for child in self.children:
            if child.context.run_id == run_id:
                return await child._route_local_async(event, payload)
            if child.fork_group is not None and child.fork_group.contains(run_id):
                return await child.route_async(run_id, event, payload)
        raise MachineNotRunnable(f"Fork 中不存在子运行 {run_id!r}")


class ForkController:
    """为一个 Machine 管理 Fork 子运行和 Join。"""

    def __init__(self, parent: "Machine"):
        self.parent = parent
        self.group: Optional[ForkGroup] = None
        self.last_join_event: Optional[Event] = None

    def start(self, outcome: Fork) -> ForkGroup:
        if self.group is not None:
            raise MachineNotRunnable("当前运行已经存在待 Join 的 Fork")

        children: list["Machine"] = []
        for index, branch in enumerate(outcome.branches):
            child = self._create_child(branch)
            children.append(child)
            if outcome.failure_policy == "fail_fast" and child.status is Status.FAILED:
                children.extend(
                    self._stopped_child(remaining, "fork_fail_fast")
                    for remaining in outcome.branches[index + 1 :]
                )
                break
        self.group = self._mark_waiting(
            ForkGroup(
                id=uuid.uuid4().hex,
                parent=self.parent,
                children=tuple(children),
                join_event=outcome.join_event,
                policy=outcome.policy,
                failure_policy=outcome.failure_policy,
                max_concurrency=outcome.max_concurrency,
                kind=outcome.kind,
            )
        )
        return self.group

    async def start_async(self, outcome: Fork) -> ForkGroup:
        if self.group is not None:
            raise MachineNotRunnable("当前运行已经存在待 Join 的 Fork")

        limit = outcome.max_concurrency or max(len(outcome.branches), 1)
        if outcome.failure_policy == "fail_fast":
            children = list(await self._create_children_fail_fast(outcome, limit))
            self.group = self._mark_waiting(
                ForkGroup(
                    id=uuid.uuid4().hex,
                    parent=self.parent,
                    children=tuple(children),
                    join_event=outcome.join_event,
                    policy=outcome.policy,
                    failure_policy=outcome.failure_policy,
                    max_concurrency=outcome.max_concurrency,
                    kind=outcome.kind,
                )
            )
            return self.group

        semaphore = asyncio.Semaphore(limit)

        async def run_branch(branch: ForkBranch) -> "Machine":
            async with semaphore:
                return await self._create_child_async(branch)

        tasks = [asyncio.create_task(run_branch(branch)) for branch in outcome.branches]
        aggregate = asyncio.gather(*tasks)
        cancellation_waiter = None
        try:
            if self.parent.cancellation is None:
                children = await aggregate
            else:
                cancellation_waiter = asyncio.create_task(
                    self.parent.cancellation.wait_async()
                )
                wait_for: set[asyncio.Future[Any]] = {aggregate, cancellation_waiter}
                done, _ = await asyncio.wait(
                    wait_for,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if cancellation_waiter in done and aggregate not in done:
                    self._cancel_tasks(tasks)
                    await asyncio.gather(*tasks, return_exceptions=True)
                    try:
                        await aggregate
                    except BaseException:
                        pass
                    self.parent._check_cancelled()
                children = aggregate.result()
        except BaseException:
            self._cancel_tasks(tasks)
            await asyncio.gather(*tasks, return_exceptions=True)
            raise
        finally:
            if cancellation_waiter is not None and not cancellation_waiter.done():
                cancellation_waiter.cancel()
                await asyncio.gather(cancellation_waiter, return_exceptions=True)
        self.group = self._mark_waiting(
            ForkGroup(
                id=uuid.uuid4().hex,
                parent=self.parent,
                children=tuple(children),
                join_event=outcome.join_event,
                policy=outcome.policy,
                failure_policy=outcome.failure_policy,
                max_concurrency=outcome.max_concurrency,
                kind=outcome.kind,
            )
        )
        return self.group

    async def _create_children_fail_fast(
        self,
        outcome: Fork,
        limit: int,
    ) -> tuple["Machine", ...]:
        """并发启动分支，并在首个失败分支完成时取消未完成兄弟。"""
        semaphore = asyncio.Semaphore(limit)
        created: dict[int, "Machine"] = {}

        async def run_branch(index: int, branch: ForkBranch) -> "Machine":
            async with semaphore:
                child = self._new_child(branch)
                created[index] = child
                try:
                    await child.start_async()
                    if child.status is Status.RUNNING and branch.event is not None:
                        await child.dispatch_async(branch.event, branch.payload)
                    self._ensure_child_finished_or_waiting(child)
                    return child
                except asyncio.CancelledError:
                    if child.status not in _TERMINAL:
                        if child.context.node_id is None:
                            child.context.node_id = child.graph.initial
                        child.context.waiting = None
                        child.context.status = Status.STOPPED
                        child.context.metadata["stop_reason"] = "fork_fail_fast"
                    raise

        tasks = {
            asyncio.create_task(run_branch(index, branch)): index
            for index, branch in enumerate(outcome.branches)
        }
        pending = set(tasks)
        results: dict[int, "Machine"] = {}
        cancellation_waiter = None
        try:
            if self.parent.cancellation is not None:
                cancellation_waiter = asyncio.create_task(
                    self.parent.cancellation.wait_async()
                )
            while pending:
                wait_for: set[asyncio.Task[Any]] = set(pending)
                if cancellation_waiter is not None:
                    wait_for.add(cancellation_waiter)
                done, _ = await asyncio.wait(
                    wait_for,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                pending.difference_update(done)
                if cancellation_waiter is not None and cancellation_waiter in done:
                    self._cancel_tasks(list(pending))
                    await asyncio.gather(*pending, return_exceptions=True)
                    self.parent._check_cancelled()

                failed = False
                for task in done:
                    if task is cancellation_waiter:
                        continue
                    if task.cancelled():
                        continue
                    error = task.exception()
                    if error is not None:
                        self._cancel_tasks(list(pending))
                        await asyncio.gather(*pending, return_exceptions=True)
                        raise error
                    branch_task = cast("asyncio.Task[Machine]", task)
                    child = branch_task.result()
                    results[tasks[branch_task]] = child
                    failed = failed or child.status is Status.FAILED

                if failed:
                    self._cancel_tasks(list(pending))
                    await asyncio.gather(*pending, return_exceptions=True)
                    break

            children = []
            for index, branch in enumerate(outcome.branches):
                branch_child = results.get(index) or created.get(index)
                if branch_child is None:
                    branch_child = self._new_child(branch)
                if branch_child.status not in _TERMINAL:
                    if branch_child.context.node_id is None:
                        branch_child.context.node_id = branch_child.graph.initial
                    branch_child.context.waiting = None
                    branch_child.context.status = Status.STOPPED
                    branch_child.context.metadata["stop_reason"] = "fork_fail_fast"
                children.append(branch_child)
            return tuple(children)
        except BaseException:
            self._cancel_tasks(list(pending))
            await asyncio.gather(*pending, return_exceptions=True)
            raise
        finally:
            if cancellation_waiter is not None and not cancellation_waiter.done():
                cancellation_waiter.cancel()
                await asyncio.gather(cancellation_waiter, return_exceptions=True)

    def join(self) -> Optional["TransitionResult"]:
        self.last_join_event = None
        group = self._ready_group()
        self.parent._check_cancelled()
        if not group.successful:
            self._finish_group(group)
            self.parent.context.status = Status.FAILED
            self.parent.context.metadata["join_failure"] = group.id
            return None
        if group.join_event is None:
            self._finish_group(group)
            self.parent.context.status = Status.RUNNING
            self.parent._complete_if_terminal()
            self.last_join_event = Event(
                "__join__",
                {
                    "fork_id": group.id,
                    "children": [
                        child.context.snapshot() for child in group.children
                    ],
                },
                self.parent.context.run_id,
            )
            return None
        payload = self._join_payload(group)
        snapshot = self.parent.context.snapshot()
        self._prepare_join_dispatch(group)
        committed = [False]
        aborted = [False]

        def finish() -> None:
            committed[0] = True
            self._finish_group(group, merge_data=False)

        def restore() -> None:
            aborted[0] = True
            self._restore_join(group, snapshot)

        try:
            return self.parent._dispatch_event(
                self.parent._event(group.join_event, payload),
                on_commit=finish,
                on_abort=restore,
            )
        except BaseException:
            if not committed[0] and not aborted[0]:
                self._restore_join(group, snapshot)
            raise

    def route(
        self,
        run_id: str,
        event: "Event | str",
        payload: object = None,
    ) -> "TransitionResult":
        group = self.group
        if group is None:
            raise MachineNotRunnable("当前运行没有待 Join 的 Fork")
        return group.route(run_id, event, payload)

    async def join_async(self) -> Optional["TransitionResult"]:
        self.last_join_event = None
        group = self._ready_group()
        self.parent._check_cancelled()
        if not group.successful:
            self._finish_group(group)
            self.parent.context.status = Status.FAILED
            self.parent.context.metadata["join_failure"] = group.id
            return None
        if group.join_event is None:
            self._finish_group(group)
            self.parent.context.status = Status.RUNNING
            self.parent._complete_if_terminal()
            self.last_join_event = Event(
                "__join__",
                {
                    "fork_id": group.id,
                    "children": [
                        child.context.snapshot() for child in group.children
                    ],
                },
                self.parent.context.run_id,
            )
            return None
        payload = self._join_payload(group)
        snapshot = self.parent.context.snapshot()
        self._prepare_join_dispatch(group)
        committed = [False]
        aborted = [False]

        def finish() -> None:
            committed[0] = True
            self._finish_group(group, merge_data=False)

        def restore() -> None:
            aborted[0] = True
            self._restore_join(group, snapshot)

        try:
            return await self.parent._dispatch_event_async(
                self.parent._event(group.join_event, payload),
                on_commit=finish,
                on_abort=restore,
            )
        except BaseException:
            if not committed[0] and not aborted[0]:
                self._restore_join(group, snapshot)
            raise

    async def route_async(
        self,
        run_id: str,
        event: "Event | str",
        payload: object = None,
    ) -> "TransitionResult":
        group = self.group
        if group is None:
            raise MachineNotRunnable("当前运行没有待 Join 的 Fork")
        return await group.route_async(run_id, event, payload)

    def join_child(self, run_id: str) -> Optional["TransitionResult"]:
        group = self.group
        if group is None or not group.contains(run_id):
            raise MachineNotRunnable(f"Fork 中不存在子运行 {run_id!r}")
        child = group.find(run_id)
        if child is None:
            raise MachineNotRunnable(f"Fork 中不存在子运行 {run_id!r}")
        return child.join()

    async def join_child_async(self, run_id: str) -> Optional["TransitionResult"]:
        group = self.group
        if group is None or not group.contains(run_id):
            raise MachineNotRunnable(f"Fork 中不存在子运行 {run_id!r}")
        child = group.find(run_id)
        if child is None:
            raise MachineNotRunnable(f"Fork 中不存在子运行 {run_id!r}")
        return await child.join_async()

    def snapshot(self) -> Optional[dict]:
        if self.group is None:
            return None
        group = self.group
        return {
            "id": group.id,
            "join_event": group.join_event,
            "policy": group.policy,
            "failure_policy": group.failure_policy,
            "max_concurrency": group.max_concurrency,
            "kind": group.kind,
            "joined": group.joined,
            "children": [child.snapshot() for child in group.children],
        }

    def restore(self, snapshot: dict) -> ForkGroup:
        if self.group is not None:
            raise MachineNotRunnable("当前运行已经存在待 Join 的 Fork")
        children = tuple(
            self.parent.__class__.from_snapshot(
                self._child_graph(child_snapshot),
                child_snapshot,
                executor=self.parent.executor,
                events=self.parent.events,
                hooks=self.parent.hooks,
                retry_policy=self.parent.retry_policy,
                cancellation=self.parent.cancellation,
                timeout=self.parent.timeout,
                idempotency=self.parent.idempotency,
                graph_resolver=self.parent.graph_resolver,
                selector=self.parent.selector,
                outcome_interpreter=self.parent.outcome_interpreter,
                fork_runtime_factory=self.parent.fork_runtime_factory,
                clock=self.parent.clock,
                max_internal_steps=self.parent.max_internal_steps,
                _runtime_observer=self.parent._runtime_observer,
            )
            for child_snapshot in snapshot.get("children", ())
        )
        self.group = ForkGroup(
            id=snapshot["id"],
            parent=self.parent,
            children=children,
            join_event=snapshot.get("join_event"),
            policy=snapshot.get("policy", "all"),
            failure_policy=snapshot.get("failure_policy", "fail"),
            max_concurrency=snapshot.get("max_concurrency"),
            kind=snapshot.get("kind", "fork"),
            joined=bool(snapshot.get("joined", False)),
        )
        return self.group

    def _create_child(self, branch: ForkBranch) -> "Machine":
        child = self._new_child(branch)
        child.start()
        if child.status is Status.RUNNING and branch.event is not None:
            child.dispatch(branch.event, branch.payload)
        self._ensure_child_finished_or_waiting(child)
        return child

    async def _create_child_async(
        self,
        branch: ForkBranch,
    ) -> "Machine":
        child = self._new_child(branch)
        try:
            await child.start_async()
            if child.status is Status.RUNNING and branch.event is not None:
                await child.dispatch_async(branch.event, branch.payload)
            self._ensure_child_finished_or_waiting(child)
            return child
        except asyncio.CancelledError:
            if child.status not in _TERMINAL:
                if child.context.node_id is None:
                    child.context.node_id = child.graph.initial
                child.context.waiting = None
                child.context.status = Status.STOPPED
                child.context.metadata["stop_reason"] = "fork_cancelled"
            raise

    def _new_child(self, branch: ForkBranch) -> "Machine":
        data = copy.deepcopy(self.parent.context.data)
        data.update(copy.deepcopy(dict(branch.data)))
        graph = branch.graph or self.parent.graph
        metadata = {
            "parent_run_id": self.parent.context.run_id,
            # 让父级绑定在 ForkGroup 注册前也能识别正在启动的后代运行。
            ROOT_RUN_ID_METADATA: self.parent.context.metadata.get(
                ROOT_RUN_ID_METADATA,
                self.parent.context.run_id,
            ),
        }
        return self.parent.__class__(
            graph,
            context=Context(
                graph_id=graph.id,
                graph_version=graph.version,
                data=data,
                metadata=metadata,
            ),
            executor=self.parent.executor,
            events=self.parent.events,
            hooks=self.parent.hooks,
            retry_policy=self.parent.retry_policy,
            cancellation=self.parent.cancellation,
            timeout=self.parent.timeout,
            idempotency=self.parent.idempotency,
            graph_resolver=self.parent.graph_resolver,
            selector=self.parent.selector,
            outcome_interpreter=self.parent.outcome_interpreter,
            fork_runtime_factory=self.parent.fork_runtime_factory,
            clock=self.parent.clock,
            max_internal_steps=self.parent.max_internal_steps,
            _runtime_observer=self.parent._runtime_observer,
        )

    def _stopped_child(
        self,
        branch: ForkBranch,
        reason: str,
    ) -> "Machine":
        child = self._new_child(branch)
        child.context.node_id = child.graph.initial
        child.context.status = Status.STOPPED
        child.context.waiting = None
        child.context.metadata["stop_reason"] = reason
        return child

    def _child_graph(self, snapshot: dict):
        graph_id = snapshot.get("graph_id")
        if graph_id == self.parent.graph.id:
            return self.parent.graph
        if self.parent.graph_resolver is None:
            raise PersistenceError(
                f"no graph resolver is configured for subgraph {graph_id!r}"
            )
        if not isinstance(graph_id, str):
            raise PersistenceError("child snapshot graph_id must be a string")
        return self.parent.graph_resolver(graph_id)

    @staticmethod
    def _ensure_child_finished_or_waiting(child: "Machine") -> None:
        if child.status not in _TERMINAL | {Status.WAITING, Status.PAUSED}:
            raise MachineNotRunnable(
                f"Fork 分支在初始事件后处于未知状态: {child.status.value}"
            )

    def _mark_waiting(self, group: ForkGroup) -> ForkGroup:
        self.parent.context.waiting = {
            "kind": "fork",
            "fork_id": group.id,
            "policy": group.policy,
            "failure_policy": group.failure_policy,
            "max_concurrency": group.max_concurrency,
            "children": [child.context.run_id for child in group.children],
        }
        self.parent.context.metadata["fork"] = {
            "id": group.id,
            "policy": group.policy,
            "max_concurrency": group.max_concurrency,
            "children": [child.context.run_id for child in group.children],
        }
        self.parent.context.status = Status.WAITING
        return group

    @staticmethod
    def _cancel_tasks(tasks: list["asyncio.Task[Machine]"]) -> None:
        for task in tasks:
            if not task.done():
                task.cancel()

    def _ready_group(self) -> ForkGroup:
        group = self.group
        if group is None:
            raise MachineNotRunnable("当前运行没有待 Join 的 Fork")
        if not group.finished:
            raise MachineNotRunnable("Fork 子运行尚未完成")
        return group

    def _finish_group(self, group: ForkGroup, *, merge_data: bool = True) -> None:
        stop_remaining = (
            group.policy == "any" and group.successful
        ) or (group.failure_policy == "fail_fast" and group.failed)
        if stop_remaining:
            for child in group.children:
                if child.status not in _TERMINAL:
                    if child.context.node_id is None:
                        child.context.node_id = child.graph.initial
                    child.context.waiting = None
                    child.context.status = Status.STOPPED
                    child.context.metadata["stop_reason"] = (
                        "fork_join_any"
                        if group.policy == "any" and group.successful
                        else "fork_fail_fast"
                    )
        group.joined = True
        if (
            merge_data
            and group.kind == "subgraph"
            and group.successful
            and group.children
        ):
            self.parent.context.data.update(
                copy.deepcopy(group.children[0].context.data)
            )
        if self.group is group:
            self.group = None
            self.parent.context.waiting = None

    @staticmethod
    def _join_payload(group: ForkGroup) -> dict[str, Any]:
        return {
            "fork_id": group.id,
            "children": [child.context.snapshot() for child in group.children],
        }

    def _prepare_join_dispatch(self, group: ForkGroup) -> None:
        # Detaching the completed group lets the committed join transition fork
        # again, while _restore_join can reinstall it if dispatch never commits.
        self.group = None
        self.parent.context.waiting = None
        self.parent.context.status = Status.RUNNING
        if group.kind == "subgraph" and group.children:
            self.parent.context.data.update(
                copy.deepcopy(group.children[0].context.data)
            )

    def _restore_join(self, group: ForkGroup, snapshot: dict[str, Any]) -> None:
        restored = Context.from_snapshot(snapshot)
        context = self.parent.context
        context.node_id = restored.node_id
        context.status = restored.status
        context.data.clear()
        context.data.update(restored.data)
        context.metadata.clear()
        context.metadata.update(restored.metadata)
        context.last_event = restored.last_event
        context.attempt = restored.attempt
        context.waiting = restored.waiting
        group.joined = False
        self.group = group
