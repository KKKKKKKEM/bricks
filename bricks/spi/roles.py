"""Runtime 依赖的角色协议；便利等待接口由门面组合 Execution 实现。"""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable, Iterator
from typing import Any, Protocol, runtime_checkable

from ..engine.core import Output
from ..engine.events import Event
from ..engine.execution import Execution, ExecutionLimits
from ..engine.graph import ExecutionPlan, Graph
from ..engine.hooks import HookHandle, HookPhase, NodeHook
from ..engine.observation import ObserverHandle, RuntimeObserver
from ..engine.policies import InputSelector
from ..engine.slots import SlotProvider
from .runtime import EventHandler


@runtime_checkable
class RouterRole(Protocol):
    @property
    def idle(self) -> bool: ...

    def observe(self, event_type: str, handler: EventHandler) -> object: ...

    def route(
        self,
        event_type: str,
        *,
        graph: str,
        queue: str,
        subscription: str | None = None,
        limits: ExecutionLimits | None = None,
    ) -> object: ...

    def emit(self, event_or_type: Event | str, payload: Any = None) -> Event: ...

    def publish(self, event: Event) -> None: ...

    def observe_runtime(self, observer: RuntimeObserver) -> ObserverHandle: ...

    def wait_idle(self, timeout: float | None = None) -> None: ...

    def close(self) -> None: ...


@runtime_checkable
class WorkerRole(Protocol):
    @property
    def idle(self) -> bool: ...

    def register(self, name: str, graph: Graph) -> object: ...

    def consume(
        self,
        queue: str,
        *,
        concurrency: int = 1,
        slots: SlotProvider | None = None,
    ) -> object: ...

    def start(
        self,
        graph: str,
        inputs: Any = None,
        *,
        plan: ExecutionPlan | None = None,
        max_steps: int = 0,
        timeout: float | None = None,
        output_buffer: int = 64,
    ) -> Execution: ...

    def iter(
        self,
        graph: str,
        inputs: Any = None,
        *,
        plan: ExecutionPlan | None = None,
        max_steps: int = 0,
        timeout: float | None = None,
        output_buffer: int = 64,
    ) -> Iterator[Output]: ...

    def aiter(
        self,
        graph: str,
        inputs: Any = None,
        *,
        plan: ExecutionPlan | None = None,
        max_steps: int = 0,
        timeout: float | None = None,
        output_buffer: int = 64,
    ) -> AsyncIterator[Output]: ...

    def get_execution(self, execution_id: str) -> Execution: ...

    def executions(self) -> tuple[Execution, ...]: ...

    def attach(
        self,
        hook: NodeHook | Callable[..., object],
        *,
        phase: HookPhase | str | None = None,
        graph: str | None = None,
        node: str | None = None,
    ) -> HookHandle: ...

    def contribute_hook(
        self,
        hook: NodeHook | Callable[..., object],
        *,
        phase: HookPhase | str | None = None,
        graph: str | None = None,
        node: str | None = None,
    ) -> HookHandle | None: ...

    def register_policy(self, name: str, selector: InputSelector) -> object: ...

    def observe_runtime(self, observer: RuntimeObserver) -> ObserverHandle: ...

    def wait_idle(self, timeout: float | None = None) -> None: ...

    def close(self) -> None: ...
