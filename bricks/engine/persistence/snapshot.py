"""可序列化的运行快照值对象。"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Mapping

from ..runtime.context import Context
from ..types import freeze_value, thaw_value

if TYPE_CHECKING:
    from ..runtime.machine import Machine


CURRENT_SNAPSHOT_VERSION = 1


@dataclass(frozen=True, slots=True)
class ContextSnapshot:
    """可保存的运行快照包装。

    ``runtime`` 保存 Machine 的完整恢复信息；``context`` 保留上下文的独立视图，
    方便只保存 Context 的场景。两者都不包含 Graph、Action 或执行器实例。
    """

    graph_id: str
    run_id: str
    context: Mapping[str, Any]
    runtime: Mapping[str, Any] | None = None
    version: int = CURRENT_SNAPSHOT_VERSION
    revision: int = 0
    graph_version: str = "1"

    def __post_init__(self) -> None:
        if not self.graph_id:
            raise ValueError("snapshot graph_id cannot be empty")
        if not self.graph_version:
            raise ValueError("snapshot graph_version cannot be empty")
        if not self.run_id:
            raise ValueError("snapshot run_id cannot be empty")
        if self.version < 1:
            raise ValueError("snapshot version must be positive")
        if self.revision < 0:
            raise ValueError("snapshot revision cannot be negative")
        context_run_id = self.context.get("run_id")
        if context_run_id is not None and context_run_id != self.run_id:
            raise ValueError("context run_id does not match snapshot run_id")
        context_graph_id = self.context.get("graph_id")
        if context_graph_id is not None and context_graph_id != self.graph_id:
            raise ValueError("context graph_id does not match snapshot graph_id")
        if self.runtime is not None:
            runtime_graph_id = self.runtime.get("graph_id")
            if runtime_graph_id is not None and runtime_graph_id != self.graph_id:
                raise ValueError("runtime graph_id does not match snapshot graph_id")
            runtime_context = self.runtime.get("context")
            if (
                runtime_context is not None
                and runtime_context.get("graph_id") != self.graph_id
            ):
                raise ValueError(
                    "runtime context graph_id does not match snapshot graph_id"
                )
            runtime_graph_version = self.runtime.get("graph_version")
            if (
                runtime_graph_version is not None
                and str(runtime_graph_version) != self.graph_version
            ):
                raise ValueError(
                    "runtime graph_version does not match snapshot graph_version"
                )
        object.__setattr__(self, "context", freeze_value(self.context))
        if self.runtime is not None:
            object.__setattr__(self, "runtime", freeze_value(self.runtime))

    @classmethod
    def from_context(cls, context: Context) -> "ContextSnapshot":
        return cls(
            context.graph_id,
            context.run_id,
            context.snapshot(),
            version=CURRENT_SNAPSHOT_VERSION,
            revision=int(context.metadata.get("snapshot_revision", 0)),
            graph_version=context.graph_version,
        )

    @classmethod
    def from_machine(cls, machine: "Machine") -> "ContextSnapshot":
        """保存完整运行快照，包括 Fork 子运行。"""
        snapshot = machine.snapshot()
        return cls(
            machine.graph.id,
            machine.context.run_id,
            snapshot["context"],
            snapshot,
            CURRENT_SNAPSHOT_VERSION,
            revision=int(machine.context.metadata.get("snapshot_revision", 0)),
            graph_version=machine.graph.version,
        )

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "ContextSnapshot":
        """从外部存储读取快照；缺少 version 时按第一版兼容。"""
        return cls(
            graph_id=value["graph_id"],
            run_id=value["run_id"],
            context=value["context"],
            runtime=value.get("runtime"),
            version=int(value.get("version", CURRENT_SNAPSHOT_VERSION)),
            revision=int(value.get("revision", 0)),
            graph_version=str(value.get("graph_version", "1")),
        )

    def to_machine_snapshot(self) -> dict[str, Any]:
        """转换为 Machine.from_snapshot() 接受的结构。"""
        if self.runtime is not None:
            runtime = thaw_value(self.runtime)
            context = dict(runtime.get("context") or {})
        else:
            runtime = {
                "graph_id": self.graph_id,
                "context": thaw_value(self.context),
            }
            context = runtime["context"]
        context.setdefault("graph_version", self.graph_version)
        metadata = dict(context.get("metadata") or {})
        metadata["snapshot_revision"] = self.revision
        context["metadata"] = metadata
        runtime["context"] = context
        runtime["graph_version"] = self.graph_version
        return runtime

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "revision": self.revision,
            "graph_id": self.graph_id,
            "graph_version": self.graph_version,
            "run_id": self.run_id,
            "context": thaw_value(self.context),
            **(
                {"runtime": thaw_value(self.runtime)}
                if self.runtime is not None
                else {}
            ),
        }
