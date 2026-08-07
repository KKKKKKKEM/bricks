"""未来由运行时传给节点的执行上下文。"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any

from ._validation import require_non_empty_string


@dataclass(frozen=True, slots=True)
class ExecutionContext:
    """保存只读的执行身份和调用方元数据。"""

    run_id: str
    task_id: str
    flow: str
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """校验身份字段，并复制 metadata 以隔离外部修改。

        异常：
            TypeError: 身份字段不是字符串，或 metadata 不是映射。
            ValueError: 身份字段为空。
        """

        require_non_empty_string(self.run_id, "run_id")
        require_non_empty_string(self.task_id, "task_id")
        require_non_empty_string(self.flow, "flow")
        if not isinstance(self.metadata, Mapping):
            raise TypeError("metadata must be a mapping")

        object.__setattr__(
            self,
            "metadata",
            MappingProxyType(dict(self.metadata)),
        )
