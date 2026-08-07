"""Engine 核心抽象抛出的公共异常。"""

from __future__ import annotations

from collections.abc import Iterable


class BricksError(Exception):
    """所有 Bricks 公共异常的基类。"""


class GraphError(BricksError):
    """Graph 定义和查询异常的基类。"""


class GraphDefinitionError(GraphError):
    """Graph 构建过程中定义不合法。"""


class GraphFrozenError(GraphError):
    """调用方尝试修改已经冻结的 Graph。"""

    def __init__(self, graph_name: str) -> None:
        """创建 Graph 冻结异常。

        参数：
            graph_name: 被修改的 Graph 名称。
        """

        super().__init__(f"graph {graph_name!r} is frozen")
        self.graph_name = graph_name


class GraphValidationError(GraphError):
    """Graph 因定义不合法而无法冻结。"""

    def __init__(self, issues: Iterable[str]) -> None:
        """创建包含全部校验问题的异常。

        参数：
            issues: 本次 Graph 校验发现的问题描述。
        """

        self.issues = tuple(issues)
        detail = "\n".join(f"- {issue}" for issue in self.issues)
        super().__init__(f"graph validation failed:\n{detail}")


class UnknownNodeError(GraphError):
    """Graph 中不存在指定 Node ID。"""

    def __init__(self, node_id: str) -> None:
        """创建未知 Node 异常。

        参数：
            node_id: 查询失败的 Node ID。
        """

        super().__init__(f"unknown node {node_id!r}")
        self.node_id = node_id


class UnknownFlowError(GraphError):
    """Graph 中不存在指定 Flow。"""

    def __init__(self, flow_name: str) -> None:
        """创建未知 Flow 异常。

        参数：
            flow_name: 查询失败的 Flow 名称。
        """

        super().__init__(f"unknown flow {flow_name!r}")
        self.flow_name = flow_name


class InvalidOutputError(BricksError, TypeError):
    """NodeResult 产生了非 Output 值。"""

    def __init__(self, value: object) -> None:
        """创建非法节点输出异常。

        参数：
            value: NodeResult 实际产生的非法值。
        """

        super().__init__(
            "NodeResult must produce Output instances, "
            f"got {type(value).__name__}"
        )
        self.value = value
