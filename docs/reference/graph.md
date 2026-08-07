# 图定义 API

## 节点

所有节点都继承 `BaseNode`。基类只提供节点 ID、元数据以及统一的 `enter()` / `exit()`
协议。Machine 只依赖这两个协议，不为每一种节点增加类型分支。

内置节点是最小的可选扩展：

- `ActionNode`：继承基类默认行为，通常在进入时执行 `action`。
- `WaitNode`：没有自定义结果时返回 `Wait`。
- `TerminalNode`：将 `terminal` 设为真，进入后运行完成。

自定义节点直接继承 `BaseNode`：

```python
from dataclasses import dataclass
from typing import ClassVar

from bricks.engine.graph import BaseNode


@dataclass(frozen=True)
class ApprovalNode(BaseNode):
    kind: ClassVar[str] = "approval"
    role: str = "reviewer"

    def enter(self, context, event, executor):
        context.set("approval_role", self.role)
        return super().enter(context, event, executor)
```

覆盖方法时，如果还需要执行基类 Action，应调用 `super()`。节点 Action 通过 Machine
的执行器运行，因此节点不需要知道线程、协程或远程执行的细节。

如果自定义节点只覆盖同步 `enter()` / `exit()`，异步 Machine 也会保留这些扩展逻辑；
如果节点需要专门的异步实现，可以同时覆盖 `enter_async()` / `exit_async()`。自定义
异步方法应通过传入的执行器执行 Action，不要直接改变 Machine 的节点位置。

## 迁移

`Transition` 表示：

```text
当前节点 + Event -> target 节点
```

迁移可以附带：

- `guard(context, event)`：是否允许选择该迁移；可以是同步 Guard，也可以是异步
  Guard。异步 Guard 必须通过异步运行入口求值；同步和异步入口都会把完整 `Event`
  传给 Guard。
- `action(context, event)`：迁移发生时执行的 Action。
- `priority`：同一事件存在多条候选边时的升序优先级。
- `metadata`：给 Saga 等组合语义使用的声明性元数据。

迁移的 Action 在节点退出之后、目标节点进入之前执行。节点进入或退出返回的
`Outcome` 会交给运行时应用。

同步 `dispatch()` 遇到异步 Guard 会抛出 `AsyncGuardRequired`，不会改变当前位置；
`dispatch_async()` 会等待 Guard 再按优先级和声明顺序选择迁移。

## 校验和不可变性

内置节点会冻结元数据等声明字段。自定义 `BaseNode` 子类新增的字段仍由扩展作者负责保持
不可变；需要参与 `GraphBuilder.include()` 重命名时，可覆盖 `with_id()`，并确保新 ID
相关的派生字段和校验会重新执行。

`GraphBuilder.build()` 和公开的 `Graph(...)` 构造都会检查初始节点、迁移源和目标、ID
和事件名称。构建完成后，`Graph.nodes` 使用只读映射，迁移集合为元组；节点、迁移的
metadata 以及 SubGraphNode 的静态 data 会递归冻结。要修改流程，应修改 Builder 并重新
构建新的 Graph。

`SubGraphNode` 的静态 `data` 在构建时深复制并以只读映射保存，修改构建前的输入或图中
节点的顶层数据不会改变共享 Graph。

`GraphBuilder(..., version="...")` 为图定义提供版本标识。它与序列化格式的
`version` 不同：前者用于判断当前流程定义是否兼容，后者用于判断字典格式是否兼容。

Graph 不保存 Context，也不保存运行时生成的 Event、重试次数或等待信息。

## 组合和检查

- `GraphBuilder.include(graph, prefix=...)`：将已校验 Graph 作为命名空间片段加入，返回
  被包含图的入口节点 ID。
- `GraphBuilder.subgraph(node_id, graph, ...)`：声明一个进入时启动独立 Graph 的节点。
- `Graph.describe()`：返回带有 `bricks.graph.description` schema 和版本号的结构描述，
  字段设计适合 JSON 编码，不包含 Action、Guard 等可执行对象。调用方提供的 metadata
  和静态 data 仍应使用 JSON 兼容值。内置 `WaitNode` 和
  `SubGraphNode` 的静态配置会放在节点的 `config` 中，适合编辑器、检查器和 Mermaid/DOT
  等转换器使用。
- `Graph.to_mermaid()`：生成可交给 Mermaid 渲染器的流程图文本。
- `Graph.to_dot()`：生成可交给 Graphviz 或其它 DOT 工具链的流程图文本，不需要安装
  Graphviz Python 包。
- `reachable_nodes(graph)` / `unreachable_nodes(graph)`：检查从初始节点可达的定义节点。
- `terminal_nodes(graph)`：返回声明的终止节点。
- `dead_end_nodes(graph)`：返回可达但没有迁移、也不是终止节点的死端，供发布前检查。
- `cycle_nodes(graph)`：返回参与有向环的节点，包含不可达节点。
- `non_terminating_nodes(graph)`：返回可达但没有路径到任何终止节点的节点。
- `transition_conflicts(graph)`：返回同一来源、事件和优先级下的迁移 ID 组，供人工检查
  Guard 是否互斥；运行时仍按优先级和声明顺序选择。

这些函数只提供静态检查结果，不改变 Graph，也不会把循环或等待节点视为构建错误。
循环可以用于事件驱动流程，等待节点也可能是有意的外部边界；是否允许它们由发布流程
自行决定。

## 结构化序列化

`Graph.to_dict()` 输出带 schema `version` 和 `graph_version` 的结构化图定义，
`Graph.from_dict()` 会拒绝重复节点 ID，校验格式版本并重新执行图校验。图定义只保存引用，不猜测 Python
函数的导入路径：

```python
encoded = graph.to_dict(
    action_serializer=lambda action: action_registry.name(action),
    guard_serializer=lambda guard: guard_registry.name(guard),
)
restored = Graph.from_dict(
    encoded,
    action_resolver=action_registry.resolve,
    guard_resolver=guard_registry.resolve,
)
```

内置节点的配置会直接编码。自定义 `BaseNode` 需要同时提供
`node_serializer(node) -> mapping` 和 `node_resolver(definition) -> BaseNode`。`SubGraphNode`
只保存子图 ID，恢复时还需要 `graph_resolver(graph_id)`。解析器是显式边界，调用方可以
使用注册表、数据库或自己的配置系统实现它们。

`describe()`、Mermaid 和 DOT 文本适合检查和可视化，但不包含恢复所需的 Action、Guard
和自定义节点配置；需要持久化执行定义时应使用 `to_dict()`。
