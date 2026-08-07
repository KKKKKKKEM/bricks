# Bricks 开发原则与总体思路

这份文档记录 Bricks 的长期定位、建模方式和开发约束。

它不是某个具体模块的 API 说明，而是判断“一个新能力应该如何设计、放在哪里、
是否值得加入核心”的依据。

## 一、Bricks 的定位

Bricks 的目标不是重新做一个 Spider 框架，也不是现在就做一个 Agent 框架。

Bricks 的核心定位是：

> 一个领域无关、可组合、可扩展的图编排与执行引擎。

Spider、Agent、ETL、Browser、RPC、自动化任务等，都应该建立在这个引擎之上：

```text
Spider / Agent / ETL / Browser / RPC
                  ↓
          Bricks Graph Engine
                  ↓
       Graph / Context / Event / Machine
```

引擎只提供通用的流程表达和运行能力，不直接依赖：

- HTTP 或浏览器
- 数据库或消息队列
- LLM 或 Agent 规划器
- Spider 的请求、响应和调度模型
- 某个具体业务领域的对象

这些能力以后应该放到 `adapters/`，而不是反向污染 `engine/`。

## 二、先做内核，再做领域能力

开发顺序遵循：

```text
图模型
  ↓
一次运行
  ↓
事件迁移
  ↓
Hook 和策略
  ↓
持久化和调度
  ↓
Spider / Agent / ETL 等适配器
```

当前阶段优先保证图引擎的边界、模型和运行语义稳定，不提前实现领域功能。

不能因为未来可能支持 Agent，就提前把 LLM、工具调用、记忆和规划器塞进核心；
也不能因为最初来自 Spider，就让图引擎继续依赖请求、下载器和队列。

## 三、用图表达编排，用 Context 表达运行

### Graph 是静态定义

`Graph` 描述流程本身：

- 节点
- 迁移
- 事件名称
- Guard
- Action 引用
- 元数据

它应该是不可变的、可复用的，并且可以被多个运行实例共享。

```text
Graph = 流程定义
Machine = 一次运行
Context = 运行数据
```

### Context 是运行上下文

Bricks 不使用一个独立的 `State` 对象来承载业务状态，而使用 `Context`：

- `node_id`：当前流程位置
- `status`：运行生命周期
- `data`：业务数据
- `metadata`：运行元数据
- `waiting`：等待和恢复信息
- `attempt`：当前尝试次数

这里的 `Status` 只表示运行生命周期，例如 `RUNNING`、`WAITING`、`COMPLETED`，
不是一套与 Context 并列的业务状态模型。

### 状态变化必须经过图

Action 可以返回 `Outcome` 表达控制意图，但不能直接绕过图修改流程位置：

```text
Event + 当前节点
        ↓
    Transition
        ↓
      Action
        ↓
     Outcome
        ↓
OutcomeInterpreter
        ↓
Effect 继续 / Control 停止
```

这样可以保证流程变化是可追踪、可校验、可监听和可恢复的。

Machine 不应为新的领域 Outcome 增加类型分支。通用内建语义和领域扩展都通过不可变
`OutcomeRegistry` 组合；扩展只需决定处理函数以及处理后继续还是停止当前迁移。

## 四、图不只有一种语义

Bricks 的基础图模型可以承载多种编排思路，但不为每种思路复制一套引擎。

### 事件迁移

这是核心语义：

```text
当前节点 + Event → Transition → 下一个节点
```

它可以表达传统状态机，但 Bricks 不把自己限制为只有状态机。

### 工作流和 DAG

通过节点和迁移的拓扑关系，可以表达顺序步骤、DAG 和批处理流程。

### 事件驱动和响应式

`EventBus` 负责事件发布订阅，`ReactiveRuntime` 负责把外部事件路由到 Machine。

事件总线不应该知道 Machine 的内部实现，Machine 也不应该知道 Redis、Kafka 或
HTTP 回调的具体形式。

### 并行和补偿

Fork/Join、Saga 和补偿流程属于组合语义，不应该变成 Machine 内部不断增长的
特殊分支。

原则是：

> 一个基础图模型，组合多种运行语义；不要为每种语义复制一套核心对象。

## 五、核心接口必须极简

核心入口只保留构建和运行一张图真正需要的对象：

```python
from bricks import Context, Event, Graph, GraphBuilder, Machine, Outcome, Status
```

核心调用保持简单：

```python
machine = Machine(graph)
machine.start()
machine.dispatch("next", payload)
```

核心对象的职责如下：

| 对象 | 唯一职责 |
| --- | --- |
| `GraphBuilder` | 构建图定义 |
| `Graph` | 保存不可变图定义 |
| `Machine` | 执行一次图运行 |
| `Context` | 保存一次运行的数据 |
| `Event` | 表达输入信号 |
| `Outcome` | 表达 Action 的控制意图 |
| `Status` | 表达运行生命周期 |

核心入口不应该暴露所有内部类型，也不应该把每个扩展模块的对象都重新导出到
`bricks` 顶层。

## 六、面向对象，但避免全能对象

面向对象不是把所有功能都放到一个大类里，而是让对象拥有清晰、稳定的职责。

### 一个对象只负责一个变化原因

- `Graph` 不负责运行、存储和发布事件。
- `Context` 不负责解析迁移和执行 Action。
- `Machine` 不负责数据库、队列、调度器和领域业务。
- `EventBus` 不负责理解图结构。
- `PersistenceBinding` 不负责改变流程语义。
- `Outcome` 不负责直接修改 Graph。

当一个对象因为多个完全不同的原因频繁变化时，就应该考虑拆分。

### BaseNode + 继承扩展

节点允许有多种类型，但所有节点先共享一个最小基类 `BaseNode`：

```text
BaseNode
├── ActionNode
├── WaitNode
├── TerminalNode
└── CustomNode
```

`BaseNode` 只放所有节点共有的字段和行为。具体节点通过继承增加自己的数据和
语义，不把所有未来字段都提前堆到基类中。

节点通过统一的 `enter()` / `exit()` 行为扩展：

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

`Machine` 只依赖 `BaseNode` 的协议，不需要为每个自定义节点添加 `isinstance` 分支。

### 优先组合，谨慎继承 Machine

扩展能力优先通过以下方式接入：

1. 通过协议替换执行器、存储或外部传输。
2. 通过 Hook 观察生命周期。
3. 通过 EventBus 接入外部事件。
4. 通过语义对象组合 Machine。
5. 只有行为确实改变核心运行规则时，才继承 Machine。

继承应该主要发生在具有稳定层次关系的对象上，例如 `BaseNode`；基础设施和
领域能力优先使用组合。

## 七、扩展必须在边界之外

当前扩展分层如下：

```text
bricks
└── GraphBuilder / Graph / Machine / Context / Event / Outcome / Status

bricks.engine.graph
└── BaseNode / ActionNode / WaitNode / TerminalNode / Transition / Guard

bricks.engine.events
└── EventBus / HookRegistry

bricks.engine.persistence
└── PersistenceBinding / SnapshotStore / EventLog

bricks.engine.policies
└── RetryPolicy / TimeoutPolicy / CancellationToken / IdempotencyKey

bricks.engine.semantics
└── Workflow / ReactiveRuntime / Parallel / SagaRuntime

future adapters
└── Spider / Agent / ETL / Browser / RPC
```

上层模块可以依赖核心，核心不能反向依赖上层模块。

例如：

- 接入 Redis，只实现 `SnapshotStore`，不修改 `Graph`。
- 接入消息队列，只把消息转换成 `Event`，不让 `EventBus` 感知队列类型。
- 接入 Agent，只在 `adapters/agent/` 中实现工具、记忆和规划，不给核心增加 LLM
  特殊分支。
- 接入 Spider，只在 `adapters/spider/` 中实现下载、解析和领域节点。

## 八、拒绝冗余和过早抽象

以下规则是强约束：

- 不为只有一个实现的概念提前创建工厂、注册表或管理器。
- 不为同一份数据同时保留多个等价模型。
- 不为了“以后可能需要”把领域字段放进核心对象。
- 不把简单函数包装成没有独立职责的新对象。
- 不让一个对象同时承担运行、持久化、调度、消息和领域业务。
- 一个扩展如果通过 Hook 或协议就能完成，就不要修改 Machine。
- 公共接口少于内部实现，内部辅助类不从 `bricks` 顶层导出。
- 能删除的冗余代码优先删除，不用兼容层掩盖设计问题。

新对象只有在以下条件同时满足时才值得公开：

1. 有独立且稳定的职责。
2. 有自己的生命周期或可替换实现。
3. 用已有对象表达会导致职责混乱。
4. 至少有一个真实场景可以独立使用。

## 九、文档和代码同步演进

文档不是开发完成后的附属品，而是架构边界的一部分。

文档按四层组织：

```text
README.md                 项目定位和最小示例
docs/graph_engine.md      核心运行机制
docs/module_map.md        模块职责和阅读顺序
docs/design_principles.md 总体原则和设计取舍
docs/guides/              面向使用场景的教程
docs/reference/           稳定 API 参考
docs/decisions/           长期架构决策记录
```

当前只创建已经有内容的文档目录，未来目录在对应能力稳定后再创建，避免保留空
目录和无意义的占位文件。

文档规则：

- 核心文档只讨论通用图执行，不提前引入具体领域。
- 示例优先使用最小公开 API。
- 参考文档只描述稳定接口，不把内部方法当作公共 API。
- 设计文档记录边界和取舍，不重复粘贴代码注释。
- 接口删除、移动或重命名时，同时更新文档和测试。
- 代码注释和核心说明优先使用中文，名称和 API 保持清晰的英文技术命名。

## 十、通过测试保持边界

每个核心能力都应该至少有一个行为测试：

- 图可以构建并校验。
- Graph 不可变且可以复用。
- Machine 可以启动和派发事件。
- Context 可以保存业务数据和恢复信息。
- Guard 可以控制迁移选择。
- Hook 可以观察生命周期。
- 自定义 BaseNode 不需要修改 Machine 就能运行。
- 扩展模块不会污染核心入口。

测试的目标不是覆盖每一行代码，而是保护模型边界、公开接口和扩展方式。

## 十一、当前和未来的开发节奏

前六阶段已经形成当前基线：核心 API、事件和 Hook、运行策略、快照边界、组合语义
以及文档体系都已有实现、行为测试和公开说明。后续不再以“增加文件数量”为进度，
而以契约稳定性和边界质量为进度。

### 已完成：冻结 0.3 行为契约

- 已固定同步、异步、取消、超时、幂等和 Hook 异常的行为矩阵。
- 已补齐公开 API 的类型标记、异常说明和最小示例。
- 已通过快照版本和 Graph 身份校验保留恢复扩展边界。
- 后续接口变化必须同时更新测试、参考文档和变更记录。

### 当前阶段：增强图编排能力

- SubGraph 和可复用图片段。
- 图定义序列化、可视化和静态分析。
- 条件分支、循环、Join 和错误边的更清晰建模。
- Fork 的真实并发执行协议，以及外部调度器、队列和指标边界。

### 最后阶段：领域适配器

- Spider
- Agent
- ETL
- Browser
- RPC

领域适配器必须建立在稳定的引擎协议之上，不通过继续增加 Machine 特殊分支来快速
堆功能。详细顺序见 [roadmap.md](roadmap.md)。

## 最终判断标准

设计一个新功能时，优先问：

1. 这是所有领域都需要的能力，还是某个领域的能力？
2. 它是否有清晰、独立、稳定的职责？
3. 能否通过继承、组合、Hook 或协议完成？
4. 是否真的需要新增公开对象？
5. 是否会让 Machine 或 Context 变得更复杂？
6. 文档和测试是否能清楚表达它的边界？

如果一个设计不能让核心更清晰、更小、更容易扩展，就不应该进入核心。
