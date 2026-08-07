# Bricks 文档

Bricks 文档按“先理解模型，再使用能力，最后扩展实现”的顺序组织。

## 阅读路径

### 第一次接触

1. [graph_engine.md](graph_engine.md)：了解一次图运行的完整流程
2. [module_map.md](module_map.md)：了解每个目录和文件的职责
3. [architecture.md](architecture.md)：了解依赖边界和扩展方向
4. [design_principles.md](design_principles.md)：了解 Bricks 的总体原则和接口为什么这样设计
5. [roadmap.md](roadmap.md)：了解当前基线和后续推进顺序

### 开始使用

使用文档统一放在 `guides/`，每篇只解决一个具体问题：

```text
guides/
├── quickstart.md       # 创建图、启动 Machine、派发事件
├── events.md           # EventBus、Hook 和响应式运行
├── external-driver.md  # 按 run_id 恢复和驱动长运行实例
├── persistence.md      # 快照、恢复和事件日志
├── waiting.md          # Wait、Retry、暂停和恢复
├── routing.md          # Context 更新和条件路由
├── semantics.md        # Workflow、Reactive、Parallel、Saga
└── invocation.md       # invoke、stream 和批量运行
```

当前可直接运行的最小示例放在 `examples/`：

```text
examples/
├── basic_graph.py      # 基础图执行
├── custom_node.py      # BaseNode 继承扩展
├── composed_graph.py   # Graph 组合和结构描述
├── subgraph.py         # 运行时子图和 Join
├── serialized_graph.py # 版本化 Graph 定义和显式解析器
├── runtime_events.py   # 生命周期和父子运行事件流
├── custom_executor.py  # ActionExecutor 边界
├── custom_outcome.py   # 不修改 Machine 的领域 Outcome Effect
├── spider_adapter.py   # OutcomeRuntime + 原子 Outbox 的 Spider 控制面
├── async_guard.py      # 异步 Guard 和异步事件迁移
├── nested_fork.py      # 嵌套 Fork 的路由和 Join
├── fork_failure.py     # Fork 部分失败后的继续汇聚
├── external_events.py  # 外部消息驱动、持久化和恢复
├── external_driver.py   # 按 run_id 恢复并投递外部消息
├── custom_store.py      # 只实现 save/load/delete 的外部快照存储
├── graph_visualization.py # Graph 的 JSON、Mermaid 和 DOT 输出
├── exception_retry.py    # 节点异常的显式自动重试
├── snapshot_history.py   # 可选快照历史查询
├── dynamic_route.py    # Context 更新和内部条件路由
├── map_reduce.py       # 动态 Fan-out 和 Join 汇聚
├── context_update.py   # 控制面更新 Context 和人工审批事件
└── replay.py           # EventLog 的显式事件重放
```

### 查 API

稳定的公开 API 说明统一放在 `reference/`：

```text
reference/
├── core.md             # Graph、Context、Event、Machine、Outcome
├── graph.md            # Builder、BaseNode、Transition、Guard
├── extensions.md       # events、persistence、policies、semantics
├── behavior.md         # 0.3 同步、异步、生命周期和恢复契约
└── orchestration.md    # 与通用图编排能力的对照
```

### 了解设计决策

有长期影响的架构选择放在 `decisions/`，每篇记录一个决策，不在普通使用文档里
重复解释：

```text
decisions/
├── 001-context-over-state.md
├── 002-machine-core-boundary.md
├── 003-extension-by-composition.md
├── 004-static-graph-composition.md
├── 005-external-execution-boundary.md
├── 006-outcome-interpretation.md
└── 007-runtime-reliability-ports.md
```

## 当前文档和代码的对应关系

```text
docs/graph_engine.md      核心运行概念和示例
docs/module_map.md        模块职责和阅读顺序
docs/architecture.md      分层、依赖和扩展边界
docs/design_principles.md 接口设计约束
docs/roadmap.md           当前基线和后续路线
docs/guides/              面向任务的使用说明
docs/reference/           当前稳定 API
docs/decisions/           长期架构决策
```

文档内容应遵守以下规则：

- 核心文档只讨论通用图执行，不提前引入 Spider、Agent 或具体基础设施。
- 使用文档优先给出最小可运行示例，再解释可选参数。
- 参考文档只描述当前稳定接口，不把内部方法当作公共 API。
- 设计文档记录边界和取舍，不复制代码注释。
- 删除或移动接口时，同时更新对应文档和测试。
