# Bricks 宪法

状态：Normative
适用范围：核心 Runtime、扩展适配器与领域框架

## 第一条：Less is more

1. 顶层公共模型只保留 Graph 基础类型、Event、Context、Slot 和 Runtime。
2. 内部责任划分不自动升级为用户概念。
3. 删除错误抽象优先于维护错误抽象的兼容层。
4. 新能力能由 Graph、Event、Runtime 和领域组件组合时，不进入核心。
5. 新用户概念必须证明不能由现有概念承担，且至少服务两个不同领域。

顶层 API 固定为：

```text
Ports、Node、AsyncNode、InputPolicy、Output、Edge、Graph、ExecutionPlan、
Event、Context、Slot、SlotPool、Runtime
```

## 第二条：局部数据流与跨图事件分离

1. Output 只在当前 Graph 内沿 Edge 传播。
2. Event 只通过 `Context.emit()` 或 `Runtime.emit()` 进入跨图流程。
3. Node 返回值不得隐式转换为 Event，Event 不得隐式转换为 Edge 数据。
4. 目标 Graph 不在源 Node 的 emit 调用栈内执行。
5. 跨图事件使用 imperative `emit()`，不得依靠 `yield` 的多义语义。

## 第三条：Graph 是静态有向定义

1. Node 声明 typed input/output Ports。
2. InputPolicy 只判断端口组合，不读取领域值。
3. Graph 进入 Runtime 前必须冻结并校验可达性和类型兼容；普通 Edge 可以组成环。
4. Node ID 属于 Graph binding，Node 不保存某次 execution 状态。
5. Graph 内循环由 Output 沿回边继续传值，并在不再产生可执行数据时自然结束。
6. 无下游 Edge 的 Output 由 `Runtime.run()` 返回。

## 第四条：Event 是最小事实

1. Event 核心只携带 `type` 和 `payload`。
2. 关联 ID、时间、来源、幂等键和 tracing 数据属于领域 payload 或外部观察者。
3. `emit()` 成功返回表示事件传输已接受 Event。
4. 已接受 Event 不因源 Graph 后续失败而撤回。
5. Runtime 接受每一个 Event，不比较 payload，不做领域去重。

## 第五条：Runtime 是 composition root

1. Runtime 注册 Graph，用 `on(event, graph, queue, concurrency)` 组合路由与本地消费，并用 `observe()` 独立注册
   观察者；高级组合可以分别调用 `route()` 与 `consume()`。
2. Runtime 显式组合 EventRouter 与 GraphWorker；Router 组装事件发布和任务投递，Worker 组装任务消费和 Graph
   执行。自建组件由创建者管理，注入组件默认由调用方管理。
3. Runtime 不得直接实现消息持久化、队列算法、Node 执行或领域策略。
4. Trigger、Task、Queue、Scheduler、execution record 可以作为内部职责存在，但不要求用户逐项组装。

## 第六条：可替换性通过窄协议获得

1. EventBus 只负责 Event 发布、订阅和投递生命周期。
2. TaskPublisher 负责工作投递，TaskConsumer 负责命名执行通道、本地并发和消费；TaskBackend 是两者的组合。
3. GraphExecutor 只负责执行冻结 Graph。
4. EventBus、任务传输和 GraphExecutor 必须可以独立替换和组合，不得合并为万能 Backend。
5. SPI 位于高级扩展层，不进入顶层 `bricks` API。
6. Runtime 只依赖协议，不得使用默认内存实现的私有状态。

## 第七条：可靠性不得夸大

1. 当前内存实现不得宣称持久 broker、消息 ack、跨进程租约、恢复或 exactly-once。
2. 核心不自动重试；emit 后的失败不会撤回已发布 Event。
3. Redis/MQ 适配器必须明确自身 delivery、ack、retry 和 failure 语义。
4. URL、tool_call_id、batch_id 和外部副作用幂等属于领域 Graph/Store。

## 第八条：Context 保持狭窄

1. Context 只提供 `emit()` 和当前逻辑执行链的 `slot`。
2. 数据库、HTTP client、LLM provider、领域 Store 和 tracing 通过 Node 构造器或观察者注入。
3. Node 不得获得 Runtime 内部工作请求、TaskBackend 或 executor。

## 第九条：同步与异步共享语义

1. 同步行为继承 Node，需要 await 的行为继承 AsyncNode。
2. Graph.freeze() 校验 Node 类目与 execute 风格一致。
3. queue concurrency 限制完整 Graph execution，而不是单个 Node。
4. 同步和异步 Node 共享同一 InputPolicy、Output、Edge 和 Event 语义。
5. Node 默认必须无状态且可重入；跨 Work execution 状态放入 Slot，不得隐式存放在共享 Node 实例中。

## 第十条：Slot 跟随逻辑执行链

1. Slot 不绑定线程、Worker 或 Consumer；Work 跨 Consumer 流转时必须携带同一个 Slot。
2. 根 Work 从 Consumer 配置的 SlotPool 获取 Slot，整个逻辑链结束后自动归还。
3. 分支 Work 可以共享 Slot，但同一个 Slot 的 Graph execution 不得并发执行。
4. `concurrency` 限制 Consumer 的本地 Graph execution；`slots.size` 限制池中的逻辑执行链，两者相互独立。
5. 等待 Slot 的根 Work 不得占用 Consumer 的执行线程，也不得阻塞已携带 Slot 的延续 Work。

## 第十一条：公共行为必须可验证

1. Graph 冻结与执行约束必须有失败测试。
2. Event 提交、跨图连接、并发和错误传播必须有契约测试。
3. 三个替换协议必须有非默认实现组合测试。
4. 示例必须只通过相同顶层 API 组合，并覆盖核心编排方式。
5. 文档不得把计划能力写成已实现能力。

## 修订记录

### 2026-08-08：最小公共模型与可替换能力端口

审计曾把 Trigger、Task、TaskQueue、Scheduler、GraphInstance 和 Engine 全部提升为顶层对象。虽然内部职责更
清楚，但用户连接两张 Graph 时被迫理解运行实现，违背 less is more。

本次修订恢复精简顶层词汇，用 `Runtime.route()` 表达事件到 Graph 的连接、用 `observe()` 表达事件观察；
运行职责退回内部。与此同时引入 EventBus、TaskPublisher、TaskConsumer、TaskBackend、GraphExecutor
高级结构协议，默认内存实现
通过 Runtime 构造器注入。由此把“用户模型精简”和“基础设施可替换”分开解决。

领域去重、外部调用幂等和业务重试继续留在应用代码；默认内存实现不提供自动重试、持久化或 exactly-once。
