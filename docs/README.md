# Bricks 文档

Bricks 是一个小而明确的 Python 编排内核：在一张 Graph 内传递数据，在多张 Graph 之间发布领域事件，
再由 Runtime 管理路由和执行生命周期。

```text
Node -- Output / Edge --> Node          同一张 Graph 内的数据流
Graph -- Event / Runtime --> Graph      多张 Graph 间的工作流
```

## 从这里开始

1. [快速开始](getting-started.md)：用一张 Graph 跑通最小程序。
2. [核心概念](core-concepts.md)：理解 Ports、Node、Output、Edge 和 Event 的边界。
3. [核心架构](architecture.md)：查看 Runtime 组装关系和从 Event 到 Graph execution 的完整流程。
4. [运行语义](runtime-semantics.md)：查阅输入触发、并发、失败、关闭等实际行为。
5. [扩展 Runtime](extending-runtime.md)：实现 EventBus、TaskPublisher、TaskConsumer 或 GraphExecutor 适配器。
6. [常见编排方式](examples.md)：运行线性、分支汇聚、循环、事件路由和异步 Node 示例。

需要了解项目的设计约束和贡献边界时，再阅读[架构原则](constitution.md)。它不是入门教程，也不是尚未实现的
路线图。

## 当前能力边界

默认 Runtime 是单进程实现：事件在内存中同步分发，路由后的 Graph 使用内存队列和线程池执行。它支持类型
校验、Graph 冻结、命名队列并发和可替换后端；不提供持久化、消息确认、进程恢复、定时任务、死信队列或
exactly-once 语义。

这些限制是实现事实，不是计划中的能力。需要对应保证时，请实现并明确声明自己的后端适配器语义。
