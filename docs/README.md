# Bricks：从 Graph 到事件工作流

这是一份按顺序阅读的 Bricks 手册。它从设计动机开始，用一张最小 Graph 建立直觉，再分别深入 Graph 数据流、
跨图事件、执行控制、内部架构和插件开发。

第一次接触 Bricks 时，建议从第一章连续读到第五章；实现基础设施适配器或参与内核开发时，再继续阅读第六章以后。

## 目录

### 第一部分：建立模型

1. [设计哲学与心智模型](01-design-philosophy.md)
   为什么区分 Output 与 Event，什么属于微内核，什么可以插件化。
2. [第一张 Graph](02-first-graph.md)
   定义 Node、构建 Graph、注册并运行，完成最小闭环。

### 第二部分：掌握运行语义

3. [Graph 数据流](03-graph-dataflow.md)
   Ports、Edge、InputPolicy、循环、冻结和 ExecutionPlan。
4. [Event 与跨图工作流](04-events-and-workflows.md)
   Context.emit、事件路由、Work、观察者和 Slot 链路。
5. [Execution、并发与失败](05-execution.md)
   输出流、取消、超时、并发、错误传播和生命周期。

### 第三部分：理解和扩展系统

6. [Runtime 内部架构](06-runtime-architecture.md)
   Router、Worker、Backend、Executor 与 PluginHost 的组装和调用序列。
7. [插件、SPI 与适配器开发](07-plugins.md)
   插件生命周期、能力贡献、后端协议、Hook 和 Observer。
8. [编排模式](08-patterns.md)
   线性、分支汇聚、KeyedJoin、循环、事件路由、异步和输出流。

## 如何使用这本手册

- 想快速跑起来：读第一、二章。
- 正在设计业务 Graph：读第三、四、五章。
- 正在实现插件或远程适配器：读第六、七章。
- 想寻找可复用结构：直接查第八章。
- 正在维护仓库包结构：读第六章的“源码目录与包职责”和“依赖方向”。

## 图示约定

关系、调用顺序、状态变化、生命周期和典型拓扑优先使用 Mermaid 图，并紧邻对应的文字或代码。图用于先建立整体
直觉，正文负责精确定义边界与异常语义；新增或修改图示时，两者必须同步更新。单一事实和已经足够清楚的短代码
不强行配图，避免视觉噪声。

## 当前能力边界

默认 Runtime 由 PluginHost 安装单进程 LocalRuntimePlugin：事件在内存中同步分发，路由后的 Graph 使用内存队列
和线程池执行。它支持类型校验、Graph 冻结、命名队列并发、可独立替换的事件/任务传输与执行器，以及受控插件
贡献；不提供持久化、broker
消息确认、进程恢复、定时任务、死信队列或 exactly-once。

[开始阅读：第一章](01-design-philosophy.md)
