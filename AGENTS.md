# Bricks Repository Instructions

## 执行要求

本文件是仓库级 AI 编码代理指令。修改代码、公共 API、运行语义、架构或相关文档前，必须完整阅读并遵守下面的
架构宪法。

- 实现、测试和文档必须与当前架构宪法一致。
- 以最新架构为准。除非用户明确要求，不保留旧别名、隐式转换、弃用路径、兼容分支或历史架构描述。
- 有意进行的设计调整若与宪法冲突，必须同步修改本文件、实现、测试和相关手册章节，不得绕过冲突。
- 架构宪法保持规范、简洁；原理解释和使用教程写入 `docs/` 中的编号章节。

## 架构宪法

状态：Normative
适用范围：核心 Runtime、扩展适配器与领域框架

## 第一条：Less is more

1. 顶层公共模型只保留 Graph 基础类型、Event、Context、Execution、Slot 和 Runtime。
2. 内部责任划分不自动升级为用户概念。
3. 删除错误抽象优先于维护错误抽象的兼容层。
4. 新能力能由 Graph、Event、Runtime 和领域组件组合时，不进入核心。
5. 新用户概念必须证明不能由现有概念承担，且至少服务两个不同领域。

顶层 API 固定为：

```text
Ports、Node、AsyncNode、InputPolicy、Output、Edge、Graph、ExecutionPlan、
Event、Context、Execution、ExecutionLimits、ExecutionStatus、Slot、SlotPool、Runtime
```

## 第二条：局部数据流与跨图事件分离

1. Output 只在当前 Graph 内沿 Edge 传播。
2. Event 只通过 `Context.emit()` 或 `Runtime.emit()` 进入跨图流程。
3. Node 返回值不得隐式转换为 Event，Event 不得隐式转换为 Edge 数据。
4. 目标 Graph 不在源 Node 的 emit 调用栈内执行。
5. 跨图事件使用 imperative `emit()`，不得依靠 `yield` 的多义语义。

## 第三条：Graph 是静态有向定义

1. Node 声明 typed input/output Ports。
2. InputPolicy 和受控 selector contribution 只根据端口与可用 token 数量选择组合，不读取领域值。
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

## 第五条：Runtime 是门面，PluginHost 是装配根

1. Runtime 注册 Graph，用 `on(event, graph, queue, concurrency)` 组合路由与本地消费，并用 `observe()` 独立注册
   观察者；高级组合可以分别调用 `route()` 与 `consume()`。
2. Runtime 的默认构造必须通过 PluginHost 安装 LocalRuntimePlugin，再取得 EventRouter 与 GraphWorker；不得另设
   只供内建实现使用的装配路径。
3. Router 组装事件发布和任务投递，Worker 组装任务消费和 Graph 执行。显式传入 Runtime 的 Router/Worker 由
   Runtime 管理；注入角色或插件的底层组件默认仍由调用方管理。
4. Runtime 不得直接实现消息持久化、队列算法、Node 执行、插件发现或领域策略。
5. Trigger、Task、Queue、Scheduler、execution record 可以作为内部职责存在，但不要求用户逐项组装。

## 第六条：可替换性通过窄协议获得

1. EventBus 只负责 Event 发布、订阅和投递生命周期。
2. TaskPublisher 负责工作投递，TaskConsumer 负责命名执行通道、本地并发和消费；TaskBackend 是两者的组合。
3. GraphExecutor 只负责执行冻结 Graph。
4. EventBus、任务传输和 GraphExecutor 必须可以独立替换和组合，不得合并为万能 Backend。
5. SPI 位于高级扩展层，不进入顶层 `bricks` API。
6. Runtime 和插件装配层只依赖公开角色或能力协议，不得使用默认内存实现的私有状态。

## 第七条：可靠性不得夸大

1. 当前内存实现不得宣称持久 broker、消息 ack、跨进程租约、恢复或 exactly-once。
2. 核心不自动重试；emit 后的失败不会撤回已发布 Event。
3. Redis/MQ 适配器必须明确自身 delivery、ack、retry 和 failure 语义。
4. URL、tool_call_id、batch_id 和外部副作用幂等属于领域 Graph/Store。

## 第八条：Context 保持狭窄

1. Context 提供 `emit()`、当前逻辑执行链的 `slot`、协作式 `checkpoint()`，以及按 Node/插件命名空间隔离的
   execution-local `state()` 与静止阶段 `on_quiescence()`。
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

## 第十一条：Execution 控制必须默认开放

1. 一次 Node firing 计为一步；`max_steps=0` 表示不限制步数。
2. Graph execution 的 `timeout=None` 和 Node 的 `timeout=None` 表示各自不限制时长，正数统一使用秒。
3. 取消和同步 Node timeout 是协作式语义；核心不得宣称能够安全强杀任意 Python 函数。
4. 控制异常不得被 Node Hook 当作普通业务异常恢复。
5. 跨 Graph 的每个 Work 是独立 execution，独立计步和计时。
6. Execution 是同步等待、异步等待和 terminal Output 流的统一句柄；便利接口不得维护不同执行语义。
7. 已交给流消费者的 terminal Output 不因后续 Graph 失败而撤回。

## 第十二条：扩展点保持受控

1. Runtime 生命周期通过只读事件观察；观察者失败不得改变业务执行结果。
2. selector contribution 必须使用命名空间 ID，只能选择当前非空端口，每个 firing 对每个端口最多消费一个 token。
3. Graph 冻结时绑定 selector 实现快照；缺失 contribution 必须在冻结阶段失败。
4. TaskConsumer 以 DeliveryResult 明确表达 ACK、RETRY 或 REJECT；broker lease、重投递和死信实现留给 backend。
5. keyed join、窗口和领域状态属于扩展 Node，不进入 Engine 的固定调度语义。

## 第十三条：公共行为必须可验证

1. Graph 冻结与执行约束必须有失败测试。
2. Event 提交、跨图连接、并发和错误传播必须有契约测试。
3. 三个替换协议必须有非默认实现组合测试。
4. 插件依赖、冲突、API 兼容、失败回滚、逆序关闭和默认装配路径必须有契约测试。
5. 示例必须只通过相同顶层 API 组合，并覆盖核心编排方式。
6. 文档不得把计划能力写成已实现能力。

## 第十四条：插件化止于内核语义

1. Graph、Node、Ports、Edge、Output、Event、Execution 的基本语义、冻结校验和错误契约属于微内核，不得由插件替换。
2. 部署能力和非本质策略通过具名 capability 扩展；默认实现必须与第三方实现经过同一 PluginHost 装配路径。
3. 插件必须声明 namespaced ID、插件版本、SPI 主版本、依赖与提供的 capability；宿主按依赖顺序 setup/start，
   并按逆序 stop。
4. 单例 capability 冲突、缺失依赖、循环依赖、SPI 不兼容和未兑现的 capability 声明必须在 Runtime 可用前失败。
5. 输入策略、Node Hook 和 Runtime Observer 使用统一贡献通道，但仍保留各自的强类型和权限边界。
6. 插件不得访问 Runtime 私有状态；新增扩展类型优先成为 capability，不得继续增加互不相干的全局注册表。

## 验证要求

行为变更应先运行相关测试，并在可行时运行完整检查：

```bash
uv run --with pytest pytest -q
uv run --with mypy mypy bricks
```

文档变更还应检查本地链接、Markdown 围栏和发生变化的 Mermaid 图。
