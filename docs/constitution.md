# Bricks Engine 宪法

状态：Normative  
适用范围：Engine 核心、核心扩展和所有领域框架  
解释原则：若实现便利与本宪法冲突，以本宪法为准；确需偏离时必须记录设计决策。

## 序言

Bricks Engine 的价值不在于内置功能数量，而在于提供少量、稳定、正交的图执行原语。核心必须足够
小，使 Agent、ETL、Spider 以及尚未出现的领域都能在其上生长；核心也必须足够严格，使这些领域
不会通过隐式行为互相污染。

本宪法约束 Engine 如何演进。它不是 API 参考，而是判断一个概念是否应该进入核心的最高原则。

## 第一条：Less is more

1. 核心只保留无法通过现有原语组合得到的概念。
2. 新抽象进入核心前，必须至少有两个不同领域的真实用例。
3. 不得以“未来可能需要”为唯一理由增加接口、状态或生命周期 Hook。
4. 删除错误抽象优先于为错误抽象增加兼容层。
5. 第一版核心应保持小型、可阅读，并能由单人完整理解其执行循环。

当前核心词汇为：

```text
Graph、Node、Edge、Flow、Endpoint、RunRequest、Run、Task、
Ports、NodeInputs、InputPolicy、InputGroup、InputSelection、InputToken、
NodeResult、Output、GraphOutput、ExecutionContext、Engine
```

增加新的核心名词必须说明为什么上述原语无法组合出所需能力。

## 第二条：核心提供机制，领域提供策略

1. Engine 只负责执行、路由、生命周期和扩展边界。
2. Engine 不得依赖 Agent、ETL、Spider 或其他领域包。
3. Engine 中不得出现 LLM、Prompt、Dataset、Request、Item 等领域类型判断。
4. 条件、重试、限流、缓存和序列化策略不得以硬编码分支进入执行循环。
5. 领域框架可以依赖 Engine；Engine 不得反向依赖领域框架。

禁止出现：

```python
if node.type == "llm":
    ...
elif node.type == "spider.request":
    ...
```

## 第三条：定义与实例必须分离

1. Graph、Flow、Edge 和 Node binding 是静态定义。
2. Run 和 Task 是动态实例。
3. Node 是行为定义，Task 是 Node 的一次执行。
4. Node ID 属于 Graph 中的位置，不属于 Node 对象本身。
5. 一个 Node 行为允许绑定到多个 Node ID。
6. Node 不得保存某次 Run 或 Task 的可变运行状态。

## 第四条：Graph 在运行期间不可变

1. Graph 可以通过 Builder 构建，但进入 Engine 前必须冻结。
2. Run 执行期间不得原地修改 Graph 拓扑。
3. 动态发现的数据必须表达为新的 Output、InputToken 和 Task，而不是复制或修改 Graph Node。
4. 真正的动态图需求必须通过版本化 Graph 或独立扩展设计，不得破坏基础 Graph 语义。

## 第五条：数据流必须显式

1. Node 必须通过 Ports 声明 typed input/output ports。
2. Node 只通过 NodeResult 产生 Output。
3. Output 必须包含 value 和 output port。
4. Edge 只描述 `source.output_port -> target.input_port`，不得执行领域逻辑。
5. 条件路由由 Node 将判断结果转换为 output port。
6. 一个 Output 匹配多条 Edge 表示广播；多个 Output 表示多个独立数据项。
7. 未命中当前 Flow Endpoint 且没有下游 Edge 的 Output 必须产生明确错误。
8. 静默丢弃 Output 是禁止行为；主动过滤必须使用 NodeResult.empty()。
9. InputPolicy 必须统一表示为组内 AND、组间 OR，并返回明确的 InputSelection。
10. InputPolicy 只能读取 token 数量和顺序，不得读取领域 value。
11. Node 声明的每个 input port 必须被至少一个 InputGroup 覆盖。
12. 每次基础 firing 从选中组的每个 port 消费一个 FIFO InputToken。
13. 端口类型不兼容时必须拒绝 Edge，不得进行隐式类型转换。
14. Task 只能在策略选中的输入完整就绪后创建。

## 第六条：Flow 是 Graph 的公共执行接口

1. 一次 RunRequest 必须选择一个 Flow。
2. Flow 只定义稳定名称、入口和终止 Endpoint。
3. Flow 不得重复保存完整节点路线。
4. 路线由 Graph Edge 和 Node Output port 共同决定。
5. 同一 Graph 的不同 Flow 可以共享任意部分路径。
6. 同一 `(node_id, port)` 可以在一个 Flow 中终止，在另一个 Flow 中继续传播。
7. 差异过大的流程应优先拆为多个 Graph，而不是向 Flow 或 Edge 添加大量特例。

## 第七条：输入与上下文必须分离

1. RunRequest 包装 Flow 选择、初始 NodeInputs 和运行级 metadata。
2. Node 只接收只读 NodeInputs 和只读 ExecutionContext。
3. Node 不得被迫从万能字典中提取 Engine 元数据。
4. ExecutionContext 不得成为任意共享可变 state。
5. ExecutionContext 不得无边界演变为 service locator。
6. 新增 Context 能力必须有明确生命周期、并发和测试语义。

## 第八条：渐进式输出一经路由即提交

1. NodeResult 可以渐进式产生 Output。
2. Engine 必须逐项消费，不得默认完整缓存流。
3. Output 一经 Engine 接收并路由，不因后续生成失败而撤回。
4. 渐进式执行默认是 at-least-once 风格，不承诺 exactly-once。
5. 重试、去重、幂等和 checkpoint 应由明确插件或领域策略提供。
6. 需要原子性的 Node 必须在完成计算后再暴露 Output。

## 第九条：图内输出与观测事件必须分离

1. Output 用于推动 Graph 执行或形成 GraphOutput。
2. Event 用于描述已经发生的事实以及提供日志、进度和观测。
3. 不得让生命周期 Event 隐式驱动 Graph 路由。
4. 不得默认将日志、心跳、下载进度或 LLM token 转换为下游 Task。
5. 领域明确需要时，可以主动将任意数据建模为 Output。

## 第十条：失败必须明确

1. Node 的意外失败通过异常表达，不伪装成普通 Output。
2. 路由失败、图校验失败和领域执行失败必须是可区分的错误类别。
3. Engine 不得吞掉异常并假装 Run 成功。
4. Retry 是执行策略，不是 NodeResult 的默认状态。
5. 部分 Output 已提交后发生的失败必须保留可观测事实。

## 第十一条：扩展依赖小接口

1. 不建立包含安装、启用、配置和所有生命周期方法的万能 Plugin 基类。
2. 扩展通过 Node 子类、对象组合、decorator、middleware 或小型 port 接口完成。
3. 每个 port 应只表达一个稳定职责。
4. 只有存在真实替代实现时，才提炼基础设施接口。
5. 默认本地实现不得泄漏到核心抽象中。
6. 基础设施适配器依赖 Engine port，Engine 不依赖具体数据库、队列或网络库。

## 第十二条：继承表达类型，组合表达能力

1. 领域节点可以继承 Node。
2. 日志、超时、重试、缓存、指标等横切能力优先使用组合。
3. 不得通过多层能力型子类制造组合爆炸。
4. Node 的公开契约应小于其领域实现。
5. 可以在不修改 Engine 执行循环的前提下增加新 Node 类型。

## 第十三条：类型属于正确的层级

1. Engine 的 input 和 output value 保持领域无关。
2. 领域框架负责提供类型明确的公共 API。
3. Engine 不为了获得静态类型提示而依赖领域模型。
4. 序列化能力不得反向塑造所有运行时对象。
5. 大对象应由领域层使用引用或 artifact 表达，Engine 不要求其进入共享内存状态。
6. 基础端口类型使用普通 Python class，并按 `issubclass(source, target)` 判断兼容性。
7. typing 泛型、自定义结构类型和隐式 coercion 在真实需求形成独立 TypeSystem 前不得进入核心。

## 第十四条：公共行为必须可验证

1. 每条核心语义必须有契约测试。
2. 新抽象至少由两个不同领域的最小示例验证。
3. 测试优先验证可观察行为，不依赖私有实现结构。
4. 渐进式输出必须测试部分提交、异常和取消场景。
5. 多 Flow 必须测试共享路径、不同入口和不同 Endpoint。
6. Graph.freeze() 的每条约束必须有对应失败测试。

## 第十五条：实现必须简洁一致

1. 可执行源码中的每个函数和方法都必须包含中文 docstring。
2. docstring 必须说明参数含义；存在返回值或公开异常时，还必须说明返回值和异常条件。
3. 相同语义的校验、转换和错误处理必须复用同一实现，不得散落多份近似代码。
4. 共享内部逻辑应放入职责明确的私有模块，不得为了复用而扩大公共 API。
5. 同类对象的字段校验、方法结构和异常风格必须保持一致。
6. 新抽象应减少调用方和实现方的重复代码；仅用于包装名称、不增加语义的层级不得进入核心。

## 第十六条：文档必须与实现同步

1. 每完成一个可独立使用的开发阶段，必须同时输出对应的使用文档。
2. 使用文档必须以通俗语言解释概念，并包含可以直接验证的示例。
3. 文档必须明确区分已经实现、计划实现和明确不支持的能力。
4. 公共 API、错误行为或执行语义变化时，文档必须在同一次变更中更新。
5. 架构设计文档不能代替使用文档；源码注释也不能代替完整示例。
6. 文档示例必须纳入人工检查或自动化测试，避免长期失效。

## 第十七条：演进必须保留清晰性

任何核心变更必须回答：

1. 它解决了哪两个真实领域用例？
2. 为什么现有原语无法组合解决？
3. 它属于机制还是策略？
4. 它是否让 Engine 知道了领域概念？
5. 它对同步、异步、并发和流式执行分别意味着什么？
6. 它的失败、取消和部分完成语义是什么？
7. 它是否要求 Graph、Run 或 Node 保存新的可变状态？
8. 它能否作为 Engine 外部的插件实现？
9. 它会增加多少公共 API，未来如何删除或替换？
10. 最小契约测试是什么？

不能清楚回答这些问题的变更，不应进入核心。

## 修订规则

1. 本宪法可以修订，但修订必须显式记录原因、替代方案和影响。
2. 修订不得仅由单一领域的便利性驱动。
3. 修订应优先简化核心，而不是为已有复杂度辩护。
4. 与本宪法冲突的实验能力必须放在 Engine 核心之外，并明确标注实验状态。
