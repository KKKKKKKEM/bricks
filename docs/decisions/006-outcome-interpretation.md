# 006：Outcome 通过组合解释，不扩张 Machine 类型分支

## 决策

Machine 只维护事件迁移的阶段顺序，不直接决定每个 Outcome 类型的含义。所有结果通过
`OutcomeInterpreter` 解释为两个最小指令：

```text
CONTINUE  当前 Effect 已完成，继续下一个迁移阶段
STOP      当前 Control 已接管流程，停止本次迁移的后续阶段
```

默认 `OutcomeRegistry` 注册 Update、Emit、Wait、Retry、Next、Fork、Stop 和 Fail。
领域适配器使用 `with_handler()` 返回一个新 Registry，不修改共享默认值。

## 原因

如果每增加 Tool、Memory、HumanInput、Delegate 等领域语义都修改 `Machine._apply()`，
核心会逐渐依赖 Agent 或工作流领域。注册式解释让内建能力和外部能力遵循同一个机制，
同时保留普通 Python Callable 作为扩展入口。

Registry 选择不可变组合，而不是全局可变注册中心，原因是：

- 一台 Machine 的完整语义可以从构造参数看出。
- 多个领域框架可以在同一进程使用不同解释规则。
- Fork 子运行继承父运行的解释器，不依赖导入顺序或全局状态。
- 测试可以构建局部 Registry，不需要清理全局注册项。

## 边界

- handler 负责解释一个 Outcome，不负责选择 Transition。
- Effect 使用 `CONTINUE`；会暂停、终止、调度后续工作的 Control 使用 `STOP`。
- handler 通过窄 `OutcomeRuntime` 读取 ContextView，并使用 `update()`、`publish()` 或
  `stage_effect()` 等显式能力；它不能直接推进 Machine。
- ContextView 只保证顶层映射不可写，不承诺把领域对象递归深拷贝为深层不可变值；跨线程或
  跨进程传递应使用 Context 快照。
- 异步 handler 必须通过异步 Machine 入口执行。
- `outcome_handlers` 仅作为兼容入口保留，新扩展使用 `OutcomeRegistry`。

## 结果

Machine 的同步和异步迁移使用同一个 Transition frame，只消费解释器指令。新增领域语义
不需要在核心增加 `isinstance` 分支，也不需要新增节点基类或全局插件系统。
