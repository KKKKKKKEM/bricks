# 002：Machine 保持为最小运行核心

## 决策

Machine 只负责一次运行的 Context、事件迁移、节点行为和 Outcome 应用。持久化、
响应式路由、Fork/Join、策略和领域基础设施通过组合对象或协议接入。

## 原因

如果所有能力都进入 Machine，任何一个功能变化都会扩大核心分支并增加测试组合。把
这些能力放在 `PersistenceBinding`、`ReactiveRuntime`、`ForkController` 和策略对象
之外，可以保持核心生命周期清晰，也允许替换存储、执行器和外部传输。

这不是为了拆分文件，而是为了让每个对象只有一个主要变化原因。
