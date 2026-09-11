# 第十一章：从 dev 迁移爬虫插件和代理

本章对照 `dev` 的爬虫实现说明当前接口。当前版本不提供旧导入别名或兼容分支。
业务能力通过显式函数、客户端和 Graph 装配迁移，旧函数名相同也不代表调用语义完全相同。

| dev 入口或能力 | 当前入口与调整 |
| --- | --- |
| `air.Spider` 四个业务方法 | 仍为 make_seeds/make_request/parse/item_pipeline；由初始化和消费两张 Graph 执行 |
| `survey(*seeds, extract=...)` | 新实例调用同名手动消费入口，正常构造器提供业务配置；默认跳过 item_pipeline，保留插件和动态种子消费 |
| `events.on`、const、隐式 Context | `bricks.hooks.on`、hooks 常量；每个回调首参数显式接收 Context，业务插槽由 HookNode 执行 |
| `use(form, {...})` | 保留 func/args/kwargs/index/match 字段；func 与 match 必须是实际函数 |
| `plugins.make_seeds.by_*` | CSV、SQLite、Mongo、Redis、range 均已迁入；显式传入恢复 record |
| `by_csv` 的 `<TABLE>` SQL | 临时表明确命名为 data；参数使用 SQL 占位符，不替换脚本文本 |
| SQLite/Mongo 的 Bricks DB 包装器 | 使用原生 SQLite Connection 或 Mongo Collection；不恢复整个 DB 包装层 |
| `storage.to_sqlite` | 首参 Context，path 是数据库文件，table 是表名；每批独立连接事务 |
| `storage.to_mongo/to_redis` | 首参 Context，conn 是显式原生 Collection/客户端；不修改 Items |
| `storage.to_csv` | 首参 Context，检查表头；并发写入者显式共享 lock |
| Redis string 的 `$key`、拼接身份 | 使用 row_keys 的规范 JSON 摘要生成前缀键，业务记录不增删 `$key` |
| `Before.set_proxy` | 保留请求显式地址，支持 Air 固定地址或 Slot 管理器 |
| `Before.fake_ua` 的 `@chrome(...)` | 使用实际 factory 函数，例如调用已有 UA 库；不执行字符串表达式 |
| `After.show_response/bypass` | 显式 Context；bypass 可接收同步业务条件 |
| `After.conditional_scripts` | 条件与动作函数序列，按序或首个命中执行 |
| `scripts.is_success/turn_page/inject` | 已迁入，pre/post/flow/action 均使用明确函数，禁止字符串代码 |
| `raise signals.Retry/Success/Failure` | 使用 `bricks.signals`，由领域节点转换为图内路由或明确失败；不进入核心解释 |
| `BEFORE_PIPELINE` 中的 Skip | 跳过 pipeline，仍执行 AFTER_PIPELINE；其他 before 阶段按明确替代数据规则处理 |
| 启停、领取和错误插槽 | 以 HookContext 在真实边界同步执行；物理线程启停和 ON_* 状态标记不作为 Air 插槽 |
| `bricks.lib.proxies.*` | `bricks.proxies`；固定/API/Redis/Clash 供应源和 Manager 已迁入 |
| 全局 manager、线程模式 | 显式 Manager，操作必须提供 Slot；不建立全局或线程局部代理缓存 |
| `context.clear_proxy()` | 显式丢弃 Slot 中的请求代理并清除地址；不热切固定代理会话 |
| 代理 `ref` 字符串配置 | 直接构造供应源对象并传给 Spider 或 Manager |
| Clash 构造时改全局模式 | 改为显式 configure；拒绝占用期间切换共享出口 |
| Clash 订阅工具 | 独立 extract_nodes/fetch_subscription/cfg_tpl/subscribe，解析与联网分开 |

## 执行与可靠性调整

正常存储返回 None，后置插槽和整次 Graph 完成后才确认交付。
`raise signals.Success()` 是明确的提前完成，会立即退出回调并跳过未执行的后续业务步骤。
Context.success/failure/retry、ResponseAction 和 action/stop 标志已移除，不保留第二套控制方式。
存储插件异常仍使执行失败，不会因前面某一步正常返回而提前 ACK。
`context.submit()` 返回新 Work，默认按内容去重，不返回未来的 Context，不继承当前 Slot 或执行选项。
`raise signals.Retry()` 只在 after_make_request、before_request、download、after_request 有效，
回边经过重试前后插槽返回 before_request；同次图执行保留 Slot，重新领取任务不保证原 Slot。

初始化断点不是消费图每个步骤的持久快照。种子读取的稳定排序、Mongo 业务唯一索引、外部写入幂等、
CSV 并发锁和代理服务的出口稳定性由应用明确装配。
survey 不承诺无副作用：插件写入和动态提交仍然发生。它不启动初始化图；旧 attrs/modded 和类/实例双绑定不保留，
通过正常实例配置及方法重写装配。默认返回完成上下文，可按 extract 读取属性；显式注入队列时也消费其中已有任务。
SQLite 原生连接有线程亲和性，源迭代器若交给 Air，应由调用方使用适合串行跨线程推进的连接。

## 当前范围

本轮完成的是 Air、旧领域插件和代理管理工具的迁移；`form/template` 配置式爬虫、
旧爬虫 RPC/CLI、SQLite/RocksDB 任务队列、旧 DB 包装器和部分传输实现尚不属于当前公开能力。
不需要为了迁移一个插件恢复旧 Flow、动态脚本执行器或全局状态。

完整示例见 [Air 爬虫](08-air-spider.md)、[领域插件](09-domain-plugins.md)、[代理管理](10-proxies.md)
和[Hook 节点与 signals](12-hooks-and-signals.md)。

[返回目录](README.md)
