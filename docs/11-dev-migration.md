# 第十一章：从 dev 迁移爬虫插件和代理

本章对照 `dev` 的爬虫实现说明当前接口。当前版本不提供旧导入别名或兼容分支。
业务能力通过显式函数、客户端和 Graph 装配迁移，旧函数名相同也不代表调用语义完全相同。

| dev 入口或能力 | 当前入口与调整 |
| --- | --- |
| `air.Spider` 四个业务方法 | 仍为 make_seeds/make_request/parse/item_pipeline；由初始化和消费两张 Graph 执行 |
| `survey(*seeds, extract=...)` | 新实例调用同名手动消费入口，正常构造器提供业务配置；默认跳过 item_pipeline，保留插件和动态种子消费 |
| `response.extract(engine=..., rules=...)`、`response.get(expr)` | `response.extract(engine, rules)` 使用 `bricks.parsers` 规则语法；`get` 对 JSON 执行 JMESPath，缺失或 null 返回 default |
| `response.xpath/xpath_first/jsonpath/jsonpath_first/re/re_first` | 保留同名方法并新增 `css/css_first`；`*_first` 的 default 为第二个参数，选项传给对应解析器 |
| `Request(ok=..., max_retry=...)`、`$maxRetry` | 任务执行配置 `crawler.response.ok`、`crawler.retry.max`，或 form 的 `Download(ok=..., max_retry=...)`；耗尽一律进入 dead，不再按成功丢弃 |
| `task_queue.reverse/smart_reverse` | `queue.revive(id)` 恢复单个死信，`queue.revive_dead(limit)` 批量恢复，交付次数清零 |
| `LocalQueue/RedisQueue/SqliteQueue` | `MemoryTaskQueue`、`RedisTaskQueue(client, name)`、`SqliteTaskQueue(path, name)`；不保留 set/list/zset genre 与优先级插队 |
| `self.fetch(request)` | `context.fetch(request)` 复用当前下载器与 Slot 会话，异步下载器使用 `await context.afetch(request)`；不经过插槽与重试 |
| `context.success()` | 删除；存储正常返回即在后置插槽完成后确认，提前结束使用 `raise signals.Success()` |
| `form.Spider`、`template.Spider` | 合并为 `bricks.spider.form.Spider`，按 Download 分段并逐段持久化，见[配置式爬虫](13-form-spider.md) |
| `events.on`、const、当前 Context | `bricks.hooks.on`、hooks 常量；回调可显式接收 Context 或在调用期间使用同类 Context.get_context()，业务插槽由 HookNode 执行 |
| `use(form, {...})` | 保留 func/args/kwargs/index/match 字段；func 与 match 必须是实际函数 |
| `plugins.make_seeds.by_*` | CSV、SQLite、Mongo、Redis、range 均已迁入；显式传入恢复 record |
| `by_csv` 的 `<TABLE>` SQL | 继续支持并替换为临时 data 表；SQL 值参数使用占位符 |
| SQLite/Mongo 的 Bricks DB 包装器 | 使用原生 SQLite Connection 或 Mongo Collection；不恢复整个 DB 包装层 |
| `storage.to_sqlite` | 保留 path 表名、conn 数据库目标、items、row_keys、with_lock 顺序；conn 使用路径或原生 Connection |
| `storage.to_mongo/to_redis` | 保留 path/conn/items/row_keys 等参数角色，改用原生客户端；省略 items 时读取当前 Context |
| `storage.to_csv` | 保留 path/items/conn/encoding 顺序；原生 DictWriter 可注入，并发写入者显式共享 lock |
| Redis string 的 `$key`、拼接身份 | 使用 row_keys 的规范 JSON 摘要生成前缀键，业务记录不增删 `$key` |
| `Before.set_proxy` | 保留请求显式地址，支持 Air 固定地址或 Slot 管理器 |
| `Before.fake_ua` 的 `@chrome(...)` | 内置常用 UA 预设；复杂策略仍可注入 factory |
| `After.show_response/bypass` | 可直接 use；bypass 支持字符串、映射和 callable，省略时读取 `context.ok_rule`（执行配置 `crawler.response.ok`） |
| `After.conditional_scripts` | 可直接 use，缺省从执行配置 `crawler.response.scripts` 读取可信动态脚本 |
| `scripts.is_success/turn_page/inject` | 参数顺序贴近 dev；字符串与同步 callable 可混合，Type/Generator 恢复 flow 组合 |
| `raise signals.Retry/Success/Failure` | 使用 `bricks.signals`，由领域节点转换为图内路由或明确失败 |
| `BEFORE_PIPELINE` 中的 Skip | 跳过 pipeline，仍执行 AFTER_PIPELINE；其他 before 阶段按明确替代数据规则处理 |
| 启停、领取和错误插槽 | 以 HookContext 在真实边界同步执行；物理线程启停和 ON_* 状态标记不作为 Air 插槽 |
| `bricks.lib.proxies.*` | `bricks.proxies`；固定/API/Redis/Clash 供应源和 Manager 已迁入 |
| `context.clear_proxy()` | 显式丢弃请求代理与管理器选择；自动会话模式下下一次请求取得新地址并路由到对应会话 |
| 代理 `ref` 字符串配置 | 直接构造供应源对象并传给 Spider 或 Manager |
| Clash 构造时改全局模式 | 改为显式 configure；拒绝占用期间切换共享出口 |
| Clash 订阅工具 | 独立 extract_nodes/fetch_subscription/cfg_tpl/subscribe，解析与联网分开 |
| 爬虫 `Rpc`、`$futureType` | `spider.call(seed)` 进程内同步调用，`spider.serve(...)` 提供 HTTP/JSON-RPC 服务（`service` extra）；返回字典形状与 dev 不同，见[同步调用与 RPC 服务](08-air-spider.md#同步调用与-rpc-服务) |
| 统计计数器 | 安装 `interlace.metrics.Metrics` 与 `bricks.plugins.metrics.crawler_metrics` 插件，见[指标插件](09-domain-plugins.md#指标插件) |
| `disable_statistics` | 删除；不安装指标插件即不采集 |

## 执行与可靠性调整

正常存储返回 None，后置插槽和整次 Graph 完成后才确认交付。
`raise signals.Success()` 是明确的提前完成，会立即退出回调并跳过未执行的后续业务步骤。
Context.success/failure/retry、ResponseAction 和 action/stop 标志已移除，不保留第二套控制方式。
存储插件异常仍使执行失败，不会因前面某一步正常返回而提前 ACK。
`context.submit(..., affinity="current")` 返回稳定 Work，原子持久化并优先携带当前 SlotLease；any 只持久化等待领取。
`turn_page(call_later=False)` 映射 current，True 映射 any。两种模式都逐任务 ACK，
进程异常后通过持久租约恢复，但恢复任务不保证回到原 Slot。
`raise signals.Retry()` 只在 after_make_request、before_request、download、after_request 有效，
回边经过重试前后插槽返回 before_request；同次图执行保留 Slot，重新领取任务不保证原 Slot。

`on_retry(context)` 可能运行在首次下载之前，因此 `context.response` 并非始终可用。
有界重试耗尽会让 `run()` 在已接受任务排空后抛出失败；单个 Failure（例如 404）也会使本次运行失败。
构造 `strict=False` 时两者仍进入死信，但只记入 `RunReport.rejections`，不抛出。
`survey(extract=...)` 对提前 Success 尚未产生的 request、response 或 items 返回 None。

初始化断点不是消费图每个步骤的持久快照。种子读取的稳定排序、Mongo 业务唯一索引、外部写入幂等、
CSV 并发锁和代理服务的出口稳定性由应用明确装配。
survey 不承诺无副作用：插件写入和动态提交仍然发生。它不启动初始化图；旧 attrs/modded 和类/实例双绑定不保留，
通过正常实例配置及方法重写装配。默认返回完成上下文，可按 extract 读取属性；显式注入队列时也消费其中已有任务。
SQLite 原生连接有线程亲和性，源迭代器若交给 Air，应由调用方使用适合串行跨线程推进的连接。

## 当前范围

本轮完成的是 Air、配置式 form（合并旧 template）、SQLite 任务队列、死信恢复、旧领域插件、代理管理工具、
同步调用与 RPC 服务及指标插件的迁移；旧 CLI、RocksDB/Smart/Cache 任务队列、优先级插队、旧 DB 包装器和部分传输实现
尚不属于当前公开能力。
动态脚本和 Flow 已作为领域插件恢复，但不恢复动态类路径、旧数据库包装器或进程级全局状态。

完整示例见 [Air 爬虫](08-air-spider.md)、[领域插件](09-domain-plugins.md)、[代理管理](10-proxies.md)
和[Hook 节点与 signals](12-hooks-and-signals.md)。

[返回目录](README.md)
