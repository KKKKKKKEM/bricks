# Air 爬虫基类

`bricks.spider.air.Spider` 把已有下载器、任务队列和 Interlace 装配为代码式爬虫。
业务侧通过四个方法开发，响应成功检查、日志和默认代理使用可复用插件注册。

## 完整示例

```bash
uv run --locked python -m examples.air_spider --output products.sqlite3
```

[完整代码](../examples/air_spider.py) 启动本地演示站点，抓取两张列表页和两张详情页，
重复链接通过稳定任务 ID 去重，详情在 SQLite 中按 URL 幂等写入。每次存储事务提交后才确认交付。
示例的固定 Cookie 是本地演示站点协议的一部分，真实站点的登录态由应用显式装配。

## 业务参数与方法

```python
from bricks.spider import air
from bricks.downloaders.curl_cffi import CurlCffiDownloader
from bricks.queues.redis import RedisTaskQueue

# redis_client 由应用创建，停止爬虫之后再关闭。
task_queue = RedisTaskQueue(redis_client, "news-run-1")
spider = MySpider(
    concurrency=4,
    downloader=CurlCffiDownloader(),
    task_queue=task_queue,
    queue_name="news-run-1",
    proxy=None,
    forever=False,
)
spider.run()
```

`MySpider` 继承 `air.Spider`。业务参数沿用 concurrency、downloader、task_queue、queue_name、proxy、forever 的命名：

| 参数 | 契约 |
| --- | --- |
| concurrency | 当前进程的并发执行数，默认 1 |
| downloader | 实现 fetch 的同步或原生异步实例；默认临时 curl_cffi 下载器 |
| task_queue | 当前 TaskQueue 协议的存储；不传则创建独立内存队列 |
| queue_name | 注入队列时必须等于其 name；缺省取队列名或爬虫类完整名称 |
| proxy | 固定地址、ProxyProvider 或 Manager，Request 的显式代理优先；不接受动态 ref 配置 |
| forever | True 时空队列仍保持消费，默认 False |
| max_retry / retry_delay | 额外重试预算（默认 2）和等待秒数（默认 0）；下载前的 Retry 也消耗预算 |
| options / limits | 默认 JSON 执行配置及单种子 ExecutionLimits |
| lease_seconds / init_timeout | 交付和初始化租约秒数，以及初始化图的执行超时 |
| poll_interval | 队列空轮询间隔秒数，默认 0.05；任务完成会唤醒领取循环 |
| slot_session_reuse | 是否自动装配并复用每 Slot 同步会话，默认 True；False 使用下载器的独立请求路径 |
| slots | 借用的本地 SlotProvider，资源归调用方管理 |

这些配置应在打开实例前确定，不在运行中修改。不同进程使用同一队列名称时，必须注册相同版本的业务流程和配置。

| 业务方法 | 返回值及职责 |
| --- | --- |
| make_seeds(context, **kwargs) | kwargs["record"] 是恢复记录；可返回 None、单个 dict、种子列表或同步迭代器，迭代器也可产出一批种子 |
| make_request(context) | 返回当前 Request；默认执行 Request(**context.seeds) |
| parse(context) | 返回 Items 或 list[dict]；默认返回请求和响应快照 |
| item_pipeline(context) | 执行存储并正常返回 None，继续后置插槽与确认收尾 |
| on_retry(context) | 可选的业务重试准备，响应可能尚不存在；返回 None，预算和调度由图负责 |

`on_retry(context)` 可能在首次下载前运行，因此 `response` 尚不存在；重试耗尽错误包含最后响应状态码，或无响应时的重试触发类型。

业务方法支持普通 def 与 async def；异步 make_seeds 应返回同步种子集合，不使用异步生成器。
同步方法由引擎的执行线程调用，Awaitable 交给引擎的异步运行器等待，不将全部同步业务放入异步循环。
默认 item_pipeline 正常返回 None，不替应用持久化数据，实际爬虫应重写它。
Request、Response、Items 和 TaskQueue 使用当前包的模型与协议，不按字符串动态加载函数或按名称注入任意参数。
领域 scripts 插件可以显式执行可信业务表达式，但不改变核心对象的构造和所有权。

## 两张 Graph

```text
初始化：before_make_seeds → make_seeds → after_make_seeds → next_seed
        next_seed → before_put_seeds → submit → after_put_seeds → checkpoint → next_seed

消费：before_make_request → make_request → after_make_request
      → before_request → download → after_request
      → before_parse → parse → after_parse
      → before_pipeline → pipeline → after_pipeline → complete

重试：retry → before_retry → retry_prepare → after_retry → retry_wait → before_request

子任务：context.submit(seed, affinity="current") → 持久化并本地优先执行
```

初始化逐项推进种子源，每个种子或批次提交后才推进下一次迭代并保存记录，能够与消费者同时工作。
恢复时用稳定种子 ID 重放最后一批，已经接受的前缀不会因为后续失败而撤销。
调用线程等待初始化 Execution 时定期续租，不为每条种子创建初始化心跳线程。

消费图在进程内传递请求、响应及结果载体，跨队列只传 JSON 种子与明确的执行配置。
队列的 Work.inputs 是种子对象，图入口把它绑定到 seeds 端口。
每个 before/after 插槽是独立 HookNode，普通业务操作也是独立 Node。
HookNode 捕获领域 signals 并输出到已声明的路由；不再通过 check_response 或 Context 标志延后决策。
完成节点只输出条数、下载次数和重试次数，避免把整个响应体作为终端结果长期保留。
详细插槽、信号和图内外边界见[Hook 节点与 signals](12-hooks-and-signals.md)。

`build_init_graph()` 装配初始化流程；`build_crawl_plan(survey=False)` 返回消费执行计划。
共享定义包含正式 `pipeline` 和无默认存储的 `survey_pipeline` 两个备选分支，计划只选择其中一个，
并以 `outputs={("complete", "summary")}` 校验出口，避免重试或跳过端口因裁剪变成普通结果。
`build_graphs()` 用公开的 `Graph.compose` 将计划选定的节点、入口和连接装配成队列消费图。
插槽声明在打开时固定，因此三个 `build_*` 方法须在 `open()` 或 `with spider:` 之后调用，之前调用抛出 RuntimeError。
两种分支复用请求、解析、前后 Hook 与重试节点，资源和确认仍由正常队列执行链管理。

计划提供 `describe()` 查看入口、连接和出口，也可从 `plan.graph.plan(...)` 派生独立入口或连接选择。
中间节点接收图内任务载体；入口开放不等于自动转换 Request/Response，也不提供消费步骤的持久恢复。
不要直接无计划执行包含两个存储分支的共享定义；已注册的消费图只包含本次选中的一个分支。

## 手动消费 survey

本地 HTTP 示例会继续消费列表产生的详情和分页，但不创建 SQLite 文件：

```bash
uv run --locked python -m examples.air_spider --survey
```

```python
spider = MySpider(concurrency=2)
results = spider.survey(
    {"url": "https://example.com/list"},
    extract=["seeds", "response", "items"],
    timeout=60,
)
```

survey 在尚未打开的新实例上消费手动种子，返回后关闭实例。业务配置直接通过正常构造器传入；
不使用旧版动态类修改、类与实例双绑定或 `modded` 参数。
不调用 `make_seeds`，因此初始化 Hook、初始化租约和生产断点不参与这次操作。
只将默认 `item_pipeline` 替换为空操作：BEFORE/AFTER_PIPELINE 插件、其他插件、请求、重试和解析仍正常执行。
`context.submit()` 提交的分页或详情会继续入队消费并收集结果，插件自身的写库或其他外部操作也会真实发生。
survey 是手动消费，不是副作用沙箱。默认内存队列按实例隔离；显式注入共享队列时会消费已有任务，不能与正式消费者
共享同一任务队列却期待仅处理本次种子。

省略 extract 或传空序列返回完成节点的 Context 列表；指定字段时返回属性映射，均按完成顺序排列。
提前 Success 产生的结果可能尚未经过下载或解析；提取缺失属性时对应值为 None，不影响批次中其他结果。
run() 会在已接受任务排空后报告任务失败，单项 Failure（例如 404）也会使本次 run() 抛出错误。
异步 Graph 路径的 ERROR_OCCURRED Hook 同步运行在共享事件循环线程；阻塞型回调会暂停其他 Slot 的异步任务。
返回的 Context 仅用于读取结果，执行和自建资源已经结束；不是继续提交、发事件或复用已关闭会话的句柄。
后置插件失败仍传播异常且不 ACK。forever=True 不适合一次性结果收集，survey 会明确拒绝。

## Context 与种子提交

`air.Context` 提供 seeds、request、response、items、target、task_queue、queue_name、form、attempt 和 retries。
request/response/items 在对应阶段完成之前不可读取。数据在同一图内沿 Output 传递，不保存在共享 Spider 或 Node 上。
seeds 的修改只影响当前交付，不回写原始队列任务或自动重算 ID。

业务方法和 Hook 实际执行期间也可以调用 `Context.get_context()` 取得当前消费 Context；初始化和生命周期插件分别使用
`InitContext.get_context()` 与 `HookContext.get_context()`。绑定使用 ContextVar 隔离同步调用链和异步任务，跨越 await
仍指向同一次 firing，回调结束后立即恢复；在回调外或使用错误的 Context 类别会抛出 RuntimeError。
显式参数仍适合需要清楚依赖或复用测试的函数，当前 Context 适合直接 use 的简洁插件，两者可按场景混用。

`context.install(name, value, *, nx=False)` 将进程内业务资源写入当前 Slot，`nx` 只能按关键字传入；`context.obtain(name, default=None)`
只读取 Slot 业务命名空间。`concurrency=N` 时框架维护 N 个长期 Slot，作用类似 dev 的 N 份线程变量，
但资源身份不绑定物理线程：同一 Slot 即共享，不同 Slot 即隔离。本地优先子任务继续使用原 Slot；Slot 归还池后，
后续根任务若再次取得它，也能看到尚未清除的资源。

```python
context.submit({"url": next_url})
context.submit({"url": related_url}, affinity="current")
context.submit({"url": distributed_url}, affinity="any")
context.submit({"url": first_url}, {"url": second_url})
context.submit({"url": detail_url}, id="article:123", options={"crawler.session.reuse": False})
```

submit 返回已接受或去重命中的 Work 列表，不返回尚未执行的 Context。两种模式都先持久化并使用 Spider 默认配置
加本次 options 覆盖；affinity 只决定首次调度是否优先携带当前 Slot，不改变 Work 内容和恢复规则。
单个 id 只能对应一条提交。此入口不提供进程内递归分支或 attrs 搬运运行时对象的路径。
通用跨图业务事件仍通过核心 Context.emit 显式发出，并由应用在 Runtime 上注册目标图和路由。
当前 QueueBackend 支持 Interlace 本地 Event 的 SlotLease 延续；跨进程路由仍重新建立 Slot。

`context.submit(seed, affinity="current")` 是通用子任务入口：先原子持久化并领取新 Work，再由 Interlace 本地调度器
携带当前 SlotLease 执行。它适用于分页、详情拆分、同账号多接口等场景；每个子任务独立 ACK，进程异常后由
任务租约恢复，但恢复执行不保证原 Slot。`affinity="any"` 只持久化为 pending，允许任意 Worker 和 Slot 领取。
`scripts.turn_page(call_later=False)` 映射 current；call_later=True 映射 any。

跨图派生使用相同口径：`context.emit("detail.parse", payload, affinity="current")` 让目标 Work 持久化并优先复用
当前 Slot，any 则普通发布等待目标通道领取。目标事件仍须通过 Runtime 注册明确 route；跨图 Event 不自动
继承源 options，领域配置应放入 payload 或目标 Graph 配置。当前 execution 内无需声明复用，直接通过
`context.install/obtain` 访问 Slot 资源。

流程控制使用 `from bricks import signals` 与 `raise signals.Retry()`、`raise signals.Success()`、
`raise signals.Failure()`。信号立即退出回调；Success 跳到 complete，Failure 明确拒绝任务。
普通存储成功只需返回 None，后续 AFTER_PIPELINE 插槽全部完成后才确认。
Success 是显式提前结束，会跳过尚未执行的存储与后置插槽，不应用于“继续执行但预先登记成功”。
队列 acked 表示交付完成，业务数据及其幂等状态由存储步骤负责。

`air.InitContext` 提供 target、task_queue、queue_name、seeds、form 和 record。
make_seeds 的 kwargs["record"] 与开始生产时的 context.record 为同一个恢复对象；
在产出种子前推进记录，框架在提交成功之后保存它。更换 context.record 对象时，已有的 Python 局部引用不会随之更换。

通过子类的 `Context = MyContext` 与 `InitContext = MyInitContext` 替换领域上下文。
消费 Context 的 make(base, inputs, target) 可自行实现构造逻辑，最终通过核心 Context(parent=base) 组合执行能力。
每次 firing 都创建新对象；工厂看到的是 enter Hook 之前的输入，节点会跟随核心 Hook 对端口的显式替换绑定最终业务数据。

## 插件注册与复用

响应检查、动态脚本、响应展示、UA 预设及默认代理分别位于 `bricks.plugins.on_request.After.bypass`、
`After.conditional_scripts`、`After.show_response`、`Before.fake_ua`、`Before.set_proxy`。
Spider.install 默认通过 use 注册这些插件。

CSV/SQLite/Mongo/Redis 存储、种子源、分页动作和条件插件见[领域插件](09-domain-plugins.md)。
固定、API、Redis、Clash 代理与阈值回收见[代理管理](10-proxies.md)。

```python
from bricks import hooks
from bricks.plugins import on_request

spider.use(
    hooks.AFTER_REQUEST,
    {
        "func": on_request.After.bypass,
        "kwargs": {"ok": is_success},
        "index": 0,
    },
    {
        "func": on_request.After.show_response,
        "kwargs": {"handle": print, "fmt": 1},
        "index": -10,
    },
)
```

is_success(context) 是应用定义的同步布尔判断。False 抛出 Retry；True 正常继续，
随后默认校验仍会执行。因此展示和动态动作等插件应排在会中断流程的判断之前。
默认 bypass 对 2xx 正常返回，对 -1、429、5xx 抛出 Retry，其余抛出 Failure。
因此同一个判断函数可以通过参数注册到多个爬虫，也可在不同子类的 install 中复用。
show_response 保留 handle、fmt 参数，默认使用标准 logger.debug，fmt=0 为单行摘要，fmt=1 为多行元数据。

use(form, *events) 接受 Task 或包含 func、args、kwargs、index、match 的字典；
同阶段入口和出口都按 index 升序执行，同索引保持声明顺序。
回调可在首个参数显式接收领域 Context，也可以不声明 Context 参数并在函数内调用对应类型的 `get_context()`；
后者尤其适合直接注册的通用插件。若函数签名同时兼容两种形式，优先显式传入 Context；后续仍是 args 和 kwargs，
不会按参数名称注入其他运行时对象。match 使用相同的 Context 传入规则，但仍必须同步返回 bool。
注册参数固定外层容器，内部可变值和注入资源归调用方管理。

也可以使用 `@hooks.on(hooks.AFTER_REQUEST, index=10, match=condition)` 声明方法。
支持实例方法、staticmethod、classmethod 与继承覆盖；子类同名方法会替换父类声明。
图内回调可同步或异步，普通返回值必须为 None，改变流程时抛出 signals；match 必须同步返回 bool。
混合回调链始终串行等待，不保证链内回调处于同一操作系统线程，不应依赖线程局部资源。
Hook 自身的 match 不支持表达式字符串或动态函数路径；需要动态表达式时注册 `bricks.plugins.scripts`
或 `After.conditional_scripts`。当前 Context 只在回调作用域内可见，不是事件或资源注册表。

异步 Graph 路径的 `ERROR_OCCURRED` 同步运行在共享事件循环线程；阻塞型错误钩子会暂停所有 Slot 的异步工作。
`run()` 在排空已接受的任务后报告失败；一项失败任务（例如 HTTP 404 被 bypass 判为 Failure）会使整体运行抛错。

图内支持 BEFORE/AFTER_MAKE_SEEDS、BEFORE/AFTER_PUT_SEEDS、BEFORE/AFTER_MAKE_REQUEST、
BEFORE/AFTER_REQUEST、BEFORE/AFTER_RETRY、BEFORE/AFTER_PARSE、BEFORE/AFTER_PIPELINE。
BEFORE_START、BEFORE_CLOSE、ERROR_OCCURRED、BEFORE/AFTER_GET_SEEDS 位于真实非图边界，使用同步 HookContext。
领取后的 AFTER_GET_SEEDS 在本地 Slot 已分配、Graph 尚未开始时执行，并与同 Slot 的 Graph execution 共用 Interlace `SlotLease.execution()` 串行边界。
若父 Graph 正持有该 Slot，子任务 Hook 会等父 Graph 退出后再运行；父 Graph 不应同步等待依赖该子 Hook 的信号。
use 应在打开前调用；插槽在 BEFORE_START 运行前已经固定，在该回调或之后调用 use 会抛出 RuntimeError。
install 在首次打开时执行一次，可调用 super().install() 扩展默认插件，也可完全自行注册。
完全覆盖 install 时，响应是否接受由自行装配的插件决定；无信号的正常返回沿图的正常边继续。

## 运行与资源

```python
combined.run()                                    # 完整生产与消费实例
producer.run("init")                              # 只初始化的实例
worker.run("spider", kwargs={"timeout": 60})        # 只消费已有任务的实例
```

每次示例调用使用独立 Spider 实例；关闭后的实例不可重新打开。
需要分开运行两阶段时，可以在同一个 with Spider(...) 作用域中调用 run_init()、run_spider()。
forever=True 的消费者在空队列时仍保持运行，外部 close 会通知等待入口退出，再停止领取并清理当前交付。
初始化失败会记录断点并允许已经提交的种子排空，原始异常仍向调用方传播。
InitRecord 表示生产状态，RunReport.counts 表示当前共享队列快照，不替代各 Worker 的异常收集或外部生产者的结束协议。

未注入 slots 且同步 downloader 支持 SessionDownloader 时，Air 按 concurrency 自动创建长期 Slot 和独立 Session，
并在执行停止后关闭会话；爬虫作者无需构造 SlotPool 或重写 build_download_node。
异步 Session 与自定义多资源 Slot 可参考[会话示例](../examples/crawler_session.py)显式装配。
注入的 SlotPool、显式会话和 Redis 客户端由调用方在 Spider 停止后关闭；
异步客户端必须在使用它的事件循环中关闭，基类不会跨线程关闭调用方的浏览器会话。
使用自建 Slot 的托管代理时，Spider 在执行停止后归还占用；借用 Slot 的代理仍由装配方归还。

同步取消仍是协作式语义，close 不强杀阻塞函数。不要从自身业务或插件回调调用 run/open/close 等阻塞生命周期方法，
框架会拒绝这类重入；回调需要结束当前种子时显式抛出 signals.Success 或 signals.Failure。

## 分布式验证

```bash
BRICKS_TEST_REDIS_URL=redis://127.0.0.1:6379/0 uv run --locked pytest tests/crawler/test_air.py -q
```

测试使用随机前缀，验证两个独立进程消费同一 Redis 队列并下载本地 HTTP 数据，存储完成后才确认。
仅初始化进程关闭时不会等待留给其他进程消费的任务。真实部署仍按[任务队列](07-task-queues.md)的至少一次交付边界实现幂等。

[返回目录](README.md)
