# 任务队列

任务队列属于爬虫装配层。`bricks.queues` 提供存储协议、内存实现、交付与初始化租约，
`bricks.queues.sqlite.SqliteTaskQueue` 提供同机多进程共享、重启后保留任务的 SQLite 实现，
`bricks.queues.redis.RedisTaskQueue` 提供可跨机器共享的 Redis 实现。
`QueueBackend` 把这些存储接入 Interlace 的公开 `TaskPublisher / TaskConsumer` SPI，
完整 Graph 的执行、线程和本地 Slot 分配仍使用 Interlace。

## 安装与快速运行

```bash
uv run --locked python -m examples.crawler_queue
uv sync --locked --extra redis
```

Redis 实现使用同步 redis-py `redis.Redis` 客户端，不支持 RedisCluster；支持 Redis 7.2 及以上单实例服务。
队列键带有同一 hash tag，使一个 Lua 脚本涉及的多键位于相同 Redis Cluster hash slot；当前实现仍不支持 Cluster，
不接受前缀中额外的 `{`、`}`。默认导入 `bricks` 或 `bricks.queues` 不加载 Redis 库。客户端由调用方创建和关闭，
应显式配置连接和操作超时，保证后台续租及关闭不会无限等待。

先启动专用 Redis，再投放任务。在不同终端启动消费命令即可竞争消费同一队列：

```bash
uv run --locked python -m examples.crawler_queue --redis-url redis://127.0.0.1:6379/0 --role produce --queue demo-run-1 --count 100
uv run --locked python -m examples.crawler_queue --redis-url redis://127.0.0.1:6379/0 --role consume --queue demo-run-1 --concurrency 2
```

示例消费者在已有任务排空后退出；它不是持续监听的守护服务。持续消费服务应保持 Runtime 存活，
由外层服务负责停止信号和生命周期。示例仅打印种子，不能把日志输出当作可靠业务存储。

单机部署不需要 Redis 也能在进程退出或重启后继续：

```python
from bricks.queues.sqlite import SqliteTaskQueue

queue = SqliteTaskQueue("tasks.sqlite3", "news-run-1")
spider = MySpider(task_queue=queue, queue_name="news-run-1", concurrency=4)
```

SQLite 实现使用标准库 sqlite3，无需额外依赖。每次操作打开独立连接，在 `BEGIN IMMEDIATE` 写事务内完成校验与状态迁移，
同一数据库文件可由同机多个进程和线程共享，文件内按队列名隔离；`timeout` 设置等待其他进程写锁的秒数，默认 30。
租约时间使用本机墙上时钟，因此不支持跨机器共享或网络文件系统；系统时钟大幅回拨会使已过期租约暂时视为有效。
不接受 `:memory:` 内存数据库，进程内临时队列使用 MemoryTaskQueue。

## 身份、序列化和状态

```python
from bricks.queues import MemoryTaskQueue, make_work

queue = MemoryTaskQueue("news-run-1", max_attempts=3)
queue.put(make_work("news@v1", {"url": "https://example.com/news/1"}))
lease = queue.claim("worker-1", lease_seconds=30)
if lease is not None:
    # 真实业务应先完成幂等存储，再确认任务及显式业务结果。
    queue.ack(lease, result="success")
```

`Work.id` 是队列内的去重键。推荐使用 `make_work(graph, inputs)`，它返回普通 Interlace Work，
默认以规范化 JSON 内容计算 `sha256:` 指纹，不需要手动生成 ID。
也可用 `make_work(graph, inputs, id="news:1")` 显式指定；空字符串不视为缺省。
相同 ID 和内容重复提交返回 False，
相同 ID 对应不同内容抛出 `TaskConflictError`；换一轮爬取应使用新的队列名称。
指纹覆盖信封版本、graph、inputs、options、limits 和 trigger，字典键排序，列表顺序保留；
ID、租约与交付次数不参与计算。相同内容在不同进程生成相同 ID，图版本或执行配置变化产生不同 ID。
来源事件中变化的时间戳也会影响指纹；需要按业务身份去重时显式指定 ID。
不会自动规范化 URL，整数 1 与浮点数 1.0 也按各自 JSON 表达区分。
构造时复制输入；之后修改返回 Work 的嵌套数据不会重算 ID，需要修改内容时应重新调用 make_work。
直接使用 `interlace.spi.Work(...)` 仍沿用引擎的随机 UUID 默认值，队列保留传入的 Work.id。

`Work.graph` 建议使用不可变的逻辑版本名，如 `news@v1`，
所有消费进程必须注册相同的图定义；队列不发布或验证源码版本。

任务信封显式保存 `id / graph / inputs / trigger / limits / options`，内部格式版本为 1。
只接受 JSON 标量、列表和字符串键字典，拒绝字节、元组、非有限数、循环、动态类加载和 pickle。
Request、Response、Context、Slot 和 Client 必须由应用根据数据在消费端重新构建。
Redis Lua 不解析业务 JSON，避免大整数精度损失或空列表变为空对象。
内存与 Redis 实现都复制输入、读取结果和进度；修改领取到的 Work 输入不会修改存储副本。

| 状态 | 含义 |
| --- | --- |
| pending | 等待领取，包含尚未到达可领取时间的延迟任务 |
| leased | 已领取，租约仍需定期续期；过期记录等待下一次 claim 回收 |
| acked | 此次交付已确认；不自动推断业务成功 |
| dead | 明确拒绝或交付次数耗尽，保留最后错误与任务内容 |

`ack(lease, result="success" / "failure")` 可以原子登记显式业务结果；省略时保持 None。
`get(id)` 返回状态和独立 Work 副本，`counts()` 返回四种状态的计数。
终态不会自动删除，以便保留去重身份；`forget(id)` 只允许移除 acked/dead 任务。

`revive(id)` 把单个 dead 任务恢复为可立即领取的 pending，交付次数清零并清除错误与业务结果；
任务不存在或不是 dead 时返回 False。`revive_dead(limit=1000)` 每次原子恢复至多 limit 个死信并返回数量，
循环调用直到返回 0 即可重跑全部死信。恢复不撤销已发生的外部副作用，排查失败原因后再恢复，
避免同一问题再次耗尽交付次数。Redis 以独立有序集合索引死信，SQLite 按入队顺序查询。

```python
while queue.revive_dead():
    pass
```

## 交付租约与恢复

`claim(owner, lease_seconds=30)` 非阻塞领取一项任务，没有就绪项时返回 None。
`put_claim(work, owner, lease_seconds=30)` 按 Work.id 原子完成同内容去重、持久化和指定任务领取；
任务已 leased、处于 retry 延迟或已经终态时返回 None，不绕过现有状态。它供本地 SlotLease 延续适配器使用，
避免先 put 再 claim 时被其他 Worker 抢走；普通生产者仍只调用 put。
多个消费者按可领取时间竞争任务，不承诺严格 FIFO、公平分配或全局执行顺序。
`renew` 返回新截止时间的凭据，token 保持不变；`retry`、`reject` 和 `ack` 结束当前凭据。
重新领取产生新 token，过期凭据即使尚未被接管也不能继续写入，失败抛出 `LeaseLostError`。
未 ACK 本身不会导致回收：只要 QueueBackend 的后台线程仍正常续租，任务就保持 leased。
即使业务函数长期不返回，正常心跳也会持续保留租约；本实现没有按认领总时长强制转移任务的机制。

`retry(lease, delay=5, error="temporary failure")` 设置再次可领取的时间。
attempt 从首次领取的 1 开始递增；最大次数随第一次入队保存，消费者配置变化不重置它。
显式重投和租约过期恢复都会受最大次数约束；`reject` 直接进入 dead。
每次 claim 最多回收 100 个过期交付，不一次扫描整个 Redis 队列。
没有活跃领取器时不会自动执行过期回收，因此 counts 中 leased 不代表凭据仍有效。

内存实现以锁和单调时钟保证当前进程内的原子操作，同名不同实例不共享数据，进程退出后任务丢失。
SQLite 以文件写事务保证同机进程间的原子性，任务、终态与初始化断点随文件保留。
Redis 使用[服务端 TIME](https://redis.io/docs/latest/commands/time/)和
[Lua 原子操作](https://redis.io/docs/latest/develop/programmability/eval-intro/)保护状态与索引；
使用独立命名空间管理这些键，不应由其他代码直接改写其类型或内容。

Redis 交付按至少一次设计，持久性仍取决于 Redis AOF、复制与故障切换配置。
Worker 可能在写入外部数据库后、ACK 前崩溃，因此业务存储需要以种子 ID 等键实现幂等。
租约 token 只保护队列状态，不会强制停止已在运行的 Python 函数，也不会阻止失去租约的程序继续访问外部服务。
网络超时可能意味着操作已经在服务端生效；可用稳定 ID 重试入队、读取状态判断结果，不能推断超时即未执行。

## 使用 with 管理占用

三个具体队列实现都提供同步上下文管理入口：

```python
with queue.reserve("worker-1", lease_seconds=60) as task:
    if task is not None:
        print(task.work.inputs)
        # 业务处理完成后登记确认；真正 ACK 在正常退出时写入。
        task.ack(result="success")
```

`reserve` 进入时才领取，队列暂时没有任务时 as 值为 None。已经调用 claim 取得凭据时，可以使用 hold：

```python
lease = queue.claim("worker-1", lease_seconds=60)
if lease is not None:
    with queue.hold(lease, lease_seconds=60) as task:
        print(task.work.inputs)
        task.ack()
```

进入 hold 时会立即续租校验已有凭据，过期凭据不能重新生效。一个上下文只能进入一次，
同一队列实例不允许两个上下文管理同一租约。进入后使用 task 的方法，不能再直接 queue.ack/renew/retry 同一凭据。
TaskQueue 存储协议不强制这些便捷方法，第三方存储仍可按原协议对接 QueueBackend。

| 块内操作及退出方式 | 退出结果 |
| --- | --- |
| task.ack() 后正常退出 | 确认交付，默认不登记业务结果 |
| task.ack(result="success") 后正常退出 | 确认交付并显式登记业务成功 |
| task.retry(delay=5) 后正常退出 | 停止占用，按指定延迟请求重投 |
| task.reject(error="invalid seed") 后正常退出 | 停止占用并保留死信 |
| 未登记决定就退出 | 请求重投，未耗尽次数时回到 pending |
| 块内发生异常，包括登记 ack 后又发生异常 | 取消预定决定，释放未确认交付并继续抛出原异常 |

ack/retry/reject 只登记退出决定，不会在块内提前解锁；重复登记会报错。
重投仍受最大交付次数约束，达到上限会进入 dead。退出时写回失败会传播错误，任务只能等到租约到期后恢复。
`task.check()` 可以在业务步骤之间检查已经观察到的后台续租失败；它不发起新的服务端校验，也不能强制中断同步函数。
续租遇到暂时存储错误时会在后续心跳重试，直到本地租约截止仍未成功才登记失败；`LeaseLostError` 立即登记。
`task.work` 在整个占用期间保持同一个对象，自动续租不会覆盖本地修改，但这些修改不会更新已保存的任务内容或 ID。

同一个队列实例的所有 with 占用共享一个续租线程，按到期时间调度，不为每个任务创建线程。
第一个有效占用进入时才启动线程，最后一个占用退出时等待线程停止；空队列不启动线程。
不同队列实例拥有各自的调度器，不使用进程全局注册表。QueueBackend 自身仍使用每个消费绑定的共享轮询线程，
经它消费时无需再套用 reserve/hold。

共享线程只减少线程数量：当前每个任务仍单独调用 renew。以 1,000 个活跃任务、60 秒租约为例，
约每 20 秒续租一轮，平均约 50 次 Redis 续租调用/秒。当前没有批量续租或 async with 接口；
Redis 操作超时与单次轮询的总耗时需要留在租约余量之内。

## 初始化租约与断点

初始化与消费使用独立状态。`acquire_init(owner)` 取得生产权，`renew_init` 续租，
`save_init(lease, progress)` 保存 JSON 断点，`finish_init(lease, success=...)` 登记生产结束。
`put(work, init=lease)` 在入队的同一原子操作中校验生产者租约，阻止旧生产者接管后继续投放。
不传 init 表示普通动态种子提交，不参与初始化权限检查。

每次接管使用新 token 和递增 fence，保留前一生产者的断点。只有 failed 或已过期 running 可以重新申请；
succeeded 不自动重新开始。fence 仅用于本队列的初始化写入，不宣称能保护任意外部存储。
进度写入与前一次入队是两个独立操作：恢复时允许重放最后一批，用稳定 Work.id 消除重复入队。
可运行的续租、断点保存和恢复代码见[队列示例](../examples/crawler_queue.py)。

## 接入 Runtime

```python
from interlace import Runtime
from interlace.runtime import LocalInfrastructurePlugin
from bricks.queues import QueueBackend

backend = QueueBackend({queue.name: queue}, lease_seconds=30)
# 以基础设施插件注入任务后端，默认 Router 与 Worker 经同一装配路径使用它；注册 Graph 后再调用 runtime.consume。
# 后端由调用方在 Runtime 关闭后关闭。
runtime = Runtime(plugins=[LocalInfrastructurePlugin(tasks=backend)])
```

`QueueBackend.bind` 返回独立、幂等的注销句柄；多个绑定或进程可以竞争同一存储。
每个绑定最多预取 concurrency 项，由 Interlace 公开内存后端管理执行线程和 Slot，后台线程周期续租；交付结束时会唤醒领取循环。
单次续租的暂时存储错误会在本地租约截止前按心跳重试；截止时间按本机单调时钟计算，不与 Redis 服务端时间比较。
续租持续失败到本地截止后，该交付视为租约丢失，绑定停止领取并向等待方传播错误；`LeaseLostError` 只终止对应交付。
领取遇到暂时存储错误时会退避重试，连续失败约一个租约时长后才停止绑定并向等待方传播错误。
租约失效后若本绑定重新领到仍在本地执行的同一任务，新租约保持续租，待旧交付结束后再执行，不并发运行也不空耗交付次数。
注销或关闭只停止领取；排空期间在途交付通过 `affinity="current"` 提交的本地延续仍会持久化并执行，属于排空的一部分。
后台续租不会续期初始化生产者，初始化循环必须自行调用 renew_init。

handler 返回的 `DeliveryResult.ACK` 只确认交付，`RETRY` 要求有界重投并直接返回存储结果状态，`REJECT` 保留 dead 状态。
Interlace 当前 GraphWorker 对普通执行失败返回 REJECT，因此并不会自动重试解析、存储或编程错误。
请求级重试应由业务 Graph 明确实现；使用底层 WorkHandler 时才由该处理器显式返回 RETRY。
处理器的取消和 Interlace 执行控制异常先按任务次数限制重投，再向等待方传播；其他处理器异常按 REJECT 规则进入死信。
非法返回值及存储写回失败会向等待方传播，不因本地执行结束而谎报存储已经确认。

每次经过任务存储的交付都建立新的本地 Slot 链，包括使用 MemoryTaskQueue 时。
QueueBackend 实现 Interlace 的 `LocalTaskPublisher.submit_local`：本地 Router 已为分支 retain SlotLease，
backend 通过 `put_claim` 原子完成同内容去重、持久化和指定任务领取，并将领取返回的 Work 快照交给本地调度器，
不再使用可能被生产者随后修改的原始 Work 对象。因此不存在“先入队再被其他 Worker 抢走”的窗口；
进程在执行前后异常退出时，存储租约到期后仍可重新领取。
领取后的 before_execute Hook 与同 Slot 的 Graph execution 共用 Interlace 的 `SlotLease.execution()` 串行边界；
Graph execution 本身由 Interlace 串行执行。ACK/RETRY/REJECT 与普通持久任务使用相同状态机，
RETRY 或异常恢复后不再保证原 Slot，错误由 wait_idle 原样传播。
该能力只保证同一进程和 Slot，不保证同一操作系统线程，也不能跨进程序列化 Session。

跨图 Event 创建独立 execution，按核心契约不继承源 execution options；领域需要的配置应放入事件 payload
或目标 Graph 的领域配置。Air 的 `Context.submit(..., affinity="current")` 在事件 payload 中携带完整稳定 Work，
因此子任务保留显式 options，同时被持久化并优先延续当前 Slot。

`backend.wait_idle(timeout)` 检查配置队列的所有 pending/leased 任务和当前进程的 Slot 清理，
可能等待其他 Worker；它是当前队列状态的快照，不是分布式终止检测。
调用方仍需确认初始化结束、外部生产者停止，并收集各 Worker 的错误。
`runtime.wait_idle` 还等待本实例的跨图事件流转。
backend 关闭后，idle/wait_idle 只检查本地已接受交付的清理，不再等待留在共享存储中的任务，
因此仅负责初始化投放的进程可以关闭 Runtime 并让其他消费者继续处理。

显式装配时先关闭 QueueBackend 以停止领取并清理本地交付，再关闭 Runtime、Slot 中的会话资源和 Redis 客户端。
若已经等待队列完全排空，也可以正常退出 Runtime 上下文后再关闭 QueueBackend；只生产任务的实例不能等待共享队列排空。
注销和 close 停止新领取，等待已领取任务和 Slot 引用清理，未领取任务留在存储中。
关闭不强杀同步函数，处理器永久阻塞会阻止排空。调用方注入的队列和 Redis 客户端不会被 backend 关闭。

## 验证

```bash
BRICKS_TEST_REDIS_URL=redis://127.0.0.1:6379/0 uv run --locked pytest tests/crawler/test_queues.py -q
```

使用专用测试 Redis；测试仅删除各自随机前缀的键，不执行 FLUSHDB。
契约测试对内存、SQLite 与 Redis 三种实现参数化执行，覆盖并发去重、严格 JSON、租约接管与过期写入拒绝、
延迟与次数耗尽、死信恢复、初始化断点、长任务续租、关闭排空、跨图事件、两个独立 Runtime 竞争，
以及 Worker 进程退出后的恢复；SQLite 另验证子进程领取后退出、重新打开文件接管。
with 契约还验证共享线程数量、长任务占用、延后确认、异常释放、过期凭据拒绝和单项续租失败隔离。
未配置地址时跳过 Redis 用例，内存与 SQLite 契约仍执行。

[返回目录](README.md)
