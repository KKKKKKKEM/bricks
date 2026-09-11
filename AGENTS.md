# Bricks Repository Instructions

## 执行要求

本文件是仓库级 AI 编码代理指令。修改代码、公共 API、运行语义、架构或文档前，必须完整阅读并遵守本文件。
实现、测试和文档以当前架构为准，不保留旧别名、隐式转换、弃用路径或兼容分支。
设计调整必须同步修改本文件、实现、测试和相关手册。规范保持简洁，教程写入 `docs/` 编号章节。

## 架构宪法

状态：Normative
适用范围：Bricks 爬虫领域框架

### 第一条：领域与微内核分离

1. Bricks 负责爬虫领域模型、下载器、独立解析器和领域节点；通用编排由独立的 Interlace 包提供。
2. 依赖方向固定为用户应用 -> Bricks -> Interlace；不得复制或内置 Interlace 核心实现。
3. Bricks 只依赖 Interlace 公开 API 与 SPI，不读取运行时私有状态，不改写核心契约。
4. Graph、Event、Execution、Slot、Runtime 和插件的规范由
   [Interlace 架构宪法](https://github.com/KKKKKKKEM/interlace/blob/main/AGENTS.md)维护。
   涉及核心契约的修改须同时阅读对应依赖版本的宪法并在 Interlace 实施。

### 第二条：公共 API 保持简洁

1. 顶层 `bricks` 只导出 Request、Response、Cookies、Items、UploadFile、Downloader、AsyncDownloader、
   DownloadNode 和 AsyncDownloadNode。
2. 模型位于 `bricks.models`，下载协议与实现位于 `bricks.downloaders`，领域节点位于 `bricks.nodes`。
   独立内容解析和批量规则位于 `bricks.parsers`，不在顶层转导出。
   爬虫任务存储、初始化租约与公开任务 SPI 的装配适配器位于 `bricks.queues`，不在顶层转导出。
3. Graph、Node、Event、Context、Execution、Slot 和 Runtime 从 `interlace` 导入，不在 Bricks 转导出。
4. 新能力优先组合现有模型、普通函数、领域节点与窄协议，避免不必要的包装和全局注册表。
5. curl_cffi 是默认传输和正式依赖；HTTPX、requests、wreq、primp、requests-go、playwright、camoufox
   通过同名 extra 安装，默认导入路径不加载可选库。
   DownloadNode() / AsyncDownloadNode() 默认装配对应 curl_cffi 临时下载器；跨请求复用仍由调用方按 Slot 装配。
   wreq 仅支持 Python 3.11 以上，extra 使用环境标记；Bricks 的其他实现仍支持 Python 3.10。

### 第三条：下载职责与资源所有权

1. 下载器通过 `fetch(Request) -> Response` 或异步同义接口结构化替换，不要求继承具体实现。
2. 下载节点复制输入 Request 后选择和调用下载器，通过 Output 沿当前 Graph 传播 Response。
   DownloadNode 与 AsyncDownloadNode 均继承 Interlace Node，分别使用 def 与 async def execute；
   同步和异步下载协议仍独立，由执行器等待异步结果，不接受异步生成器作为节点返回值。
3. Request 不保存下载器实例或注册名称；选择函数由节点构造器注入，不改变核心 token 选择语义。
4. Node 和下载器默认可重入，不在共享实例中隐式保存当前执行状态；注入实例默认由调用方管理。
5. 跨逻辑执行链的资源使用 Interlace Slot，execution-local 状态遵循 Context 的命名空间边界。
6. Event 只能显式 emit，Node 返回值不得隐式变为跨图事件。目标 Graph 不在 emit 调用栈内执行。
7. 下载节点接受固定下载器映射或同步 `Slot -> 下载器映射` 解析函数；后者必须有当前 Slot，禁止无槽回退。
   Slot 隔离逻辑执行链而非线程，解析函数只读取已装配资源；同槽串行语义由 Interlace 保证。
8. 可选 SessionDownloader / AsyncSessionDownloader 协议提供 prepare_session 和 close_session；
   准备结果仍满足对应下载协议，由装配方按 Slot 保存并成对关闭，基础下载协议不强制支持会话。
   会话下载器复用独立 Client/Session，Cookie 与连接池归该会话；每个 Slot 独立装配。
   SlotPool 不负责关闭值中的资源，调用方必须在执行停止后关闭 Client；异步 Client 在使用它的事件循环内关闭。
   会话代理、TLS 与重定向上限由 Client 构造配置，Request.proxy 在会话模式下明确拒绝。
9. 会话策略从 Context.options 的 crawler.session.reuse 读取，仅接受布尔值，默认 True。
   False 使用节点显式装配的 isolated_downloaders，同名选择规则保持一致；未装配则报配置错误。
   临时下载器每次 fetch 自行创建并关闭资源，不读取 Slot 会话；该分支无需 Slot。
   配置属于当前 Graph execution，不保存到 Request 或 Slot，不随跨图事件继承。
10. curl_cffi 同步会话使用非线程局部句柄，以支持 Slot 串行跨线程复用；异步会话只在所属事件循环内使用和关闭。
    curl_cffi 响应只承诺初始提交请求及重定向 URL、状态和响应头，不伪造逐跳请求或响应体。
    requests 仅提供同步实现，重复请求头明确拒绝；不通过线程包装声称支持原生异步取消。
11. Context.options 的 crawler.fingerprint.impersonate 覆盖当前执行的预设指纹，接受非空无首尾空白字符串或 None。
    缺省或 None 沿用下载器/会话默认值，与 curl_cffi requests-like 接口一致；指纹不改变会话复用策略。
    节点通过可选 FingerprintDownloader / AsyncFingerprintDownloader 的 fetch_with_fingerprint 传入本次指纹。
    curl_cffi 普通与会话下载器均支持该能力，直接调用 Session.request(..., impersonate=...)，不改写会话默认配置。
    Cookie 和会话资源按原所有权保留；底层连接是否复用由 curl_cffi/libcurl 决定，不承诺每次重新握手。
    不支持该能力或同步异步类别错误时明确失败；指纹名称由传输库校验，不静默忽略。
12. wreq 使用原生 Profile 名称并支持会话内逐请求 emulation；保留重复头和有限重定向记录。
    primp 2.0 指纹在 Client 构造时固定，会话内要求更换指纹时明确拒绝，不重建或迁移会话。
    primp 普通临时下载支持 Context 指纹覆盖，拒绝重复请求头；响应头合并、Cookie 属性与历史缺失须如实说明。
    primp 没有原生 Client.close，close_session 释放包装器持有的引用并禁止继续使用，不宣称空操作上下文退出会关闭资源。
    primp 环境代理遵循原生库，不提供伪造的 trust_env 开关；所有新实现均保持默认请求和关闭后的生命周期校验。
13. requests-go 只提供同步下载与 Slot 会话，HTTPS 使用独立 Go 会话 ID，HTTP 沿用原生 requests 路径。
    每次 TLSConfig 深复制并绑定当前 Session 的公开 ID，不直接修改或使用模块级预设的 ID；关闭必须执行完整 Session 上下文退出。
    原生 AsyncSession 是线程包装，不宣称支持可中断的原生异步下载。
    requests-go 1.0.9 的 HTTPS 后端忽略证书校验要求，因此 verify=True 时明确拒绝 HTTPS，只有显式 verify=False 才启用。
    HTTPS 仅接受正整数秒超时并拒绝显式 Content-Length；不透明 TLSClientExeption 不作为可恢复传输失败转换。
14. Playwright 与 Camoufox 共用双模式会话实现：page 使用真实浏览器导航，api 使用关联 APIRequestContext。
    每个会话独立 BrowserContext 并懒创建、复用一个 Tab；两种模式共享 Cookie，api 不执行页面脚本也不使用浏览器 TLS 栈。
    Context 代理在 prepare_session 时固定；一 Context 一 Tab 可在同一外部 Browser 内实现不同 Tab 不同代理，不热切已有 Context 的代理。
    自建 Browser/驱动归会话关闭栈管理，外部注入 Browser 由调用方管理；关闭会话不影响其他 Context。
15. 浏览器同步会话受创建线程约束，异步会话受创建事件循环约束，错误作用域或重叠请求必须明确失败。
    浏览器操作失败、导航超时或取消后关闭整个会话，阻止残留脚本或 API 请求继续修改状态；重新执行需重新装配。
    page 默认返回 UTF-8 DOM 快照并移除失效实体头，可选 response 模式返回主文档原始内容；api 始终返回原始响应体。
    page 只支持无请求体 GET 和浏览器重定向；api 支持普通 HTTP 方法和已编码请求体，CONNECT 明确拒绝。
    Camoufox 包和浏览器版本分别固定，不将其浏览器配置伪装成请求级 TLS impersonate；默认不下载或启用附加扩展。

### 第四条：独立解析与批量规则

1. Parser 是 prepare(source) 与 extract(source, expression, **options) 的窄协议；
   prepare 接受自身准备结果，实例不保存当前文档。自定义实现无需继承 BaseParser 即可使用公共 match。
2. 解析器不依赖 Response、Items、Context 或 Runtime；输入、文档及其可变查询结果由调用方管理。
   BaseParser 仅提供可选的 extract_first 和 match 便捷方法，不建立注册表或动态字符串加载路径。
3. CSS、XPath 使用 lxml/cssselect；HTML/XML 格式显式配置，XML 禁用 DTD 加载和实体展开。
   XML 在返回文档或查询前拒绝 DOCTYPE，现成元素按所属文档检查；标准实体与数字字符引用仍支持。
   CSS 输出节点、文本或属性必须明确选择，不自定义 ::text/::attr 语法；文本不自动去空白。
4. JMESPath 和 JSONPath 接收已解码 JSON 值，字符串始终是值；JSON 文本由调用方显式解码。
   JMESPath 保留原生 null/缺失语义；JSONPath 使用 python-jsonpath 严格模式，结果始终为命中值列表。
   JSONPath 校验根值为 dict、list、str、int、float、bool 或 None，拒绝文件读取接口及字节输入，不消费输入流。
   正则仅接收文本，默认返回完整匹配，捕获组由 group 显式选择，不按组数改变输出形状。
5. match 接受字段映射、Rows、Product 或递归规则序列，返回 list[dict]；规则序列始终按顺序拼接。
   Rows 只展开明确选中的列表，其 fields 可递归使用各类记录规则；字段位置的 Rows/Product 保留为列表。
   Product 各分支在同一当前源上提取并做笛卡尔积；Rows 内组合父字段映射和子 Rows 实现关联展开与父字段继承。
   普通字段数组、嵌套映射仍保留原结构，不隐式广播、展开或合并。
6. Rule 明确描述数量、条件、前后转换、缺失判定和默认值；Group 按序选择非缺失候选。
   MISSING 与 None、空列表和假值不同；first 只接受列表，空列表转为 MISSING。
   规则不绑定调用时引擎；结构配置固定，默认值和常量逐次复制，回调及原生解析异常原样传播。
   值规则为 Rule、Constant、Group、Pipeline 和 Collect，可递归组合并用于字段和 Rows.select；
   Group 候选与 Collect 成员只接受显式值规则，不接受裸表达式、函数或记录规则。
7. Product 默认任一空分支产生零条记录；keep_empty=True 将空分支视为一条空记录，不补 None。
   零分支乘积为 [{}]；on_conflict 默认 raise，first/last 按分支顺序保留同名字段，不递归合并字段值。
   各分支每次组装只执行一次，已执行的表达式及回调错误不得被空分支吞掉；合并后的每条记录深复制。
8. lxml、cssselect、jmespath 和 python-jsonpath[strict] 是正式依赖，维护 uv.lock；
   单表达式、批量与嵌套规则、笛卡尔积、父子关联、空分支与字段冲突、第三方协议替换、
   缺失语义、规则复用、连续提取、多规则收集及异常传播必须有测试。
9. Pipeline 接受非空的值规则或单参数同步转换函数序列，前一步结果直接传入下一步，
   不隐式解码、复制、逐项映射或改变默认解析器。任一步返回 MISSING 即停止；
   单步默认值仍可继续，整链默认值与必填约束用外层 Group 表达，错误和取消原样传播。
10. Collect 各成员在同一当前源上按声明顺序执行一次，仅跳过整个成员返回的 MISSING。
    values 默认保留各结果结构；concat 要求有效结果为 list 并只拼接一层，非列表明确报错。
    不去重、不删除内部缺失位置、不隐式过滤 None 或假值；零成员或全部缺失返回 []。
    新建外层结果列表，内部值沿用来源规则的所有权；成员失败立即传播，不返回部分收集结果。

### 第五条：错误与可靠性必须如实表达

1. 网络传输失败可转换为 `Response(status_code=-1, error=...)`；HTTP 4xx/5xx 保留正常响应。
2. 程序错误、配置错误、取消和引擎控制异常继续传播，不得作为普通业务失败恢复。
3. 取消和同步 Node timeout 是协作式语义，不宣称能够安全强杀任意 Python 函数。
4. URL 去重、重试、幂等、代理、会话和持久化属于领域策略，不增加 Runtime 或 Context 职责。
5. 已接受 Event 和已交付 Output 不因后续失败撤回；不得夸大默认内存实现的消息可靠性。
6. 文档只描述已实现能力，明确 HTTP 传输、Cookie、上传和缓冲行为的限制。

### 第六条：依赖与验证

1. Interlace 使用显式、可复现的版本来源；Git 依赖固定完整提交 ID 并维护 `uv.lock`。
2. 正式依赖不得指向本地目录，不得通过隐式路径使测试通过。
3. 核心变更先在 Interlace 验证和推送，再升级 Bricks 依赖并执行领域与跨包契约测试。
4. 领域模型校验、下载器替换、同步异步下载、取消、错误传播和请求复制必须有测试。

### 第七条：Pythonic 实现与中文注释

1. 使用 Python 惯用方式和窄协议，保持接口简洁，避免重复抽象和隐式行为。
2. 模块、类、函数、方法及行内说明使用中文；标识符、标准格式标题和工具指令保留原有语法。
3. 每个函数和方法必须提供 Google 风格文档字符串，以中文概述职责，按实际契约填写 Args、Returns、
   Yields、Raises；无对应内容时省略该节，不机械填充空段落。
4. 成员字段（含类属性和私有字段）必须使用中文旁注或 Attributes 节说明含义、单位、默认值、所有权和可变边界。
5. 实现、测试和示例均遵守本条。

### 第八条：种子任务队列与分布式交付

1. TaskQueue 是具名任务存储的窄协议；MemoryTaskQueue 与可选 RedisTaskQueue 提供相同的
   put、claim、renew、ack、retry、reject、get、counts、forget 和初始化租约操作。
   QueueBackend 通过 Interlace 的公开 TaskBackend SPI 装配消费，并组合其公开内存后端管理线程及 Slot，
   不复制执行器或访问私有状态。Redis 通过 redis extra 安装，默认导入路径不加载 Redis 库。
2. Work.id 是队列内的去重身份；入队内容和待领取索引必须原子写入。同 ID 同内容返回已存在，
   不同内容明确报冲突。终态保留内容与去重身份，只有显式 forget 可以移除；待处理和在途任务禁止删除。
   make_work 默认根据规范化 JSON 信封（除 ID 外）生成 sha256 内容 ID，支持显式指定 ID；
   graph、inputs、options、limits、trigger 和信封版本均参与指纹，字典键排序，列表顺序保留。
   该普通函数返回独立 Work，不改变 Interlace Work 缺省使用随机 UUID 的契约，也不改写直接投递的 Work.id。
   URL 规范化、流程版本以及外部写入幂等由调用方确定；构造后修改 Work 的嵌套内容不会重算已有 ID。
3. 任务信封只传输公开 Work 字段及严格 JSON 值，不接受 pickle、动态类加载、字节或隐式类型转换。
   options、trigger 和执行限制显式保存；Context、Slot、Request/Response 及 Client 不直接跨队列序列化。
   两种存储都在输入和读取边界建立数据副本；Redis Lua 不解码用户业务 JSON，保留大整数与空数组形状。
4. claim 创建有期限的随机 token，attempt 从 1 开始。续租、确认、重投和拒绝均原子校验 owner、token、
   状态和到期时间；到期即失去写入权。claim 每次最多回收 100 个过期任务，重新领取使用新 token。
   retry 支持非负延迟，达到随任务保存的最大交付次数后进入 dead；显式 reject 直接进入 dead。
5. ACK 是交付完成，业务结果 success/failure 只能显式登记，不能由 ACK 推断。
   QueueBackend 不解释 Graph 的业务输出，正常 DeliveryResult.ACK 只确认交付；RETRY 有界重投，
   REJECT 保留死信；编程错误、取消和存储异常向等待方传播。存储写回失败由租约到期恢复，不谎报确认。
   Redis 提供至少一次交付，持久性取决于服务端 AOF、复制与故障切换配置；外部副作用必须幂等。
6. 初始化租约使用随机 token 与递增 fence；续租、进度写入、带租约入队与结束都校验当前有效凭据。
   接管保留 JSON 进度；成功初始化不重新申请，失败允许显式重新申请，新一轮业务使用新队列名称。
   进度提交与前一次入队不是跨调用事务，恢复时以稳定任务 ID 重放；初始化完成不代表消费完成。
7. Redis 使用服务端时间和 Lua 原子迁移，同队列键使用同一 hash tag。MemoryTaskQueue 使用进程内锁与单调时间，
   不提供进程重启恢复。同名内存实例不共享数据；Redis 客户端与存储均由装配方管理。
8. QueueBackend 每个绑定最多预取 concurrency 项并周期续租，Slot 分配及释放由 Interlace 管理。
   每次通过任务存储的交付建立本地 Slot 链，不提供 submit_local；跨队列会话延续或 Worker 身份路由不在本实现范围。
   注销和 close 停止新领取并等待已领取交付及 Slot 清理，未领取任务保留；close 不关闭借用客户端。
   wait_idle 检查已配置队列的全部 pending/leased 任务及本地 Slot 清理，不证明外部生产者已停止或全局爬取完成。
9. MemoryTaskQueue 与 RedisTaskQueue 的 reserve(owner) 提供进入时领取的同步 with 上下文，
   hold(lease) 接管已领取凭据；TaskQueue 存储协议本身保持独立。无任务时 as 值为 None。
   每个队列实例的全部上下文共享一个惰性续租线程，按最早到期时间调度；最后一个占用退出时等待线程停止。
   同一实例不允许两个上下文同时管理同一 token，不建立全局线程或租约注册表；每个上下文只能进入一次。
10. 上下文内的 ack/retry/reject 只登记一次退出决定，保持占用直到正常退出。
    未登记决定或块内异常退出时请求有界重投，原异常继续传播，达到最大次数仍进入 dead；不根据退出推断业务成功。
    正常退出先停止该占用的续租，再对最新有效凭据写入决定。续租失败由 check/退出报告，不强制中断业务函数；
    释放失败可等待租约到期恢复，不伪称已释放。占用期间不得绕过上下文直接操作同一凭据。
    续租更新凭据不替换上下文中的 Work 对象，业务修改仅保留在当前进程的本次占用中。

## 验证要求

```bash
uv run --locked pytest -q
uv run --locked mypy bricks
uv run --locked ruff check bricks tests examples
uv run --locked ruff format --check bricks
uv build
```

文档变更还应检查本地链接、Markdown 围栏和发生变化的 Mermaid 图。
队列分布式验证设置 `BRICKS_TEST_REDIS_URL` 指向专用测试 Redis 后运行队列测试；
测试仅清理随机生成的独立前缀，禁止清空共享数据库。必须覆盖真实 Redis、竞争领取、旧租约写入拒绝与进程退出恢复。
