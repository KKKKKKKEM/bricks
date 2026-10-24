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
   代码式 air 与配置式 form 爬虫基类及领域 Context 位于 `bricks.spider`，共享声明位于 `bricks.hooks`，可复用领域插件位于
   `bricks.plugins`；该插件包不包含 Interlace 的插件宿主或注册实现。
   爬虫流程信号位于 `bricks.signals`，由领域节点处理，不并入 Interlace 的执行控制异常。
   独立代理值、供应源、Clash 控制与按 Slot 装配的管理工具位于 `bricks.proxies`，不在顶层转导出。
3. Graph、Node、Output、Event、Context 和 Runtime 从 `interlace` 导入，Execution、ExecutionPlan、Slot 与 SlotPool 从
   `interlace.engine` 导入，ExecutionLimits 与任务 SPI 从 `interlace.spi` 导入；均不在 Bricks 转导出。
   自定义任务后端经 `interlace.runtime.LocalInfrastructurePlugin(tasks=...)` 装配，不直接构造 Router 或 Worker。
4. 新能力优先组合现有模型、普通函数、领域节点与窄协议，避免不必要的包装和全局注册表。
5. curl_cffi 是默认传输和正式依赖；HTTPX、requests、wreq、primp、requests-go、playwright、camoufox
   通过同名 extra 安装，Spider.serve 依赖的 interlace-py[service] 通过 service extra 安装，默认导入路径不加载可选库。
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
   SlotPool 不负责关闭值中的资源；Air 自动装配的同步会话由 Spider 在执行停止后关闭，其他调用方装配的 Client
   仍由调用方关闭；异步 Client 在使用它的事件循环内关闭。
   会话代理、TLS 与重定向上限由 Client 构造配置，Request.proxy 在会话模式下明确拒绝。
   `bricks.downloaders.proxy_sessions.ProxySessions` 组合同步会话工厂，在一个 Slot 内按 Request.proxy 懒建并复用
   固定代理的独立会话，转发前清除请求代理；超过 max_sessions 时关闭最久未使用的会话，Cookie 按"Slot × 代理"隔离。
9. 会话策略从 Context.options 的 crawler.session.reuse 读取，仅接受布尔值，默认 True。
   False 使用节点显式装配的 isolated_downloaders，同名选择规则保持一致；未装配则报配置错误。
   临时下载器每次 fetch 自行创建并关闭资源，不读取 Slot 会话；该分支无需 Slot。
   配置属于当前 Graph execution，不保存到 Request 或 Slot，不随跨图事件继承。
10. curl_cffi 同步会话使用非线程局部句柄，以支持 Slot 串行跨线程复用；curl_cffi、HTTPX 与 wreq 异步会话记录创建事件循环，跨循环使用或关闭明确抛出 RuntimeError。
    curl_cffi 响应只承诺初始提交请求及重定向 URL、状态和响应头，不伪造逐跳请求或响应体。无法解析或非 HTTP(S) 的自动重定向目标统一转换为 status_code=-1。
    显式 Cookie 请求头在可支持的后端按原值发送且不与会话 Cookie 合并；不能保证此语义且会话 Jar 非空时明确拒绝。Request.cookies 与 Cookie 请求头不可同时设置。
    显式 Cookie 头与 Request.cookies 只承诺随初始请求发送，自动重定向后的各跳是否携带因后端而异，文档须如实说明。
    requests 仅提供同步实现，重复请求头明确拒绝；不通过线程包装声称支持原生异步取消。
    requests 与 requests-go 的显式 Request.cookies 绑定初始主机，不发送到子域或其他主机；HTTPS 显式 Cookie 禁止随降级发送到 HTTP。
    会话 Cookie 保留服务端 Domain、Path、Secure 与 host-only 语义，不把无 Domain Cookie 扩大为子域可用。
    Cookies 模型与 requests 系会话发送时按 publicsuffixlist（正式依赖，随包内置列表、惰性加载、不联网更新）拒绝
    Domain 为公共后缀且不等于来源主机的 Cookie，接收与发送均校验。
11. Context.options 的 crawler.fingerprint.impersonate 覆盖当前执行的预设指纹，接受非空无首尾空白字符串或 None。
    缺省或 None 不执行指纹覆盖；curl_cffi 沿用下载器或会话默认值，None 不要求所选下载器支持指纹请求。
    节点仅对非 None 的指纹值调用可选 FingerprintDownloader / AsyncFingerprintDownloader 的 fetch_with_fingerprint。
    curl_cffi 普通与会话下载器均支持该能力，直接调用 Session.request(..., impersonate=...)，不改写会话默认配置。
    Cookie 和会话资源按原所有权保留；底层连接是否复用由 curl_cffi/libcurl 决定，不承诺每次重新握手。
    非 None 指纹在下载器不支持该能力或同步异步类别错误时明确失败；指纹名称由传输库校验，不静默忽略。
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
    页面导航超时或取消后关闭会话；已知导航网络错误（Chromium `net::ERR_*`、Firefox 证书/TLS 与重定向循环）及 API 连接、超时、
    证书校验失败与重定向超限返回 status_code=-1 并保留会话，未分类浏览器操作错误继续传播。
    page 默认返回 UTF-8 DOM 快照并移除失效实体头，可选 response 模式返回主文档原始内容；api 始终返回原始响应体。
    page 只支持无请求体 GET 和浏览器重定向；api 支持普通 HTTP 方法和已编码请求体，CONNECT 明确拒绝。
    Camoufox 包和浏览器版本分别固定，不将其浏览器配置伪装成请求级 TLS impersonate；默认不下载或启用附加扩展。

### 第四条：独立解析与批量规则

1. Parser 是 prepare(source) 与 extract(source, expression, **options) 的窄协议；
   prepare 接受自身准备结果，实例不保存当前文档。自定义实现无需继承 BaseParser 即可使用公共 match。
2. 解析器不依赖 Response、Items、Context 或 Runtime；输入、文档及其可变查询结果由调用方管理。
   BaseParser 仅提供可选的 extract_first 和 match 便捷方法，不建立注册表或动态字符串加载路径。
   Response.get(expression, default) 对 json() 执行 JMESPath，结果为 None 时返回 default；Response.extract(engine, rules)
   只接受 json、jsonpath、css、xpath、regex 固定引擎名，JSON 类读取 json()，其余读取 text，不接受动态加载路径。
   css、xpath、jsonpath、re 及其 *_first 用同一来源规则执行单个表达式，*_first 仅对列表结果取首项，不缓存解析文档。
3. CSS、XPath 使用有界编译缓存；HTML/XML 格式显式配置，XML 禁用 DTD 加载和实体展开。
   XML 在返回文档或查询前拒绝 DOCTYPE，现成元素按所属文档检查；含声明的 XML 字符串按 UTF-8 字节解析，
   标准实体与数字字符引用仍支持。CSS 输出节点、文本或属性必须明确选择，不自定义 ::text/::attr 语法；文本不自动去空白。
4. JMESPath 和 JSONPath 接收已解码 JSON 值，字符串始终是值；JSON 文本由调用方显式解码。
   JMESPath 保留原生 null/缺失语义；JSONPath 使用 python-jsonpath 严格模式，结果始终为命中值列表。
   JSONPath 校验根值为 dict、list、str、int、float、bool 或 None，拒绝文件读取接口及字节输入，不消费输入流。
   正则仅接收文本，默认返回完整匹配，捕获组由 group 显式选择，不按组数改变输出形状。
5. match 接受字段映射、Rows、Product 或递归规则序列，返回 list[dict]；规则序列始终按顺序拼接。
   Rows 只展开明确选中的列表，其 fields 可递归使用各类记录规则；字段位置的 Rows/Product 保留为列表。
   Product 各分支在同一当前源上提取并做笛卡尔积；Rows 内组合父字段映射和子 Rows 实现关联展开与父字段继承。
   Rows 中 CSS 只匹配行元素后代；XPath 使用 `.//` 相对查询，`//` 仍从所属文档根查询。
   普通字段数组、嵌套映射仍保留原结构，不隐式广播、展开或合并。
6. Rule 明确描述数量、条件、前后转换、缺失判定和默认值；Group 按序选择非缺失候选。
   MISSING 与 None、空列表和假值不同；first 只接受列表，空列表转为 MISSING。Rule 等冻结规则按对象身份比较和哈希；
   规则结构固定，默认值和常量逐次复制，回调及原生解析异常原样传播。
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

1. Interlace 使用包索引发布的 `interlace-py` 并声明兼容的次版本范围，`uv.lock` 固定完整解析结果。
   PyPI/TestPyPI 上传前必须检查 wheel 的 METADATA 和 sdist 的 PKG-INFO，直接 URL 依赖明确阻止上传，不删除依赖绕过检查。
   正式发布先发布并验证 Interlace 的包索引版本，再更新依赖和锁文件；源码构建成功不代表可以上传。
2. 正式依赖不得指向本地目录或直接 URL，不得通过隐式路径使测试通过；本地 Interlace 可编辑覆盖仅用于临时开发，
   测试与 CI 必须使用锁定的包索引版本。
3. 核心变更先在 Interlace 验证和推送，再升级 Bricks 依赖并执行领域与跨包契约测试。

### 第七条：Pythonic 实现与中文注释

1. 使用 Python 惯用方式和窄协议，保持接口简洁，避免重复抽象和隐式行为。
2. 模块、类、函数、方法及行内说明使用中文；标识符、标准格式标题和工具指令保留原有语法。
3. 每个函数和方法必须提供 Google 风格文档字符串，以中文概述职责，按实际契约填写 Args、Returns、
   Yields、Raises；无对应内容时省略该节，不机械填充空段落。
4. 成员字段（含类属性和私有字段）必须使用中文旁注或 Attributes 节说明含义、单位、默认值、所有权和可变边界。
5. 实现、测试和示例均遵守本条。

### 第八条：种子任务队列与分布式交付

1. TaskQueue 是具名任务存储的窄协议；MemoryTaskQueue、SqliteTaskQueue 与可选 RedisTaskQueue 提供相同的
   put、claim、put_claim、renew、ack、retry、reject、get、counts、forget、revive、revive_dead 和初始化租约操作；
   retry 返回 pending/dead。revive 只把 dead 恢复为立即可领取的 pending，交付次数清零并清除错误与结果；
   revive_dead 每次原子恢复有界数量，返回恢复数。
   QueueBackend 通过 Interlace 的公开 TaskBackend SPI 装配消费，并组合其公开内存后端管理线程及 Slot，
   不复制执行器或访问私有状态。Redis 通过 redis extra 安装，默认导入路径不加载 Redis 库。
2. Work.id 是队列内的去重身份；入队内容和待领取索引必须原子写入。同 ID 同内容返回已存在，
   不同内容明确报冲突。终态保留内容与去重身份，只有显式 forget 可以移除；待处理和在途任务禁止删除。
   make_work 默认根据规范化 JSON 信封（除 ID 外）生成 sha256 内容 ID，支持显式指定 ID；
   graph、inputs、options、limits、trigger 和信封版本均参与指纹，字典键排序，列表顺序保留。
   该普通函数返回独立 Work，不改变 Interlace Work 缺省使用随机 UUID 的契约，也不改写直接投递的 Work.id。
   URL 规范化、流程版本以及外部写入幂等由调用方确定；构造后修改 Work 的嵌套内容不会重算已有 ID。
3. 任务信封只传输公开 Work 字段及严格 JSON 值，不接受 pickle、动态类加载、字节、额外字段或隐式类型转换；
   parser MISSING 到达编码边界时明确报错并指出 MISSING。
   options、trigger 和执行限制显式保存；Context、Slot、Request/Response 及 Client 不直接跨队列序列化。
   两种存储都在输入和读取边界建立数据副本；Redis Lua 不解码用户业务 JSON，保留大整数与空数组形状。
4. claim 创建有期限的随机 token，attempt 从 1 开始。续租、确认、重投和拒绝均原子校验 owner、token、
   状态和到期时间；到期即失去写入权。claim 每次最多回收 100 个过期任务，重新领取使用新 token。
   retry 支持非负延迟，达到随任务保存的最大交付次数后进入 dead；显式 reject 直接进入 dead。
   put_claim 按指定 Work ID 原子完成同内容去重、持久化和就绪任务领取；有效 leased、延迟 pending 及终态
   返回 None，不绕过既有状态，内容冲突明确失败。进程在本地调度前退出时，租约到期后仍可恢复。
5. ACK 是交付完成，业务结果 success/failure 只能显式登记，不能由 ACK 推断。
   QueueBackend 不解释 Graph 的业务输出；RETRY 有界重投，REJECT 保留死信。
   LeaseLostError 只终止对应租约；续租中的暂时存储错误按心跳重试至本地租约截止，claim 错误退避重试，
   连续失败达到约一个租约时长后停止绑定并向等待方传播。handler 取消及 Interlace 控制异常先有界重投再传播，
   普通编程错误可进入死信。wait_idle 与 close 保留所有观察到的错误；写回失败由租约到期恢复。
   Redis 提供至少一次交付，持久性取决于服务端 AOF、复制与故障切换配置；外部副作用必须幂等。
6. 初始化租约使用随机 token 与递增 fence；续租、进度写入、带租约入队与结束都校验当前有效凭据。
   接管保留 JSON 进度；成功初始化不重新申请，失败允许显式重新申请，新一轮业务使用新队列名称。
   进度提交与前一次入队不是跨调用事务，恢复时以稳定任务 ID 重放；初始化完成不代表消费完成。
7. Redis 使用服务端时间和 Lua 原子迁移，同队列键使用同一 hash tag 以满足 Lua 多键同槽要求，死信使用独立有序集合索引；
   仅支持同步 redis.Redis，不支持 RedisCluster。MemoryTaskQueue 使用进程内锁与单调时间，不提供进程重启恢复，
   同名内存实例不共享数据。SqliteTaskQueue 使用标准库 sqlite3，每次操作独立连接并在 BEGIN IMMEDIATE 事务内迁移，
   同机多进程共享同一文件并在重启后保留任务与断点；使用本机墙上时钟，拒绝内存数据库，不支持跨机器或网络文件系统。
   Redis 客户端与存储均由装配方管理。
8. QueueBackend 每个绑定最多预取 concurrency 项并周期续租；交付结束会唤醒领取循环。Slot 分配及释放由 Interlace 管理。
   每次通过任务存储的根交付建立本地 Slot 链；submit_local 实现 Interlace 的可选本地发布 SPI，接管 Router
   已保留的 SlotLease，并通过 put_claim 原子持久化及领取目标 Work 后在同一进程串行延续 Slot。
   本地优先 Work 与普通领取使用相同 TaskLease、续租、ACK/RETRY/REJECT 和领取 Hook；RETRY 回到持久队列后
   不再保证原 Slot。错误与引用释放向 wait_idle 如实传播，不承诺跨进程 Slot 或线程身份。
   本地调度使用 put_claim 返回的 Work 快照，不继续执行提交方可修改的原对象，保持首次执行与恢复内容一致。
   注销和 close 停止新领取并等待已领取交付及 Slot 清理，未领取任务保留；close 不关闭借用客户端。
   wait_idle 检查已配置队列的全部 pending/leased 任务及本地 Slot 清理，不证明外部生产者已停止或全局爬取完成。
   后端关闭后 idle/wait_idle 只检查本地清理，不再等待存储中留给其他消费者的任务，允许只生产任务的 Runtime 正常关闭。
   可选 before_claim 同步门控返回 None 放行或非负秒数延后领取，延迟不能中断续租；
   before_execute 在已分配 Slot 的交付边界返回 None 继续 Graph，或返回显式 DeliveryResult 终结交付。
   有 Slot 时领取后 Hook 必须进入公开 SlotLease.execution 串行边界，不与同槽父 Graph 并发访问资源。
   可选 raise_dead(work, error) 只在结束决定已写入死信后调用，返回 False 的错误保留在死信记录中、不交给
   wait_idle 与 close；租约丢失与存储错误不经此判定，判定函数自身失败替代原错误传播。
9. 三种具体队列的 reserve(owner) 提供进入时领取的同步 with 上下文，
   hold(lease) 接管已领取凭据；TaskQueue 存储协议本身保持独立。无任务时 as 值为 None。
   每个队列实例的全部上下文共享一个惰性续租线程，按最早到期时间调度；最后一个占用退出时等待线程停止。
   同一实例不允许两个上下文同时管理同一 token，不建立全局线程或租约注册表；每个上下文只能进入一次。
10. 上下文内的 ack/retry/reject 只登记一次退出决定，保持占用直到正常退出。
    未登记决定或块内异常退出时请求有界重投，原异常继续传播，达到最大次数仍进入 dead；不根据退出推断业务成功。
    正常退出先停止该占用的续租，再对最新有效凭据写入决定。续租失败由 check/退出报告，不强制中断业务函数；
    释放失败可等待租约到期恢复，不伪称已释放。占用期间不得绕过上下文直接操作同一凭据。
    续租更新凭据不替换上下文中的 Work 对象，业务修改仅保留在当前进程的本次占用中。

### 第九条：Air 爬虫基类与领域插件

1. `bricks.spider.air.Spider` 的构造参数只保留 concurrency、downloader、task_queue、queue_name、proxy、forever、
   slots、options、limits、plugins 等基本参数与资源，concurrency 默认 1，forever 默认 False；其余配置统一为 options
   中的点分键。`spider.*` 是实例级配置，由 `bricks.models.SpiderSettings.split` 在构造时拆出并校验，未知键报错，
   不写入任务、不参与 Work ID：spider.run.strict（默认 False）、spider.init.timeout（默认 None）、
   spider.queue.lease（交付与初始化租约秒数，默认 30）、spider.queue.poll（QueueBackend 轮询秒数，默认 0.05）、
   spider.session.reuse（默认 True）与 spider.session.proxies（默认 4）。其余键作为默认执行配置合并进每个任务；
   任务级 options 携带 `spider.*` 键时明确报错。
   下载器按 fetch 协议注入，queue_name 必须与已注入任务存储的 name 一致；proxy 接受固定地址、ProxyProvider 或 Manager，
   固定地址显式规范为 URL，供应源由实例装配成 Manager。未注入 slots、同步 downloader 实现 SessionDownloader，
   spider.session.reuse 与下载器的 slot_session_reuse 均未设为 False 时，框架按 concurrency 创建长期 Slot，每个 Slot 装配
   ProxySessions(max_sessions=spider.session.proxies) 与 DownloadNode，在停止执行后关闭；托管代理的 auth、threshold、ttl 照常按请求生效，轮换地址即路由到对应会话。
   同步浏览器、异步 Session 或自定义多资源 Slot 仍由调用方显式装配。plugins 接受 Interlace 插件并在打开时装入自有 Runtime，
   不能再提供任务后端；指标等扩展经此装配，Spider 不内置统计方法。
2. 运行装配初始化、消费与同步调用三张领域 Graph。公开 Spider API 与生命周期编排位于 `bricks.spider.air`，
   `_graphs` 负责图装配，`_initialization` 驱动初始化租约和断点，`_resources` 管理自有 Slot、会话、代理及运行时资源。
   业务 Hook 的每个插槽必须是独立 HookNode，节点按 index 执行该阶段回调快照，不通过 Interlace 节点中间件装配业务插槽。
   初始化逐项或逐批通过 make_seeds、next_seed、submit、checkpoint 推进，消费通过
   make_request、download、parse、pipeline、complete 推进，并显式连接对应 before/after 插槽。
   retry、before_retry、retry_prepare、after_retry、retry_wait 构成回到 before_request 的重试分支。
   普通返回值沿 Output 传递，不隐式发布 Event；不保留 check_response、ResponseAction 或 action/stop 路由。
3. 业务入口为 make_seeds(context, **kwargs)、make_request(context)、parse(context)、item_pipeline(context)。
   make_seeds 的 record 关键字携带恢复记录，接受 None、单个 dict、记录列表或同步迭代器，迭代器可产出一批记录；
   parse 明确接受 Items 或 list[dict]。业务方法可以返回 Awaitable，但不接受异步生成器或隐式函数参数注入。
4. air.Context 的 seeds/request/response/items/target/task_queue/queue_name/form 为领域接口，
   它与 air.InitContext 均通过 Context(parent=base) 组合核心执行能力；Spider.Context 可替换并重写 make。
   当前数据由图内载体传递，实例每次 firing 独立创建；核心节点中间件替换输入后节点绑定最终输入。
   业务方法与 Hook 实际调用期间通过 ContextVar 绑定当前领域 Context；Context、InitContext 和 HookContext
   的 get_context() 提供同类别的便捷访问，按同步调用链与异步任务隔离，回调结束后恢复上一层绑定。
   install(name, value, *, nx=False) 与 obtain(name, default=None) 只使用 Slot 内 Bricks 业务命名空间；同一 Slot
   在 Spider 生命周期内跨任务复用资源，不因物理线程变化失效，不同 Slot 互相隔离。
   Context 中的运行时对象不跨队列序列化，提交只接受 JSON 种子。
   context.fetch(request) 与异步同义的 afetch 经 build_download_node() 的选择与当前 Slot 会话立即下载附加请求，
   不经过请求插槽、代理插件或重试，会话模式按 request.proxy 选择 Slot 内会话；下载器同步异步类别不匹配时明确拒绝。
   框架执行配置键统一使用 `crawler.` 前缀，不使用 `$` 前缀键；`bricks.models.CrawlerOptions` 一次解析并校验全部
   crawler.* 键，未知键在构造 Work 与节点读取时明确报错，to_options() 只生成非默认键。context.max_retry 按执行配置
   crawler.retry.max（含 Spider options 合并的默认值）、form 段级 Download.max_retry、默认 5 的顺序取重试上限；context.ok_rule 读取
   crawler.response.ok，form 另回退段级 Download.ok。重试耗尽一律进入 dead，不按成功丢弃。
   crawler.retry.delay 为重试等待秒数（默认 0，Retry(delay=...) 优先）；crawler.proxy.retry 默认 5，是 is_proxy_error
   判定的代理错误不消耗 crawler.retry.max 的独立上限，用尽后照常消耗重试预算，0 表示不单独计数。is_proxy_error(context) 可由子类重写为同步布尔判断，默认要求
   status_code=-1、请求带代理，且异常类名为 ProxyError 或信息含 proxy；context.proxy_retries 读取已用次数。
5. context.submit(*obj, affinity="current") 与初始化使用同一稳定 Work 构造规则，返回 Work 列表而不返回未来 Context。
   current 通过实例内本地 Event 交给 QueueBackend.put_claim，原子持久化并优先携带当前 SlotLease 执行；any 只 put
   到待领取队列，允许任意 Worker 领取。两者均持久化并按 Work.id 去重；显式 id 只用于单种子提交，配置从
   Spider 默认值加本次覆盖构造。父执行后续失败不撤回已接受子任务；本地进程异常后由租约恢复且不保证原 Slot。
   context.emit(type, payload, affinity="current") 为跨图通用入口：current 使用核心本地 emitter 并由 QueueBackend
   持久化目标 Work，any 通过 Runtime 普通发布；目标 Event 路由仍显式装配，跨图不继承源 options。
6. 业务流程通过 `bricks.signals` 的 Retry、Success、Failure、Skip、Pass 明确中断回调；
   信号继承独立 Signal(BaseException)，由当前领域节点捕获并转为已声明端口或业务失败，核心不解释信号。
   不保留 Context.success/failure/retry 包装或延后生效的流程标志。普通业务方法返回 None 表示继续。
   Success 跳到 complete 后才 ACK；Failure 使执行失败。Pass 只停止当前插槽剩余回调。
   Skip 只在受支持 before 插槽跳过对应业务操作，仍经过 after 插槽；跳过请求构造、下载、解析须先提供替代数据。
   Retry 只允许在 after_make_request、before_request、download、after_request，次数独立计数，
   即使尚未下载也消耗重试预算；不重建 Slot。同步与异步回调中的信号使用同一处理规则。
   普通错误、取消、超时和非法阶段信号不得作为普通重试恢复，存储失败仍不确认交付。
7. 通用成功检查、响应展示及代理设置实现为 `bricks.plugins.on_request` 中的 Before.set_proxy、
   Before.fake_ua、After.show_response、After.conditional_scripts 和 After.bypass，由 install 调用 use 注册；
   Spider 不内置 HTTP 成功判断方法。内置插件通过 hooks.current 声明使用 Context.get_context()，可直接 use。
   bypass 默认 2xx 正常返回，-1、429、5xx 抛出 Retry，其余抛出 Failure；映射未命中沿用此默认规则；同步 ok(context)
   或可信表达式返回 False 时抛出 Retry，True 时正常继续。展示与统计插件应排在会中断插槽的校验之前。
   show_response 两种格式均隐藏 URL/代理认证、查询值及片段；详细格式仅列请求头名称，不输出头值或请求体内容。
   Script namespace 仅含 context/ctx/request/response/items/seeds/signals/logger/time 及调用方显式传入对象；
   ERROR_OCCURRED 在异步路径也同步运行于共享事件循环线程，阻塞 Hook 会阻塞其他 Slot。
8. 共享 Hook 使用 on(form, index, args, kwargs, match) 与 Task(func, args, kwargs, index, match)，
   Spider.use(form, *events) 接受 Task 或参数字典。插件可以首个参数显式接收 Context，也可声明不接收 Context 并在
   调用期间通过对应领域类的 get_context() 取得；两种签名均可绑定时优先显式传参，其他业务参数不按名称反射注入。
   声明按实例绑定，支持方法继承覆盖，
   func 与同步布尔 match 使用相同 Context 传入规则；图内支持同步与异步回调及同步布尔条件，
   插槽内按 index 升序，信号终止剩余回调。
   普通回调返回 None，节点实例不保存执行状态；一个插槽的一组异步回调保持串行等待，调用线程不作为资源身份。
   hooks.current 可消除可选首参造成的签名歧义；当前 Context 不使用线程局部栈或进程级单例。
   不支持的阶段在 Spider.use 注册时明确拒绝；install 在打开时执行一次。
9. run(task_name="all", args=None, kwargs=None) 分派 all/init/spider 并在结束时关闭资源。
   run_init、run_spider 可在 with Spider(...) 中分开运行；forever=True 时消费者保持服务直到外部关闭。
   初始化调用线程等待 Execution 时定期续租，不为每条种子新增线程。失败记录与检查点受初始化 token/fence 保护。
   初始化失败仍允许已提交种子排空，原始失败继续传播。
   单项 Failure 或重试耗尽是业务拒绝：写入死信、记入 RunReport.rejections（本进程最近 20 条）与 rejected
   （按原因计数，最多 50 个原因，其余合并为 "(other)"），不中断消费。spider.run.strict 默认 False，run 正常返回报告；
   为 True 时在队列排空后抛出携带报告的 RejectedTasksError，不在首个拒绝处停止。
   程序错误、存储错误与初始化失败不受该开关影响，仍然抛出。
10. Spider 关闭自建 Runtime、QueueBackend、SlotPool 及框架自动创建的同步下载会话；注入的 Slot 资源、
    显式会话与 Redis 客户端由调用方在停止执行后关闭。关闭协作式取消初始化并排空已接受的消费，
    不强杀同步函数；禁止在自身业务或插件回调中重入阻塞生命周期方法。RunReport 返回队列快照与本进程业务拒绝摘要，
    不替代跨进程错误收集。
    Spider 记录自建 Slot，并在执行停止后归还其中的托管代理；借用 Slot 内的代理由调用方归还。
    关闭先停止 QueueBackend 领取并排空本地交付，再等待运行入口退出；等待使用完整超时预算，
    不以捕获通用 TimeoutError 的短轮询方式吞掉存储或非图 Hook 的真实超时。
11. BEFORE_START、BEFORE_CLOSE、ERROR_OCCURRED、BEFORE_GET_SEEDS、AFTER_GET_SEEDS
    在真实非图边界以 HookContext 调用同步回调。领取前没有种子与 Slot；领取后在本地 Slot 已分配、
    Graph 尚未开始的交付边界运行，不暂停在途续租。Success/Failure 可明确终结已领取交付，
    状态仍由 QueueBackend 校验租约后写回；Retry 在领取边界非法。
    领取前只允许 Pass、Skip、Wait，Wait 延后轮询而不阻塞续租；启动 Success 跳过本实例的初始化和消费。
   BEFORE_CLOSE 在停止执行后、释放自有资源前运行；错误插槽不能恢复原失败，Pass 仅停止该插槽剩余回调，
   二次错误不得覆盖原异常已有的 __cause__。
12. BEFORE_PUT_SEEDS、AFTER_PUT_SEEDS 是初始化图中的真实节点，只覆盖初始化迭代批次；
    AFTER_PUT_SEEDS 之后才提交恢复记录，失败时按稳定 Work ID 重放。Skip 表示有意跳过当前批次，
    仍执行 after 插槽和进度提交；普通 context.submit 不伪装成初始化投放。
13. build_init_graph 独立装配生产流程；build_crawl_plan 从同一消费定义选择正式 pipeline、survey_pipeline 或
    同步调用分支，声明完整出口并保留前后 Hook 和重试回路。build_graphs 与 build_call_graph 使用公开 Graph.compose
    装配所选节点和边，队列消费继续通过正常 Work/Slot/ACK 契约执行，不复制执行器或把 Plan 写入 JSON 任务信封。
14. survey(*seeds, extract=None, timeout=None) 是新实例的手动消费入口，要求 forever=False，结束后关闭。
    跳过 make_seeds 和默认 item_pipeline，保留下载、重试、解析、前后插件与动态种子提交及消费。
    默认实例使用独立内存队列；显式装配队列时也消费原有任务，不宣称只消费本次传入的种子或没有副作用。
    完成节点收集 Context，按完成顺序返回，可提取指定属性；提前 Success 尚未产生的 request/response/items 提取为 None。
    后置插件失败不确认任务；Context 仅供读取完成结果，不作为可继续执行或持有活跃会话的句柄。
15. 打开时注册 `<queue_name>.call` 同步调用图：跳过默认 item_pipeline，保留下载、重试、解析与前后插件，完成节点输出
    seeds、request（method/url）、response（status_code/url/text）、items、attempts 与 retries 的 JSON；提前 Success
    尚未产生的部分为 None。call(seed, options=None) 经 Runtime.start(slots=...) 在本实例 SlotPool 内执行，复用 Slot
    会话；调用没有根 Work，不 ACK、不写死信，失败原样抛出。调用中 current 亲和的 submit/emit 明确拒绝，any 照常持久化。
    serve() 在同一 Runtime 上以 Interlace GraphService 提供 HTTP/JSON-RPC 与可选 /metrics，传入本实例 SlotPool，
    服务期间运行队列消费者，服务器退出后关闭实例。远程 options 仅接受 allowed_options，默认 crawler.retry.max、
    crawler.fingerprint.impersonate、crawler.proxy.rotate 与 crawler.proxy.retry；crawler.response.ok/scripts 执行可信 Python，不得默认开放给远程请求。

### 第十条：可复用领域插件与代理管理

1. `plugins.make_seeds` 的 by_csv、by_sqlite、by_mongo、by_redis、by_range 是独立种子函数，
   不隐式读取 Context。批次只包含严格 JSON 对象；record 保留调用方原对象，在 yield 前更新，
   Air 在整批入队后提交进度。排序及源快照由调用方保证；Redis SCAN 不承诺断点恢复或无重复。
   参数顺序尽量与 dev 保持一致；CSV SQL 支持 `<TABLE>` 指向临时 data 表，SQLite 查询原样执行，
   不以正则改写 LIMIT/OFFSET；by_csv 对现存路径按字面路径读取，否则展开 glob；by_range 恢复直接计算起始偏移。
   各 by_* 在调用时校验参数并返回迭代器；非 JSON 类型由显式同步 transform 转换。
2. `plugins.storage` 的 to_sqlite、to_mongo、to_csv、to_redis 沿用 dev 的 path、conn、items、row_keys
   参数角色；items 省略时通过当前 Context 读取，可直接 use，也可传入显式记录独立调用。
   不修改 Items，不提前 ACK；SQLite 接受数据库文件或调用方管理的原生连接并使用事务，跨 Slot 共享连接须提供 lock，
   或按 Slot 管理独立连接；Mongo 接受原生 Collection、Database 或 Client，写入必须有确认，批量 upsert 透传 ordered。
   Mongo row_keys 仅接受顶层字面字段名，拒绝点号和以 $ 开头的名称；身份值通过显式 $eq 匹配，更新字段也不得包含点号或以 $ 开头。
   Redis 使用规范 JSON，string 身份由 row_keys 的 JSON 摘要生成。CSV 并发需调用方提供共享锁，
   CSV 追加和 Mongo 多文档写入不声称原子或自动幂等，Redis 事务不声称命令错误回滚。
3. fake_ua 缺省展开请求头中的 @chrome、@edge、@mobile、@pc、@random/@get 预设，也接受显式同步工厂。
   conditional_scripts 缺省读取执行配置 crawler.response.scripts；bypass 未传 ok 时读取 context.ok_rule。
   scripts.is_success、turn_page 和 inject 的 match、pre、post、flow、action 支持可信 Python 字符串与同步
   callable 混合组合，并通过 Type/Generator 提供 define、choice、condition、code 步骤。
   脚本编译结果有界缓存，标准命名空间仅提供 context/ctx/request/response/items/seeds/signals/logger/time；
   调用方可追加可信对象。该能力执行任意 Python，不是安全沙箱，不得运行不可信租户输入。
   callable 动作返回 None；signals、脚本错误和取消原样传播。is_success 缺省失败抛出 Retry，成功正常继续。
   turn_page 统一调用 context.submit：call_later=False 映射 affinity="current"，缺省原子持久化并优先复用当前 Slot；
   call_later=True 映射 affinity="any"，只入队等待任意 Worker 领取；success=True 仅在实际提交下一页后提前结束当前任务。
4. `proxies.ProxyProvider` 是 get/recover/clear 窄协议，BaseProxy 是可选辅助基类。
   CustomProxy、ApiProxy、RedisProxy 和 ClashProxy 接收显式配置与借用客户端；默认导入不加载
   Redis、Mongo 或 YAML，可选依赖分别通过 redis、mongo 和 clash extra 安装，缺少 YAML 时给出安装提示。
5. Manager 不创建全局注册表、单例或线程局部状态，get/now/use/recover/clear/fresh 明确接收 Slot。
   不同管理器在同一 Slot 使用不同命名空间；阈值按新请求次数与明确 ttl 秒数计算，use 只登记，
   下一次 get 才回收。Air 自动会话按请求代理路由到 Slot 内对应会话，auth、threshold 与 ttl 照常生效。
   Before.set_proxy 经 context.assign_proxy 写入并登记所选代理；重试回到 before_request 且该代理已用于下载时，
   默认经 context.clear_proxy 丢弃（托管代理不放回供应池）并重新选择；执行配置 crawler.proxy.rotate=False 时沿用原代理，
   可在 Spider options 或单个任务中设置，set_proxy 不另设同义参数。
   业务或 make_request 显式设置的 Request.proxy 不登记，重试中始终保留。context.clear_proxy 显式丢弃请求代理并清除地址。
   供应源无可用代理时报错，不静默直连；程序错误、取消、认证和回收失败不转换为空代理。
6. 会话代理仍在 prepare_session 时固定，托管请求代理不热切现有会话。Clash 控制器是共享出口，
   同一实例只允许一个未归还占用，重叠获取立即失败；不在构造时修改模式，不承诺隔离外部控制者。
   订阅内容解析与联网获取分开；Clash YAML 使用 safe_load，VMess 只支持明确实现的 TCP/WebSocket。
7. 旧版能力迁移按当前公共契约实施，不恢复动态 ref、按名称注入任意参数、进程级全局 Context 或旧导入别名。
   Context.get_context() 仅为当前回调的 ContextVar 视图，不改变 Graph 数据传递和资源所有权；共享内部状态通过具名内部模块导入，
   插件模块可在新解释器中独立导入，不依赖蜘蛛包初始化顺序。
   插件必须有独立用例和 Air 组合验证，覆盖恢复位置、写入失败、不提前确认、Slot 隔离和重试资源保持。
8. `plugins.metrics.crawler_metrics` 是不提供 capability 的 Interlace 插件，只订阅 NODE_INSPECT 并经 METRIC 通知
   crawler.requests{status}、crawler.download.seconds、crawler.retries、crawler.items、crawler.seeds.completed、
   crawler.seeds.rejected{stage} 与 crawler.seeds.submitted{source}；current/any/init 派生由 submit 经 Context.hooks 记录。
   插件不改变节点、请求或交付结果，聚合与导出由 interlace.metrics.Metrics 等 METRIC 处理函数负责，标签只含有界分类。

### 第十一条：配置式爬虫

1. `bricks.spider.form.Spider` 继承 Air，以 Config(spider, init, events) 声明流程，复用 Air 消费图、队列、重试、插件与资源装配，
   不另建执行图；旧 template 合并入 form，不保留 `$config` 轮换、`$signpost`/`$bookmark` 或 archive 标记。
2. 每个 Download 开启一段，段内依次为下载后 Task、至多一个 Parse、解析后 Task 与按序执行的 Pipeline/Task；
   首步必须为 Download，首段不得 each=True，非法顺序在打开时失败。Task 即 hooks.Task，Parse/Pipeline 函数遵循同一 Context 传入规则。
   下载后 Task 在 AFTER_REQUEST 末尾执行并允许 Retry；Download 字符串中的 {name} 按种子渲染，整串占位符保留原类型，缺失字段报错。
3. 每个 Work 只执行一段，段号保存在 Work.options 的 crawler.form.stage，不写入种子。AFTER_PIPELINE 开头经 context.submit
   持久化下一段：each=False 沿用当前种子，each=True 对每条 Items 合并当前种子派发；Download.affinity 选择 current 或 any。
   form.Context.submit 默认留在当前段；Success 结束当前种子不再推进。下一段在当前交付确认前持久化，中断后从未确认的段重放，
   已持久化的段按稳定 Work ID 去重，段内副作用由业务幂等保证。
4. Init 接受静态种子、种子列表或以 record 关键字接收恢复记录的同步函数；Layout 依次执行 default、show、factory、rename。
   Download.max_retry 与 Download.ok 为段级重试上限与成功判断，执行配置同名键优先；ok 可为 callable，不随任务序列化。
   form 必须有段推进、逐条派发、段内翻页、断点接管、段内 Retry、段级 ok/max_retry 与配置校验测试。

## 验证要求

```bash
uv run --locked pytest -q
uv run --locked mypy bricks
uv run --locked ruff check bricks tests examples .github/scripts
uv run --locked ruff format --check bricks tests examples .github/scripts
uv lock --check
uv build
uv run --locked python .github/scripts/check_distribution.py dist/*.whl dist/*.tar.gz
```

Ruff 规则集（含 B、ASYNC、UP、I）与 mypy 严格选项以 `pyproject.toml` 为准，CI 在 Python 3.10 与 3.13 上执行
Ruff、格式与 mypy 检查。只对有意为之的写法使用逐行 `noqa` 并在上方注释原因，不整文件或全局屏蔽规则；
未安装的可选依赖由 mypy 模块覆盖忽略缺失导入，不使用随安装状态失效的行内 `type: ignore`。
时间敏感测试使用带截止时间的轮询或事件同步，不依赖紧贴租约或超时的固定 sleep。
文档变更还应检查本地链接、Markdown 围栏和发生变化的 Mermaid 图。
队列分布式验证设置 `BRICKS_TEST_REDIS_URL` 指向专用测试 Redis 后运行队列测试；
测试仅清理随机生成的独立前缀，禁止清空共享数据库。必须覆盖真实 Redis、竞争领取、旧租约写入拒绝与进程退出恢复；
SQLite 队列与内存队列执行同一契约测试，并覆盖子进程退出后重新打开文件接管。
Mongo 插件验证通过 `BRICKS_TEST_MONGO_URL` 指向专用实例，只删除测试创建的随机数据库；CI 装配专用 Redis 和 Mongo 服务。
