# 第三章：爬虫下载

下载器负责 HTTP 传输；下载节点负责接收 Request、复制请求、选择下载器，并通过 response 端口输出 Response。
Request 不保存下载器实例或注册名称。下载能力属于爬虫领域，不增加 Runtime、Context 或核心 selector 的职责。

## 下载器协议

实现遵循仓库宪法的 Pythonic 接口与中文注释约束：方法使用 Google 风格文档字符串，按实际契约说明参数、返回值
和异常；成员字段通过中文旁注或 Attributes 节说明用途与所有权。协议、适配器、节点、测试与示例使用同一要求。

`bricks.downloaders` 提供两个结构化协议：

```python
class Downloader(Protocol):
    def fetch(self, request: Request) -> Response: ...

class AsyncDownloader(Protocol):
    async def fetch(self, request: Request) -> Response: ...
```

新增下载器只需实现对应方法，不要求继承具体实现类。同步与异步接口分开，不返回 Response 与 Awaitable 的混合类型。
下载器不把当前请求、响应保存在共享实例中。节点不关闭注入的实例，外部资源由调用方管理；
需要跨 Work 延续的会话资源按 Slot 隔离，使用本章的会话下载器，不将业务登录态放入全局共享实例。

网络传输失败由下载器转换为 `Response(status_code=-1, error=...)`。HTTP 4xx/5xx 保留正常 HTTP 响应；
配置错误、程序错误、取消和引擎控制异常必须继续传播。下载节点不通过捕获所有 Exception 将程序错误伪装成网络失败。

## 节点和动态选择

```python
from urllib.parse import urlsplit

from interlace import Graph, Runtime
from bricks import DownloadNode, Request
from bricks.downloaders.curl_cffi import CurlCffiDownloader


def select_download(request: Request) -> str | None:
    if urlsplit(request.url).path.startswith("/strict/"):
        return "strict"
    return None


node = DownloadNode(
    downloaders={
        "default": CurlCffiDownloader(),
        "strict": CurlCffiDownloader(max_redirects=0),
    },
    default="default",
    select=select_download,
)
graph = Graph(entrypoint="download").add(download=node)
with Runtime() as runtime:
    runtime.register("crawl", graph)
    response = runtime.run("crawl", Request("https://example.com"))[0].value
```

下载器集合可以混合不同的同步实现，上例用同一实现的两个实例演示不同参数。映射在节点构造时复制，之后修改原映射不影响节点。
默认名称必须已注册；未提供 select 或返回 None 时使用默认名称。返回未注册名称或非字符串会明确失败，不静默回退。
注册对象必须提供对应的同步或异步 fetch 方法；返回值必须是 Response。

select 是同步、快速、可重入的领域函数，接收本次请求副本，仅负责返回名称；不要在其中执行网络 I/O 或保存当前请求状态。
即使使用 AsyncDownloadNode，选择函数本身也保持同步，真正需要 await 的行为放在 AsyncDownloader.fetch 中。
这与核心 InputPolicy/selector contribution 无关：选择下载实现不改变 Graph 的静态连接或 token 消费规则。

同一 Graph 可以重复绑定下载节点类的不同实例，也可以由同一个节点按请求动态选择多个下载器。
节点的输入端口为 `request: Request`，输出端口为 `response: Response`，可继续连接解析或业务处理节点。
下载前后执行 Context.checkpoint；同步 I/O 的取消仍是协作式的，不能强杀正在执行的 Python 函数。

异步版本使用 `AsyncDownloadNode` 与 `AsyncCurlCffiDownloader`，其余 Graph 组合方式相同。
完整可运行示例见 [crawler_download.py](../examples/crawler_download.py)：

```bash
uv run python -m examples.crawler_download https://example.com
```

## 默认实现与可选依赖

| 模块 | 下载器 | 安装方式 |
| --- | --- | --- |
| `bricks.downloaders.curl_cffi` | `CurlCffiDownloader`、`AsyncCurlCffiDownloader` | 默认依赖 |
| `bricks.downloaders.httpx` | `HttpxDownloader`、`AsyncHttpxDownloader` | `uv sync --extra httpx` |
| `bricks.downloaders.requests` | `RequestsDownloader` | `uv sync --extra requests` |
| `bricks.downloaders.wreq` | `WreqDownloader`、`AsyncWreqDownloader` | `uv sync --extra wreq`，Python ≥3.11 |
| `bricks.downloaders.primp` | `PrimpDownloader`、`AsyncPrimpDownloader` | `uv sync --extra primp` |
| `bricks.downloaders.requests_go` | `RequestsGoDownloader` | `uv sync --extra requests-go` |
| `bricks.downloaders.playwright` | `PlaywrightDownloader`、`AsyncPlaywrightDownloader` | `uv sync --extra playwright` |
| `bricks.downloaders.camoufox` | `CamoufoxDownloader`、`AsyncCamoufoxDownloader` | `uv sync --extra camoufox` |

`DownloadNode()` 和 `AsyncDownloadNode()` 无参构造时使用 curl_cffi 临时下载器，
也为 `crawler.session.reuse=False` 提供同一无状态下载器。需要跨请求保留 Cookie 和连接池时，
仍需通过 `prepare_session()` 按 Slot 显式装配；节点不隐式创建全局会话或账户池。
开发依赖组包含可选传输及类型声明，`uv sync --no-dev` 只安装默认运行依赖。

## curl_cffi 实现边界

- 同步与异步实现均使用缓冲响应和已编码的内存请求体，支持 JSON、表单和 multipart 上传。
  请求头保留重复项，Cookie、重定向和内容解压交给 libcurl；HTTP 4xx/5xx 保留原状态码。
- 普通 fetch 创建临时 Session 并成对关闭。prepare_session 返回 `CurlCffiSessionDownloader` 或
  `AsyncCurlCffiSessionDownloader`，Cookie 与连接池保留到 close_session。
- 同步会话禁用线程局部句柄，支持同一个 Slot 串行跨线程使用；不能让多个线程同时调用同一会话。
  异步会话的准备、下载、关闭在同一事件循环内进行，取消继续传播并清理临时会话。
- `impersonate="chrome"` 等可选配置交给 curl_cffi 选择 TLS 指纹；默认 None，不模拟完整浏览器，
  不执行 JavaScript，也不自动开启浏览器默认请求头。非法指纹配置继续抛出。
- `verify=True`、`trust_env=False`、`max_redirects=20` 为默认值。trust_env 控制环境代理；
  证书来源仍遵循 curl_cffi/libcurl。显式代理优先，Slot 会话禁止通过 Request.proxy 改写代理。
- Request.timeout 是 libcurl 单次传输超时，单位秒，包含自动重定向，None 不设置该超时。
  Node/Graph timeout 仍遵循协作式控制，不强杀同步网络函数。
- Response.request 保存初始提交请求，不包含 libcurl 自动添加的 Cookie 等实际请求头；
  最终地址读取 Response.url。history 仅保存重定向 URL、状态、原因和响应头，
  不提供逐跳请求、响应体或耗时，缺失内容保持模型默认值。
- 仅明确的网络错误码转为 status_code=-1；未知、配置和资源错误继续传播。
  不自动重试，也不保证失败请求未被服务器接收。

## requests 实现边界

RequestsDownloader 和 RequestsSessionDownloader 只提供同步接口，使用独立或按 Slot 装配的 requests.Session。
verify、trust_env、max_redirects 与会话所有权规则同上；默认禁用环境代理，显式固定代理优先。
每次 fetch 完整缓冲响应，重复响应头保留，history 含逐跳响应及实际请求快照。
requests 不能发送重复请求头，因此发现重复名称时明确抛出 ValueError，不能用于要求重复请求头的站点。
Request.timeout 按连接与读取阶段生效，None 禁用这些超时，不是整个重定向链的总截止时间。
close_session 可重复调用，关闭后的包装器拒绝继续 fetch；直接关闭外部 Session 的生命周期由调用方负责。

## HTTPX 实现边界

HTTPX 是可选依赖，先执行 `uv sync --extra httpx`。默认节点不会导入该模块。

`bricks.downloaders.httpx` 提供 HttpxDownloader 和 AsyncHttpxDownloader：

- 每次 fetch 创建并关闭一个独立 Client/AsyncClient，重定向链内复用会话，调用之间不共享 Cookie 或连接池。
- 发送 real_url 和已编码 body，保留重复请求头；显式 Cookie 与 Cookie 请求头的合并、重定向规则由 HTTPX 处理。
- 自动解压 HTTPX 支持的响应编码，保留原始响应头和重复 Set-Cookie。因此 size() 可以与 Content-Length 不同。
- allow_redirects 决定是否跟随重定向；max_redirects 默认 20，超限返回内部失败。没有网络或状态码自动重试。
- Cookie 在当前重定向链内按照 HTTPX 的 CookieJar 更新，包括 Set-Cookie 的删除操作；不同 fetch 不保留登录状态。
- proxy 来自 Request；verify 默认 True；trust_env 默认 False，不自动采用环境代理或环境证书配置。
- Request.timeout 作为 HTTPX 的连接、读取、写入及连接池等待阶段超时，单位秒，None 禁用这些超时。
  它不是包括所有重定向、持续分块读取在内的总下载截止时间。Graph/Node 的 timeout 继续遵循核心执行控制语义。
- 最终 Response.cost 记录本次 fetch 从会话创建前至响应转换时的耗时；history 的 cost 使用 HTTPX 的单次请求耗时。
- 成功响应的 request 保存最终实际请求的 URL、方法、请求头与 body；history 保存每次重定向响应及其对应请求。
  快照的 body_type 为 raw，查询参数已合入 URL，Cookie 位于实际请求头，便于检查实际发送内容。
- 内部失败保留 HTTPX 异常及其关联请求；不保证保留失败之前的全部重定向历史，也不保证服务器没有收到请求。

这版采用缓冲响应和内存上传，未实现流式传输、浏览器行为、业务重试、持久化或配置文件装配。
后续配置层可以创建下载器映射和选择函数，再调用相同节点构造器；无需在 Request 中绑定下载器。

## wreq 实现边界

wreq 使用同步 `blocking.Client` 或原生异步 `Client`，支持 Cookie 存储、连接池和逐请求指纹。
`WreqDownloader(impersonate="Chrome149")` 使用原生 Profile 名称，Context 覆盖时同样填写 `Chrome149`、
`Firefox151` 等大小写一致的名称，不自动把 curl_cffi 的名称转换为 wreq 名称。
TLS 校验、环境代理与重定向上限配置与其他常规下载器一致；默认禁用环境代理。
普通 fetch 创建并关闭独立客户端；prepare_session 返回对应 WreqSessionDownloader / AsyncWreqSessionDownloader。
关闭后的包装器明确报 RuntimeError，原生异步 Client 的 close 本身是同步方法，由异步关闭协议调用。
成功响应完整读取后由原生库归还连接；失败或取消才显式关闭响应，避免原生响应上下文退出关闭可复用连接。
请求级 emulation 与客户端默认配置可能使用不同的池内连接，但共享原客户端的 Cookie 存储。

请求体按已编码字节发送，重复请求头和响应头保留；Response.request 是初始提交快照。
history 仅保存重定向响应 URL、状态和头，不包含逐跳请求、响应体或耗时。未提供的 reason 保持空字符串。
Request.timeout 是连接到响应体完整读取的请求总时限，None 不设置该时限。
wreq 当前 Method 不支持 CONNECT，遇到该方法在网络调用前明确失败。

## primp 实现边界

primp 提供同步与原生异步客户端，适配器锁定 2.0.0，支持 `impersonate="chrome_146"` 和可选 `impersonate_os`。
原生库可能对未知指纹随机回退，适配器按该版本公开预设表提前校验并拒绝无效名称。
使用 Context 覆盖普通下载器的指纹时，为该次下载构造对应临时客户端；不会修改共享下载器配置。
prepare_session 返回 PrimpSessionDownloader / AsyncPrimpSessionDownloader，客户端指纹保持固定。
会话内允许省略指纹、传 None 或重复指定同一名称；指定其他名称会明确报错。
需要另一个指纹时由调用方显式选择临时下载器，例如 `crawler.session.reuse=False`，或另行装配独立 Slot。

primp 2.0.0 没有 Client.close，其客户端上下文退出也不释放内部客户端。
因此适配器的 close_session 清除包装器持有的引用，后续 fetch 明确报 RuntimeError。
不得把已关闭包装器当作可复用会话；如果调用方另存了原生 Client 引用，资源寿命会延长到外部引用释放。
异步响应使用 stream=True 加 aread() 缓冲，避免通过 content 属性在事件循环内同步读取网络。

primp 不提供 trust_env 开关，环境代理按原生规则处理，包括 PRIMP_PROXY；显式代理在会话创建时传入。
请求头不能包含重复名称，发现时明确拒绝；原生响应头已经合并，适配器不尝试猜测并拆分。
响应 Cookie 保留原生暴露的名称和值，完整 Path、Domain 等属性不可用；CookieJar 的会话行为仍由原生库管理。
不提供重定向历史、原因短语和实际发送请求快照，Response.request 保留初始提交请求。
Request.timeout 设置本次请求超时，None 沿用原生客户端设置；适配器创建的客户端不设置默认超时。

## requests-go 实现边界

RequestsGoDownloader / RequestsGoSessionDownloader 提供同步下载和会话复用，锁定 requests-go 1.0.9。
它的 AsyncSession 使用线程池执行同步请求，取消协程不会终止底层调用，因此本适配器不提供异步包装。
HTTPS 入口使用 Go TLS 后端，HTTP 沿用原生 requests HTTPAdapter。
从 HTTP 入口跨协议重定向时也遵循原生适配器路由，不能宣称所有最终 HTTPS 响应均由 Go 发送。

本地自签名证书测试确认，原生 requests-go 即使把 Verify=True 传给 Go 仍会接受无效证书。
因此默认 verify=True 时适配器在网络调用前拒绝 HTTPS；只有调用方显式选择 verify=False 才启用该 HTTPS 后端。
需要校验证书的 HTTPS 请求应使用其他已实现下载器，不通过额外预检连接假装校验了当前 Go 连接。

指纹使用原生名称，如 TLS_CHROME_LATEST（默认）、TLS_FIREFOX_LATEST，通过构造器或已有 Context 选项传入：

```python
from bricks.downloaders.requests_go import RequestsGoDownloader

# 显式接受该后端不验证 HTTPS 证书的限制。
downloader = RequestsGoDownloader(verify=False, impersonate="TLS_CHROME_LATEST")
```

会话内覆盖指纹仍保留 Cookie；每次传入独立 TLSConfig 副本，并绑定当前 Session 的公开 ID。
不同 Slot 不共享模块级预设 ID 或 Go 会话。close_session 使用完整上下文退出，同时释放 Python 连接资源和原生 Go 会话，
不能只调用 Session.close。原生请求替换 HTTPS Adapter 时，适配器会关闭已经被替换的 Python Adapter。
TLS 握手和连接复用由原生库决定，不保证每次指纹覆盖都会重新握手。

HTTPS 超时在原生库中只有整数秒精度，None 被替换为默认 15 秒。为避免隐式改变执行设置，
适配器明确拒绝 HTTPS 的 None 和非整数超时；HTTP 保留 requests 的原有超时行为。
拒绝重复请求头和 HTTPS 显式 Content-Length，Go 会根据已编码请求体生成长度；支持内存 JSON、表单和 multipart 上传。
响应复用标准 requests 模型转换，保留 raw 中的重复响应头及逐跳重定向记录；request 保存 PreparedRequest 快照，
不宣称包含 Go 自动补充的全部线级请求头。

标准 requests 的明确网络异常转换为 status_code=-1。Go 后端把配置、传输甚至响应解析失败统一包装成 TLSClientExeption，
缺少稳定的分类错误码，适配器原样传播该异常及其 ConnectionError 包装，不通过匹配错误文案猜测是否可以恢复。

## Slot 会话与并发隔离

### 通过 Context 配置单次执行

下载节点读取 `context.options["crawler.session.reuse"]`，缺省为 `True`。
设置为 `False` 时使用构造器注入的 `isolated_downloaders`，避免改变原 Slot 会话。
该映射应使用与会话映射一致的下载器名称，`select(request)` 在选中的映射中继续生效。
普通 `CurlCffiDownloader` 每次 fetch 创建并关闭临时 Session，适合作为临时下载器。

```python
downloader = CurlCffiDownloader()
node = DownloadNode(
    slot_downloaders,
    isolated_downloaders={"default": downloader},
)
```

通过 Runtime 调用时传入 `options`；配置由引擎交给本次执行的 Context：

```python
runtime.run("crawl", request, options={"crawler.session.reuse": False})
```

直接调用节点时也可以直接构造 Context：

```python
context = Context(emit, slot, options={"crawler.session.reuse": False})
response = node.execute({"request": request}, context).value
```

同步、异步节点使用相同配置，Request 不增加字段。配置作用于当前 Graph execution 内的全部下载，
不随跨图事件继承；不支持通过修改配置影响后续节点。需要不同策略的下载应使用独立执行。
禁用复用时无需 Slot；默认复用且使用 Slot 解析函数时仍必须有 Slot。
未装配临时下载器、选择的名称不存在或配置不是布尔值时明确报错，不隐式降级。
临时请求仍会发送 Request 显式提供的 Cookie，但不读取或修改 Slot 会话的 Cookie 和连接池。
Client 代理等设置由装配方分别配置，框架不从已有 Client 猜测或复制这些设置。

### 通过 Context 覆盖指纹

`crawler.fingerprint.impersonate` 接受 curl_cffi 支持的浏览器指纹名称，
例如 `"chrome"`、`"safari"`。缺省或显式 None 沿用下载器/Session 的 impersonate，
与 curl_cffi requests-like 接口一致，None 不表示清除会话预设。
空字符串、首尾空白或错误类型会在发请求前失败；不支持的浏览器名称由 curl_cffi 抛出配置异常。

```python
from interlace import Context
from bricks import DownloadNode, Request

node = DownloadNode()
context = Context(
    lambda event: None,
    options={
        "crawler.fingerprint.impersonate": "chrome",
    },
)
response = node.execute({"request": Request("https://example.com")}, context).value
```

Runtime 调用同样支持 `runtime.run("crawl", request, options={...})`，异步节点使用相同选项。
配置作用于当前 Graph execution 内的每次下载，不随跨图事件继承；Request 不增加字段。

指纹选项不改变会话策略：默认继续使用当前下载器，Slot 会话保留 Cookie 和会话资源。
显式 `crawler.session.reuse=True` 同样支持指纹覆盖；只有 False 才选择 isolated_downloaders。
默认无参节点本身使用临时下载器，需要持久会话时仍按本章方式装配 Slot。

会话下载器直接调用原 `Session.request(..., impersonate=...)`，只覆盖本次请求参数，
不改写 Session.impersonate。响应 Cookie 正常更新原会话，后续未覆盖的请求使用原预设指纹。
TLS 校验、代理和重定向上限保持会话原配置；底层是否复用连接由 curl_cffi/libcurl 决定。
复用 Session 不等于必须复用同一条 TLS 连接，也不承诺每次请求重新握手。
需要完全隔离会话状态时才指定 `crawler.session.reuse=False`。

节点使用可选 `bricks.downloaders.FingerprintDownloader` / `AsyncFingerprintDownloader` 协议，
调用 `fetch_with_fingerprint(request, impersonate=...)`；基础 fetch(Request) 协议不变。
curl_cffi 的普通下载器和 Slot 会话下载器均实现对应能力；HTTPX、requests 不支持指纹，选中时明确报错。
wreq 的普通和会话下载器同样支持；primp 普通下载支持，会话下载只能沿用固定指纹。
requests-go 同步下载和会话下载也支持原生 TLS_ 指纹名称。
自定义下载器可以结构化实现此协议，无需继承具体实现。

### 会话资源装配

会话生命周期是可选能力，位于 `bricks.downloaders`：

| 协议 | 准备 | 关闭 |
| --- | --- | --- |
| `SessionDownloader[S]` | `prepare_session() -> S` | `close_session(session: S) -> None` |
| `AsyncSessionDownloader[S]` | `async prepare_session() -> S` | `async close_session(session: S) -> None` |

S 本身满足对应的 Downloader / AsyncDownloader，因此节点仍只调用 fetch(request)，不需要认识 HTTPX、
浏览器或其他库的原生 session。基础协议不要求实现这两个方法；只有支持会话的下载器实现扩展协议。
协议可用于类型标注及运行时能力检查，不要求继承。每次 prepare_session 必须生成独立业务状态；
初始化失败继续抛出，准备成功后由装配方保证关闭，close_session 要允许重复调用。

curl_cffi、HTTPX、wreq、primp 的同步与异步下载器，以及 RequestsDownloader 均实现对应会话能力；
准备时继承 verify、trust_env 和 max_redirects，还可以通过 `prepare_session(proxy=...)` 配置固定代理。
基础下载器直接 fetch 仍使用独立临时会话；只有 prepare_session 的返回值会跨请求保留 Cookie 和连接池。

`DownloadNode` 与 `AsyncDownloadNode` 的 `downloaders` 也可以是同步函数，签名为
`(Slot) -> Mapping[str, Downloader]` 或对应异步下载器映射。节点每次执行使用 `Context.slot` 调用函数，
校验映射后继续按 `select(request)` 选择下载器。函数只读取已装配资源，不执行网络 I/O；没有 Slot 时明确失败。
固定映射在构造时校验并复制；函数返回的映射在每次执行时校验并复制，函数自身的异常继续传播。

```python
from collections.abc import Mapping

from interlace import Slot
from bricks import Downloader, DownloadNode


def slot_downloaders(slot: Slot) -> Mapping[str, Downloader]:
    """读取执行槽中由装配方管理的下载器。

    Args:
        slot: 当前逻辑执行链的资源槽。

    Returns:
        当前槽专用的下载器映射。
    """
    return slot["crawler.downloaders"]


node = DownloadNode(slot_downloaders)
```

`HttpxSessionDownloader` 和 `AsyncHttpxSessionDownloader` 是 HTTPX prepare_session 的返回类型，
位于 `bricks.downloaders.httpx`；也可直接接收外部 Client/AsyncClient，此时由外部所有者管理关闭。
每个 Slot 应装配各自的会话与下载器映射。
节点不再为这类下载器逐次建立会话。会话的代理、TLS、环境配置、连接池限制与最大重定向次数由 Client 设置；
每次 Request 的 timeout 和 allow_redirects 仍生效。Request.proxy 非 None 会抛出 ValueError，
需要多个代理时装配多个会话并通过 select 选择，不能在共享 Client 上临时改代理。

Slot 跟随逻辑执行链，不绑定操作系统线程。同进程跨 Graph 的事件延续保持 Slot，同槽 Graph execution 串行，
不同 Slot 可以并发。因此这里提供的是逻辑链隔离，不是 thread-local，也不适合要求固定线程的第三方会话。
不要在节点内部自行并发使用同槽可变 Cookie 会话。

完整装配见 [crawler_session.py](../examples/crawler_session.py)：通过 SlotPool 创建独立会话，
用 `runtime.on(..., slots=pool)` 配置事件消费。直接 `runtime.run()` 没有这里配置的池，不用于该示例的会话入口。
调用方先停止 Runtime，再关闭池和 Client；SlotPool.close() 不会递归关闭其中的资源。

Slot 归还池后，资源和 Cookie 仍然保留，后续根执行链可能复用该槽。因此 Slot 不自动等于账号：
不同账号须由领域装配分配到各自的会话资源或独立池，不能把混合账号的根请求随机交给同一个会话池。

异步 Client 必须在使用它的同一事件循环内执行 aclose。默认 Interlace 执行器使用后台事件循环，
不能在外层另起 asyncio.run 来关闭已在后台使用的会话；应在执行器仍运行时通过该执行器上的清理 AsyncNode
关闭所有会话，或由提供该循环的装配层负责清理。也不能把一个 AsyncClient 跨不同执行器的事件循环复用。
上述 HTTP 客户端取消和程序错误继续传播，取消单次 fetch 不会替调用方关闭整个会话。
浏览器会话的失败关闭策略不同，见[浏览器下载](04-browser-download.md)。

[上一章：爬虫领域模型](02-crawler-models.md) · [下一章：浏览器下载](04-browser-download.md) · [文档目录](README.md)
