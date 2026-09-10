# 第四章：浏览器下载

Playwright 和 Camoufox 使用同一套页面下载、API 请求和会话管理代码，仅浏览器启动方式不同。
默认 HTTP 下载器仍是 curl_cffi；浏览器后端需要显式安装和装配。

## 安装

```bash
uv sync --extra playwright --extra camoufox
uv run python -m playwright install chromium
uv run python -m camoufox fetch official/stable/152.0.4-beta.30
```

当前固定 Playwright 1.62.0、Camoufox Python 包 0.5.6 和 Camoufox 浏览器 152.0.4-beta.30。
Playwright 其他引擎需分别安装 `firefox` 或 `webkit`；Linux 需具备对应浏览器的系统依赖。
下载器不会在运行时自动下载浏览器。Camoufox 默认禁用内置附加扩展，不因扩展下载失败阻塞导航。

| 模块 | 同步类 | 异步类 |
| --- | --- | --- |
| `bricks.downloaders.playwright` | `PlaywrightDownloader` | `AsyncPlaywrightDownloader` |
| `bricks.downloaders.camoufox` | `CamoufoxDownloader` | `AsyncCamoufoxDownloader` |

Playwright 的 `browser_type` 可选 chromium（默认）、firefox、webkit。
Camoufox 使用 Firefox；`browser_version` 选择已安装版本，`launch_options` 可传原生 os、config、humanize 等设置。
配置映射会深复制；headless、proxy、persistent_context、user_data_dir 属于保留启动字段，不能混入 launch_options。
代理通过 prepare_session 的 proxy 指定，证书策略通过 verify 指定；context_options 不得覆盖这两项。

## 两种模式

| 模式 | 发送方式 | 内容 | 页面脚本 | Cookie |
| --- | --- | --- | --- | --- |
| `page`（默认） | 在保留的 Tab 中 goto | 默认渲染后的 HTML，可选原始主文档响应 | 执行 | BrowserContext 共享 |
| `api` | BrowserContext.request.fetch | 原始 HTTP 响应体 | 不执行 | 与同 Context 的 Tab 共享 |

API 模式支持 POST、PUT、PATCH、DELETE 等普通 HTTP 请求和已经编码的 JSON、表单、multipart 内容。
它使用 Playwright 的 Node.js HTTP 客户端，不继承 Chromium/Firefox/Camoufox 的 TLS 指纹。
当前 API 模式与 BrowserContext 绑定，仍会创建或借用 Browser，但不会单独创建 Tab。

```python
from bricks import Request
from bricks.downloaders.playwright import PlaywrightDownloader

downloader = PlaywrightDownloader(mode="api")
response = downloader.fetch(Request(
    "https://example.com/api/items",
    method="POST",
    body={"name": "sample"},
))
```

page 模式只接受无请求体 GET 导航，不忽略 POST/body，也不能关闭浏览器的自动重定向。
需要这些 HTTP 控制时使用 api 模式；CONNECT 隧道不属于下载接口。
两种模式均拒绝重复请求头。page 模式还拒绝手工 Host、Cookie、Content-Length，显式 Cookie 使用 Request.cookies。

## 复用 Tab 与会话

普通 fetch 每次创建并关闭独立会话。需要复用时使用 prepare_session；返回值继续满足 Downloader 或 AsyncDownloader，
可放入 Slot 下载器映射。它还提供 fetch_page 和 fetch_api，可以在同一会话内显式选择模式。

```python
from bricks import Request
from bricks.downloaders.camoufox import CamoufoxDownloader

downloader = CamoufoxDownloader()
session = downloader.prepare_session()
try:
    session.fetch_api(Request("https://example.com/login", method="POST", body={"token": "test"}))
    first = session.fetch_page(Request("https://example.com/account"))
    second = session.fetch_page(Request("https://example.com/orders"))
finally:
    downloader.close_session(session)
```

第一次页面下载才创建 Tab，后续导航沿用同一 Tab，保留它的会话存储；API 调用不增添 Tab。
关闭的 Tab 可以在后续成功调用时重新创建；会话已关闭则明确拒绝下载，不自动重建。
Request.cookies 会按请求 URL 加入 Context CookieJar，并在本会话后续调用中保留。
响应 Set-Cookie、localStorage 和页面/API 登录状态按浏览器原生规则管理，不改写调用方的 Request。

## 每个 Tab 独立代理

代理属于 BrowserContext，不是可随时修改的 Page 属性。每个会话独占一个 Context 和一个复用 Tab，
因此多个会话可在同一个 Browser 中拥有不同代理。页面和关联 API 请求均使用该 Context 的代理。

```python
from playwright.sync_api import sync_playwright
from bricks import Request
from bricks.downloaders.playwright import PlaywrightDownloader

with sync_playwright() as driver:
    browser = driver.chromium.launch(headless=True)
    downloader = PlaywrightDownloader(browser=browser)
    first = downloader.prepare_session(proxy="http://127.0.0.1:8081")
    second = downloader.prepare_session(proxy="http://127.0.0.1:8082")
    try:
        first.fetch_page(Request("https://example.com"))
        second.fetch_page(Request("https://example.com"))
    finally:
        downloader.close_session(first)
        downloader.close_session(second)
        browser.close()
```

Camoufox 同样支持注入原生 Camoufox Browser，然后为每份会话创建独立 Context。
自建浏览器由下载器关闭；注入的 Browser 归调用方管理，关闭某个会话不会关闭其他 Context 或整个 Browser。
代理 URL 支持 HTTP、HTTPS、SOCKS5 和 URL 编码的账号密码；具体浏览器不支持的代理认证组合由原生接口报错。
固定会话拒绝 Request.proxy，即使地址相同也应在 prepare_session 设置；临时 fetch 则用 Request.proxy 创建对应 Context。
更换代理需要重新装配会话，不能热切现有 Context；不自动迁移 Cookie，也不猜测该操作是否代表切换账号。

Camoufox 的启动指纹与 Context 代理是不同层级的配置。共享 Browser 时，不承诺每个 Tab 有独立的完整设备指纹，
也不会自动把时区、GeoIP 或 WebRTC 设置重写为代理出口画像。需要这些行为时应通过原生配置装配并验证。
浏览器下载器不实现请求级 TLS impersonate 协议，传入 crawler.fingerprint.impersonate 会明确报错。

## 页面操作与内容

`after_load` 接收当前 Page，可执行定位、点击、等待、截图或 JavaScript。
同步下载器要求同步回调，异步下载器要求 async 回调；该回调只在 page 模式执行。
可使用 wait_until 设置导航等待阶段，默认 load；异步生成的业务内容应由回调显式等待，不靠固定睡眠猜测完成。

page 默认 content_mode="html"：返回回调完成后的 UTF-8 DOM 快照，URL 来自当前 Page，状态和头来自最后的主文档 HTTP 响应。
Content-Length、Content-Encoding、原 Content-Type、Digest、ETag 等不再适用的实体头被移除，内容声明为 UTF-8 HTML。
这不是服务器原始响应；SPA 修改 URL 或 DOM 不代表发生了新的 HTTP 请求。
content_mode="response" 返回最后主文档的原始响应体和头，页面脚本/回调仍会执行，但不把修改后的 DOM 混入原始内容。
两种模式的 Response.request 均为初始输入快照，history 当前为空，不伪造逐跳数据。
page 模式的请求头作用于该 Tab 的后续资源请求，每次下载会重新设置；不保证只发送给主文档。

## 超时、取消和作用域

Request.timeout 以秒输入，转换为 Playwright 毫秒；None 使用 0 禁用原生超时。
page 模式作用于导航和回调中的默认操作超时，不表示用户回调的 Python 代码可被强制终止。
api 模式设置原生请求超时，max_redirects 仅控制 API 模式，allow_redirects=False 对应 0，不自动重试。

明确的页面导航 TimeoutError 返回 status_code=-1。未分类 Playwright 错误、启动失败、用户回调异常继续传播。
浏览器会话在操作失败或取消后关闭，防止残留页面活动或请求继续修改 Cookie；需要继续时重新装配会话。
异步取消 Context 创建时会等待原生创建完成并关闭结果，避免把未登记的 Context 泄漏到外部 Browser。
异步取消会话关闭时也会等待资源栈完成清理，再向调用方传播取消；清理完成前不会把会话标记为已关闭。

同步会话只能在创建它的线程内使用和关闭，不能直接把主线程准备的同步会话交给其他 Runtime 工作线程。
异步会话只能在创建它的事件循环内使用和关闭；与已有异步 HTTP 客户端一样，必须在实际执行节点的循环中装配资源。
同一会话拒绝并发或嵌套下载；多个独立会话可并发。Slot 的串行执行语义不等于操作系统线程绑定。

完整异步示例见 [crawler_browser.py](../examples/crawler_browser.py)。

[上一章：爬虫下载](03-crawler-download.md) · [文档目录](README.md)
