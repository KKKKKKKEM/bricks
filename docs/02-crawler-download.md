# 第二章：爬虫下载

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

新增下载器只需实现对应方法，不要求继承具体 HTTPX 类。同步与异步接口分开，不返回 Response 与 Awaitable 的混合类型。
下载器应可重入，不把当前请求、响应或业务登录状态保存在共享实例中。节点不关闭注入的实例，外部资源由调用方管理；
需要跨 Work 延续的会话资源应由领域层按 Slot 管理，本章的 HTTPX 实现不提供跨请求会话。

网络传输失败由下载器转换为 `Response(status_code=-1, error=...)`。HTTP 4xx/5xx 保留正常 HTTP 响应；
配置错误、程序错误、取消和引擎控制异常必须继续传播。下载节点不通过捕获所有 Exception 将程序错误伪装成网络失败。

## 节点和动态选择

```python
from urllib.parse import urlsplit

from interlace import Graph, Runtime
from bricks import DownloadNode, Request
from bricks.downloaders.httpx import HttpxDownloader


def select_download(request: Request) -> str | None:
    if urlsplit(request.url).path.startswith("/strict/"):
        return "strict"
    return None


node = DownloadNode(
    downloaders={
        "default": HttpxDownloader(),
        "strict": HttpxDownloader(max_redirects=0),
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

异步版本使用 `AsyncDownloadNode` 与 `AsyncHttpxDownloader`，其余 Graph 组合方式相同。
完整可运行示例见 [crawler_download.py](../examples/crawler_download.py)：

```bash
uv run python -m examples.crawler_download https://example.com
```

## HTTPX 实现边界

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

[文档目录](README.md)
