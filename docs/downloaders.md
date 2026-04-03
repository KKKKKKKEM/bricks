# 下载器

下载器（Downloader）负责将 `Request` 对象转化为 `Response` 对象，是爬虫与网络之间的桥梁。Bricks 提供多种内置下载器，并支持自定义扩展。

---

## 下载器概览

| 下载器类 | 模块 | 底层库 | 特点 |
|---|---|---|---|
| `cffi.Downloader` | `bricks.downloader.cffi` | `curl-cffi` | **默认下载器**，TLS 指纹与浏览器一致，绕过反爬检测 |
| `requests_.Downloader` | `bricks.downloader.requests_` | `requests` | 最通用，生态完善 |
| `httpx_.Downloader` | `bricks.downloader.httpx_` | `httpx` | 支持 HTTP/2，异步友好 |
| `playwright_.Downloader` | `bricks.downloader.playwright_` | `playwright` | 完整浏览器，处理 JS 渲染页面 |
| `curl.Downloader` | `bricks.downloader.curl` | `pycurl` | 系统级 curl，高性能 |
| `pyhttpx_.Downloader` | `bricks.downloader.pyhttpx_` | `pyhttpx` | TLS 指纹伪装 |
| `tls_client_.Downloader` | `bricks.downloader.tls_client_` | `tls-client` | TLS 指纹伪装 |
| `dp.Downloader` | `bricks.downloader.dp` | DrissionPage | 浏览器自动化 |
| `go_requests.Downloader` | `bricks.downloader.go_requests` | go-requests | Go 语言实现，高并发 |

---

## 默认下载器：cffi

`curl-cffi` 是 Bricks 的默认下载器，它会自动模拟浏览器的 TLS 指纹，无需额外配置即可绕过大多数基于 TLS 指纹的反爬检测。

```python
from bricks.downloader import cffi

# 直接实例化
downloader = cffi.Downloader()

# 指定浏览器指纹
downloader = cffi.Downloader(impersonate="chrome120")

# 发送请求
response = downloader.fetch({
    "url": "https://example.com",
    "method": "GET",
})
```

### 支持的浏览器指纹

`impersonate` 参数可以是以下浏览器标识符（来自 `curl-cffi`）：

```
chrome99, chrome100, chrome101, ..., chrome120
edge99, edge101
safari15_3, safari15_5, safari17_0, safari17_2_ios
firefox91esr, firefox95, firefox99, firefox105
```

### 在爬虫中指定下载器

```python
from bricks.spider import air
from bricks.downloader import cffi


class MySpider(air.Spider):
    downloader = cffi.Downloader(impersonate="chrome120")
    # 或者传字符串
    downloader = "bricks.downloader.cffi.Downloader"
```

---

## requests 下载器

需要额外安装依赖：`pip install bricks-py[requests]`

```python
from bricks.downloader import requests_


class MySpider(air.Spider):
    downloader = requests_.Downloader()
```

支持 `requests.Session`，适合需要 Cookie 持久化的场景。

---

## httpx 下载器

需要额外安装依赖：`pip install bricks-py[httpx]`

```python
from bricks.downloader import httpx_


class MySpider(air.Spider):
    downloader = httpx_.Downloader()
```

支持 HTTP/2，在高并发场景下性能优于 `requests`。

---

## playwright 下载器

需要额外安装依赖：`pip install bricks-py[playwright]`，并运行 `playwright install`。

```python
from bricks.downloader import playwright_


class MySpider(air.Spider):
    downloader = playwright_.Downloader()
```

适用于：
- JavaScript 动态渲染页面
- 需要执行 JS 脚本
- 需要截图或 PDF
- 需要模拟用户交互（点击、填表）

### playwright 特有请求选项

通过 `Request.options` 传递 playwright 专有配置：

```python
from bricks import Request

Request(
    url="https://example.com",
    options={
        "wait_for": "networkidle",   # 等待网络空闲
        "js": "return document.title",  # 执行 JS 并返回结果
        "screenshot": True,          # 截图
        "timeout": 30000,            # 毫秒
    }
)
```

---

## Request 对象参数详解

下载器接收 `Request` 对象，以下是所有可用参数：

```python
from bricks import Request

req = Request(
    url="https://api.example.com/list",

    # HTTP 方法
    method="GET",   # GET / POST / PUT / DELETE / PATCH / HEAD / OPTIONS

    # 请求参数
    params={"page": 1, "size": 20},     # URL 查询参数
    body={"key": "value"},              # 请求体（dict 自动序列化为 JSON）
    headers={"Authorization": "Bearer token"},  # 请求头
    cookies={"session": "xxx"},         # Cookie

    # 网络配置
    timeout=10,           # 超时（秒），... 使用默认值 5s
    proxies="http://127.0.0.1:7890",   # 代理
    allow_redirects=True,              # 是否跟随重定向

    # 会话
    use_session=False,   # 是否复用 Session（保持 Cookie）

    # 下载器专有选项
    options={
        "impersonate": "chrome120",   # cffi 指纹
        "verify": False,              # 关闭 SSL 验证
        "stream": False,              # 是否流式下载
        "chunk_size": 8192,           # 流式下载块大小
        "files": {...},               # 文件上传
        "auth": ("user", "pass"),     # HTTP 基础认证
    }
)
```

### 自动替换 Chrome UA

在 `headers["User-Agent"]` 中使用 `@chrome` 占位符，框架会自动替换为真实的最新 Chrome UA：

```python
Request(
    url="https://example.com",
    headers={"User-Agent": "@chrome"}  # 自动替换
)
```

### curl 导入导出

```python
# 从 curl 命令创建 Request
req = Request.from_curl("""
    curl 'https://api.example.com' \
      -H 'Authorization: Bearer token' \
      -H 'Content-Type: application/json' \
      --data '{"key":"value"}'
""")

# 将 Request 导出为 curl 命令
print(req.curl)
```

---

## Session 管理

当 `use_session=True` 时，同一个线程内的请求会共享 Session（保持 Cookie）：

```python
from bricks import Request

# 第一个请求：登录
login_req = Request(
    url="https://example.com/login",
    method="POST",
    body={"username": "user", "password": "pass"},
    use_session=True,   # 开启 Session
)

# 第二个请求：访问需要登录的页面（自动携带登录 Cookie）
page_req = Request(
    url="https://example.com/profile",
    use_session=True,  # 复用同一个 Session
)
```

---

## 流式下载

适用于大文件下载场景：

```python
from bricks import Request

req = Request(
    url="https://example.com/large-file.zip",
    options={"stream": True, "chunk_size": 65536}
)

response = downloader.fetch(req)

# 方式一：iter_content 分块读取
with open("output.zip", "wb") as f:
    for chunk in response.iter_content(chunk_size=65536):
        f.write(chunk)

# 方式二：一次性读取全部流内容
content = response.read_stream_content()
```

---

## 自定义下载器

继承 `AbstractDownloader` 并实现 `fetch()` 方法：

```python
from bricks.downloader import AbstractDownloader
from bricks.lib.request import Request
from bricks.lib.response import Response


class MyDownloader(AbstractDownloader):

    def fetch(self, request: Request) -> Response:
        # 实现你的下载逻辑
        # ...
        raw = your_http_client.get(request.real_url, headers=dict(request.headers))
        return Response(
            content=raw.content,
            status_code=raw.status_code,
            headers=raw.headers,
            url=request.real_url,
            request=request,
        )

    def make_session(self, **options):
        # 可选：实现 Session 创建逻辑
        return your_http_client.Session()
```

框架会自动通过 `_when_fetch` 装饰器包裹 `fetch` 方法，处理异常转换、耗时统计等公共逻辑，无需手动处理。

在爬虫中使用：

```python
class MySpider(air.Spider):
    downloader = MyDownloader()
```

---

## 调试模式

开启下载器调试模式可以打印详细的错误堆栈：

```python
from bricks.downloader import cffi

downloader = cffi.Downloader()
downloader.debug = True  # 开启后，请求失败时打印完整异常堆栈
```
