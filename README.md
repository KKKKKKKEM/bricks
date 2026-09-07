# Bricks

Bricks 是基于 [Interlace](https://github.com/KKKKKKKEM/interlace) 编排微内核构建的 Python 爬虫框架。
Bricks 负责请求、响应、数据记录、下载器和下载节点；Interlace 负责 Graph、Event、Runtime、执行控制与插件装配。

## 安装与开发

```bash
uv sync --locked
uv run python -m examples.crawler_download https://example.com
```

当前源码通过 Git 依赖固定到经过验证的 Interlace 提交，不要求本地存在另一个源码目录。
此分支的拆分版本尚未发布到 PyPI。

## 下载一张页面

```python
from interlace import Graph, Runtime

from bricks import DownloadNode, Request
from bricks.downloaders.httpx import HttpxDownloader

graph = Graph(entrypoint="download").add(
    download=DownloadNode({"default": HttpxDownloader()})
)
with Runtime() as runtime:
    runtime.register("crawl", graph)
    response = runtime.run("crawl", Request("https://example.com"))[0].value
    print(response.status_code, response.text)
```

下载节点输出 `Response`，可以通过 Interlace 的 Edge 连接后续解析与存储节点。
异步下载使用 `AsyncDownloadNode` 和 `AsyncHttpxDownloader`，保持相同的数据流语义。

## 当前范围

- `bricks`：Request、Response、Cookies、Items、UploadFile、下载器协议与下载节点。
- `bricks.models`：请求体、请求头、Cookie 和记录等领域模型。
- `bricks.downloaders`：同步与异步下载协议；`httpx` 模块提供具体传输实现。
- `bricks.nodes`：可组合到 Interlace Graph 的爬虫节点。

当前提供缓冲响应、内存上传和请求级下载器选择；没有爬虫调度器、自动重试、URL 去重、跨请求会话或流式上传。
HTTP 网络失败通过 `Response(status_code=-1)` 表达，执行取消和超时控制异常继续传播。

详细说明见[爬虫手册](docs/README.md)，通用编排语义见
[Interlace 手册](https://github.com/KKKKKKKEM/interlace/blob/main/docs/README.md)。

## 验证

```bash
uv run --with pytest pytest -q
uv run --with mypy mypy bricks
uv run --with ruff ruff check bricks tests examples
uv run --with ruff ruff format --check bricks
uv build
```
