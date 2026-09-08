# Bricks

Bricks 是基于 [Interlace](https://github.com/KKKKKKKEM/interlace) 编排微内核构建的 Python 爬虫框架。
Bricks 负责请求、响应、数据记录、下载器、独立解析器和下载节点；Interlace 负责 Graph、Event、Runtime、执行控制与插件装配。

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

graph = Graph(entrypoint="download").add(
    download=DownloadNode()
)
with Runtime() as runtime:
    runtime.register("crawl", graph)
    response = runtime.run("crawl", Request("https://example.com"))[0].value
    print(response.status_code, response.text)
```

下载节点输出 `Response`，可以通过 Interlace 的 Edge 连接后续解析与存储节点。
异步下载使用 `AsyncDownloadNode()`，默认同样使用 curl_cffi，保持相同的数据流语义。
需要显式配置时使用 `bricks.downloaders.curl_cffi` 的 `CurlCffiDownloader` / `AsyncCurlCffiDownloader`。

HTTPX 与 requests 为可选依赖，分别通过 `uv sync --extra httpx` 或 `uv sync --extra requests` 安装。
具体实现位于 `bricks.downloaders.httpx` 和 `bricks.downloaders.requests`，不会由默认节点导入。
wreq 和 primp 也可通过 `uv sync --extra wreq --extra primp` 安装：
`bricks.downloaders.wreq` 提供 WreqDownloader / AsyncWreqDownloader（Python ≥3.11），
`bricks.downloaders.primp` 提供 PrimpDownloader / AsyncPrimpDownloader。
两者均支持 Slot 会话；wreq 支持会话内按请求切换指纹，primp 的会话指纹固定。
requests-go 可通过 `uv sync --extra requests-go` 安装，模块 `bricks.downloaders.requests_go`
提供 RequestsGoDownloader 与 RequestsGoSessionDownloader，仅支持同步调用。
该库 1.0.9 的 Go HTTPS 后端未可靠执行证书校验，适配器默认拒绝 HTTPS，需显式 `verify=False` 才启用。

浏览器下载可选 Playwright 和 Camoufox，均提供同步/异步实现、`page` / `api` 两种模式和单 Tab 会话复用。
每个会话独立 Context，可在共享 Browser 中为不同 Tab 配置不同代理；API 模式共享 Cookie，但不使用浏览器 TLS 指纹。
安装和会话装配见[浏览器下载](docs/04-browser-download.md)。

## 当前范围

完整流程可直接运行 `uv run python -m examples.crawler_pipeline --output products.jsonl`，
使用本地演示站点验证列表分页、详情解析、Cookie 会话复用与 JSONL 输出，见[完整爬取示例](docs/05-crawler-pipeline.md)。

- `bricks`：Request、Response、Cookies、Items、UploadFile、下载器协议与下载节点。
- `bricks.models`：请求体、请求头、Cookie 和记录等领域模型。
- `bricks.downloaders`：同步与异步下载协议；curl_cffi 默认实现，以及可选 HTTPX、requests、wreq、primp、requests-go 实现。
- 浏览器适配器：Playwright、Camoufox，共用页面操作、API 请求和会话生命周期。
- `bricks.nodes`：可组合到 Interlace Graph 的爬虫节点。
- `bricks.parsers`：CSS、XPath、JMESPath、JSONPath、正则及独立的 `match` 批量规则，支持笛卡尔组合与父子展开；不与 Response 绑定。

解析器可单独使用，`uv run python -m examples.crawler_parse` 演示 CSS/JSON 批量提取、商品规格组合、父字段继承、
Pipeline 跨格式连续提取与 Collect 多规则收集，
接口、第三方扩展和规则语义见[独立解析与批量规则](docs/06-parsers.md)。

当前提供缓冲响应、内存上传、请求级下载器选择、Slot 会话复用与 Context 执行配置；没有爬虫调度器、自动重试、URL 去重或流式上传。
HTTP 网络失败通过 `Response(status_code=-1)` 表达，执行取消和超时控制异常继续传播。

详细说明见[爬虫手册](docs/README.md)，通用编排语义见
[Interlace 手册](https://github.com/KKKKKKKEM/interlace/blob/main/docs/README.md)。

## 验证

HTTPS 契约测试需要本机提供 `openssl` 命令，用于生成临时测试证书。
浏览器契约测试需预先执行 `uv run python -m playwright install chromium` 和
`uv run python -m camoufox fetch official/stable/152.0.4-beta.30`；Linux 还需安装浏览器运行所需的系统库。

```bash
uv run --with pytest pytest -q
uv run --with mypy mypy bricks
uv run --with ruff ruff check bricks tests examples
uv run --with ruff ruff format --check bricks
uv build
```
