# Bricks

Bricks 是基于 [Interlace](https://github.com/KKKKKKKEM/interlace) 编排微内核构建的 Python 爬虫框架。
Bricks 负责请求、响应、数据记录、下载器、独立解析器、下载节点与种子任务存储；Interlace 负责 Graph、Event、Runtime、执行控制与插件装配。

## 安装与开发

```bash
uv sync --locked --no-group browser
uv run python -m examples.crawler_download https://example.com
```

Interlace 依赖为包索引发布的 `interlace-py>=0.3.0,<0.4`，`uv.lock` 固定完整解析结果。
同步调用 HTTP/JSON-RPC 服务需要 `service` extra（`pip install "bricks-py[service]"`）。

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
Clash YAML 订阅工具通过 `uv sync --locked --extra clash` 安装。
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
- `bricks.parsers`：CSS、XPath、JMESPath、JSONPath、正则及独立的 `match` 批量规则，支持笛卡尔组合与父子展开；
  解析器不依赖 Response，`Response.get/extract` 与 `css/xpath/jsonpath/re` 及其 `*_first` 是调用它们的便捷入口。

解析器可单独使用，`uv run python -m examples.crawler_parse` 演示 CSS/JSON 批量提取、商品规格组合、父字段继承、
Pipeline 跨格式连续提取与 Collect 多规则收集，
接口、第三方扩展和规则语义见[独立解析与批量规则](docs/06-parsers.md)。

`bricks.queues` 提供内存、SQLite（单机重启可恢复）与 Redis 任务存储、默认按内容生成 ID 的 `make_work`、交付租约、
有限重投、死信恢复（`revive/revive_dead`）和初始化断点，
QueueBackend 通过 Interlace SPI 接入执行。运行 `uv run --locked python -m examples.crawler_queue` 查看本地示例；
分布式安装与装配见[任务队列](docs/07-task-queues.md)。
手动消费可使用 `with queue.reserve(...) as task` 或 `with queue.hold(lease) as task`，
同一队列实例的占用共享续租线程，退出时执行显式确认或释放。

`bricks.spider.air.Spider` 提供种子初始化与消费两张图，保留 `downloader/task_queue/queue_name` 等业务参数，
按 concurrency 自动管理同步 Slot Session，并通过 `Context.slot_set/slot_get` 共享 Slot 资源；
`Context.submit/emit(affinity="current")` 将派生 Work 持久化并优先延续当前 Slot。
可复用的 `bricks.plugins.on_request` 插件处理响应检查、展示与代理设置。
运行 `uv run --locked python -m examples.air_spider --output products.sqlite3` 可验证分页、详情发现与幂等存储，
接口和插件注册见 [Air 爬虫基类](docs/08-air-spider.md)。
业务插槽是图中的独立 HookNode，回调通过 `raise signals.Retry()` 等信号改变流程；
插槽位置和合法信号见 [Hook 节点与 signals](docs/12-hooks-and-signals.md)。
`bricks.spider.form.Spider` 以 Download、Parse、Task、Pipeline 步骤声明流程，每个 Download 开启一段并作为独立
Work 持久化，中断后从当前段恢复，列表到详情可用 `Download(each=True)` 逐条派发，见[配置式爬虫](docs/13-form-spider.md)。
`spider.call(seed)` 在进程内同步运行一次消费流程，`spider.serve(...)` 以 HTTP/JSON-RPC 对外提供，
指标通过 `interlace.metrics.Metrics` 与 `bricks.plugins.metrics.crawler_metrics` 插件采集，
见[同步调用与 RPC 服务](docs/08-air-spider.md#同步调用与-rpc-服务)和[指标插件](docs/09-domain-plugins.md#指标插件)。

`bricks.plugins` 提供 CSV、SQLite、Mongo、Redis、范围种子源，以及存储、分页、条件动作和 UA 插件，
见[领域插件](docs/09-domain-plugins.md)。`bricks.proxies` 提供固定、API、Redis、Clash 供应源和按 Slot
管理的阈值回收，见[代理管理](docs/10-proxies.md)。旧调用方式的迁移见[迁移指南](docs/11-dev-migration.md)。
Mongo 与 YAML 订阅工具分别通过 `uv sync --extra mongo`、`uv sync --extra clash` 安装。

当前支持有限下载重试、缓冲响应、内存上传、请求级下载器选择、Slot 会话复用与 Context 执行配置；
URL 规范化和业务幂等由应用提供，尚未提供流式上传。
HTTP 网络失败通过 `Response(status_code=-1)` 表达，执行取消和超时控制异常继续传播。

详细说明见[爬虫手册](docs/README.md)，通用编排语义见
[Interlace 手册](https://github.com/KKKKKKKEM/interlace/blob/main/docs/README.md)。

## 验证

HTTPS 契约测试需要本机提供 `openssl` 命令，用于生成临时测试证书。
浏览器契约测试需预先执行 `uv run python -m playwright install chromium` 和
`uv run python -m camoufox fetch official/stable/152.0.4-beta.30`；Linux 还需安装浏览器运行所需的系统库。

```bash
uv run --locked pytest -q
uv run --locked mypy bricks
uv run --locked ruff check bricks tests examples .github/scripts
uv run --locked ruff format --check bricks tests examples .github/scripts
uv build
```

真实 Redis 队列测试通过 `BRICKS_TEST_REDIS_URL` 指定专用测试实例；未设置时跳过 Redis 用例，
本地队列契约仍会执行。详细验证命令见[任务队列](docs/07-task-queues.md)。
