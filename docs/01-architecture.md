# 第一章：架构与依赖

Bricks 是爬虫领域框架，Interlace 是独立发布的编排微内核。

| 所属项目 | 职责 |
| --- | --- |
| Interlace | 静态 typed Graph、Event、Execution、Slot、Runtime、插件与基础设施协议 |
| Bricks | HTTP 请求与响应、下载器、下载节点、爬虫数据记录、独立解析器、任务存储、Air 基类、领域插件与代理供应工具 |
| 用户应用 | 站点解析规则、业务幂等、数据存储和业务流程 |

Bricks 单向依赖 Interlace 的公开 API。领域模型不持有 Runtime；下载节点通过构造器接受下载器，
通过 Context 执行协作式检查，通过 Output 交付 Response。底层 Runtime 不读取 URL 或其他爬虫领域值。

## 导入边界

```python
from interlace import Graph, Runtime
from bricks import DownloadNode, Request, Response
from bricks.downloaders.curl_cffi import CurlCffiDownloader
```

Bricks 顶层只导出爬虫领域类型，不转导出 Interlace 的引擎 API。下载器通过结构化协议替换；
EventBus、任务传输和 GraphExecutor 的替换遵循 Interlace 的 SPI。

`bricks.parsers` 通过独立 Parser 协议提供内容查询，公共 match 负责字段规则和记录展开。
解析器不绑定 Response 或执行上下文，使用方式见[独立解析与批量规则](06-parsers.md)。

`bricks.queues` 提供本地和 Redis 种子任务存储，以及初始化租约与断点。
QueueBackend 通过公开 SPI 接入 Interlace，并组合其本地调度器管理线程和 Slot，
使用方式及可靠性边界见[任务队列](07-task-queues.md)。

## 安装和版本

Interlace 尚未发布到 PyPI，`pyproject.toml` 使用完整 Git 提交 ID 声明直接依赖，`uv.lock` 固定完整解析结果。
`uv sync --locked` 可从远端安装，不依赖本地相邻目录、隐式路径或可编辑安装。
后续发布 Interlace 到 PyPI 后，再统一更新依赖声明和锁文件。

PyPI 与 TestPyPI 不接受包含直接 URL 依赖的发行包，构建成功不代表可以上传。
发布工作流在上传前通过 `.github/scripts/check_distribution.py` 检查 wheel 的真实 `Requires-Dist`，
发现直接依赖即明确失败；当前 Git 依赖仍会触发该检查，不静默跳过上传，也不删除依赖元数据。
正式发布须先将经过验证的 Interlace 版本发布到包索引，再更新 Bricks 的依赖与锁文件并运行跨包验证。
本地可执行 `uv run --locked python .github/scripts/check_distribution.py dist/*.whl` 检查构建产物。

需要在两个项目间临时联调时，可对本地开发环境做显式覆盖。此流程仅用于开发者本机运行示例，测试与 CI 均须使用锁定的远端提交：

```bash
uv sync --locked --no-group browser
uv pip install --editable ../interlace
uv run --no-sync python -m examples.crawler_download https://example.com
```

正式验证与 CI 必须使用锁定的远端版本，不得通过本地路径或可编辑安装使测试通过。修改核心契约时，
先在 Interlace 完成契约测试并提交推送，再更新 Bricks 的依赖提交和锁文件，运行下载、模型和跨包集成测试。

## 可靠性边界

默认运行时是进程内实现，不提供持久 broker、自动重试、跨进程恢复或 exactly-once。
URL 去重、下载重试、代理与会话策略属于爬虫领域。当前下载器支持代理和按 Slot 装配的会话；Air 对同步
SessionDownloader 按 concurrency 自动创建和关闭 Slot 会话，默认使用 curl_cffi，HTTPX 与 requests 为可选依赖。
Air 通过复用的响应检查插件与图内回边实现有限下载重试，
URL 规范化和业务幂等由应用提供，见 [Air 爬虫基类](08-air-spider.md)。
显式装配 QueueBackend 可以按 Work.id 去重并按交付结果有界重投；Redis 存储支持租约到期恢复，
本地子任务通过 put_claim 原子持久化并优先延续当前 SlotLease；
不改变默认内存 Runtime 的可靠性承诺，也不保证外部副作用恰好执行一次。
取消和同步超时保持协作式语义；已发布的事件或已交付的输出不因后续失败而撤回。

[文档目录](README.md) · [下一章：爬虫领域模型](02-crawler-models.md)
