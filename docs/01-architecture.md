# 第一章：架构与依赖

Bricks 是爬虫领域框架，Interlace 是独立发布的编排微内核。

| 所属项目 | 职责 |
| --- | --- |
| Interlace | 静态 typed Graph、Event、Execution、Slot、Runtime、插件与基础设施协议 |
| Bricks | HTTP 请求与响应、下载器、下载节点、爬虫数据记录 |
| 用户应用 | 页面解析、业务幂等、数据存储和业务流程 |

Bricks 单向依赖 Interlace 的公开 API。领域模型不持有 Runtime；下载节点通过构造器接受下载器，
通过 Context 执行协作式检查，通过 Output 交付 Response。底层 Runtime 不读取 URL 或其他爬虫领域值。

## 导入边界

```python
from interlace import Graph, Runtime
from bricks import DownloadNode, Request, Response
from bricks.downloaders.httpx import HttpxDownloader
```

Bricks 顶层只导出爬虫领域类型，不转导出 Interlace 的引擎 API。下载器通过结构化协议替换；
EventBus、任务传输和 GraphExecutor 的替换遵循 Interlace 的 SPI。

## 安装和版本

Interlace 尚未发布到 PyPI，`pyproject.toml` 使用完整 Git 提交 ID 声明直接依赖，`uv.lock` 固定完整解析结果。
`uv sync --locked` 可从远端安装，不依赖本地相邻目录、隐式路径或可编辑安装。
后续发布 Interlace 到 PyPI 后，再统一更新依赖声明和锁文件。

本地同时开发两个项目时，可显式覆盖当前环境：

```bash
uv sync --locked
uv pip install --editable ../interlace
uv run --no-sync --with pytest pytest -q
```

正式验证与 CI 必须使用锁定的远端版本。修改核心契约时，先在 Interlace 完成契约测试并提交推送，
再更新 Bricks 的依赖提交和锁文件，运行下载、模型和跨包集成测试。

## 可靠性边界

默认运行时是进程内实现，不提供持久 broker、自动重试、跨进程恢复或 exactly-once。
URL 去重、下载重试、代理与会话策略属于爬虫领域，目前没有这些策略的内建实现。
取消和同步超时保持协作式语义；已发布的事件或已交付的输出不因后续失败而撤回。

[文档目录](README.md) · [下一章：爬虫领域模型](02-crawler-models.md)
