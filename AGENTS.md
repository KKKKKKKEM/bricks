# Bricks Repository Instructions

## 执行要求

本文件是仓库级 AI 编码代理指令。修改代码、公共 API、运行语义、架构或文档前，必须完整阅读并遵守本文件。
实现、测试和文档以当前架构为准，不保留旧别名、隐式转换、弃用路径或兼容分支。
设计调整必须同步修改本文件、实现、测试和相关手册。规范保持简洁，教程写入 `docs/` 编号章节。

## 架构宪法

状态：Normative
适用范围：Bricks 爬虫领域框架

### 第一条：领域与微内核分离

1. Bricks 负责爬虫领域模型、下载器和领域节点；通用编排由独立的 Interlace 包提供。
2. 依赖方向固定为用户应用 -> Bricks -> Interlace；不得复制或内置 Interlace 核心实现。
3. Bricks 只依赖 Interlace 公开 API 与 SPI，不读取运行时私有状态，不改写核心契约。
4. Graph、Event、Execution、Slot、Runtime 和插件的规范由
   [Interlace 架构宪法](https://github.com/KKKKKKKEM/interlace/blob/main/AGENTS.md)维护。
   涉及核心契约的修改须同时阅读对应依赖版本的宪法并在 Interlace 实施。

### 第二条：公共 API 保持简洁

1. 顶层 `bricks` 只导出 Request、Response、Cookies、Items、UploadFile、Downloader、AsyncDownloader、
   DownloadNode 和 AsyncDownloadNode。
2. 模型位于 `bricks.models`，下载协议与实现位于 `bricks.downloaders`，领域节点位于 `bricks.nodes`。
3. Graph、Node、Event、Context、Execution、Slot 和 Runtime 从 `interlace` 导入，不在 Bricks 转导出。
4. 新能力优先组合现有模型、普通函数、领域节点与窄协议，避免不必要的包装和全局注册表。

### 第三条：下载职责与资源所有权

1. 下载器通过 `fetch(Request) -> Response` 或异步同义接口结构化替换，不要求继承具体实现。
2. 下载节点复制输入 Request 后选择和调用下载器，通过 Output 沿当前 Graph 传播 Response。
3. Request 不保存下载器实例或注册名称；选择函数由节点构造器注入，不改变核心 token 选择语义。
4. Node 和下载器默认可重入，不在共享实例中隐式保存当前执行状态；注入实例默认由调用方管理。
5. 跨逻辑执行链的资源使用 Interlace Slot，execution-local 状态遵循 Context 的命名空间边界。
6. Event 只能显式 emit，Node 返回值不得隐式变为跨图事件。目标 Graph 不在 emit 调用栈内执行。

### 第四条：错误与可靠性必须如实表达

1. 网络传输失败可转换为 `Response(status_code=-1, error=...)`；HTTP 4xx/5xx 保留正常响应。
2. 程序错误、配置错误、取消和引擎控制异常继续传播，不得作为普通业务失败恢复。
3. 取消和同步 Node timeout 是协作式语义，不宣称能够安全强杀任意 Python 函数。
4. URL 去重、重试、幂等、代理、会话和持久化属于领域策略，不增加 Runtime 或 Context 职责。
5. 已接受 Event 和已交付 Output 不因后续失败撤回；不得夸大默认内存实现的消息可靠性。
6. 文档只描述已实现能力，明确 HTTP 传输、Cookie、上传和缓冲行为的限制。

### 第五条：依赖与验证

1. Interlace 使用显式、可复现的版本来源；Git 依赖固定完整提交 ID 并维护 `uv.lock`。
2. 正式依赖不得指向本地目录，不得通过隐式路径使测试通过。
3. 核心变更先在 Interlace 验证和推送，再升级 Bricks 依赖并执行领域与跨包契约测试。
4. 领域模型校验、下载器替换、同步异步下载、取消、错误传播和请求复制必须有测试。

### 第六条：Pythonic 实现与中文注释

1. 使用 Python 惯用方式和窄协议，保持接口简洁，避免重复抽象和隐式行为。
2. 模块、类、函数、方法及行内说明使用中文；标识符、标准格式标题和工具指令保留原有语法。
3. 每个函数和方法必须提供 Google 风格文档字符串，以中文概述职责，按实际契约填写 Args、Returns、
   Yields、Raises；无对应内容时省略该节，不机械填充空段落。
4. 成员字段（含类属性和私有字段）必须使用中文旁注或 Attributes 节说明含义、单位、默认值、所有权和可变边界。
5. 实现、测试和示例均遵守本条。

## 验证要求

```bash
uv run --with pytest pytest -q
uv run --with mypy mypy bricks
uv run --with ruff ruff check bricks tests examples
uv run --with ruff ruff format --check bricks
uv build
```

文档变更还应检查本地链接、Markdown 围栏和发生变化的 Mermaid 图。
