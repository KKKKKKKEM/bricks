# 第五章：完整爬取示例

[完整示例](../examples/crawler_pipeline.py)将列表、分页、详情解析和 JSONL 写入连接起来。
无需外部站点或额外解析依赖，即可运行：

```bash
uv run python -m examples.crawler_pipeline --output products.jsonl
```

未传 URL 时自动启动仅监听本机的[演示站点](../examples/crawler_demo.py)，结束后关闭。
站点包含两张列表页和两张详情页；入口设置 Cookie，后续页面校验 Cookie。
第二页包含重复详情链接和返回第一页的链接，用于验证本次运行不会重复抓取。
成功时打印 `2 records -> products.jsonl`，文件每行是一条含 `url`、`name`、`price` 的 JSON 记录。
价格保留站点字符串，避免浮点转换。

## 组合方式

每次 Graph execution 依次执行 `DownloadNode -> Parse -> WriteJsonl`，通过显式 Output 传递
`Response` 和示例本地的 `Page`。解析函数 `parse_page` 返回发现的链接及 `Items`；存储节点逐条写入
JSONL，并将页面结果交回调用方。调用方维护串行待抓取队列，继续访问详情和下一页。

调用方创建一个 curl_cffi 会话，通过固定下载器映射注入节点，所有页面属于同一次串行业务流程。
Runtime 停止后由调用方关闭会话，文件同样由调用方关闭；节点不负责销毁注入资源。
本例不使用事件消费者或 SlotPool；需要多个逻辑执行链并发时，按
[会话示例](../examples/crawler_session.py)为每个 Slot 独立装配会话，不能直接共享本例的文件节点。

## 接入自己的页面

```bash
uv run python -m examples.crawler_pipeline http://127.0.0.1:8000/catalog --output local-products.jsonl --max-pages 100
```

当前解析器只识别下面的 HTML 契约，实际网站应修改 `PageParser` 或替换 `parse_page`：

```html
<a rel="item" href="/product/1">详情</a>
<a rel="next" href="/catalog?page=2">下一页</a>
<meta name="product:name" content="笔记本">
<meta name="product:price" content="12.00">
```

链接相对响应 URL 解析，仅访问与入口 scheme、netloc 相同的地址，移除 fragment 后按完整 URL
在内存中去重。示例禁用自动重定向和环境代理；HTTP 非 2xx、网络失败和缺失商品字段均明确失败。
默认最多抓取 20 个页面，包含列表和详情；超过上限报错。输出使用独占创建模式，拒绝覆盖已有文件。
异常时保留已写入的部分文件，不代表完整抓取成功；示例不提供自动重试、断点恢复、通用 URL 规范化
或持久化去重。取消及引擎异常继续传播。

这是应用层组合示例，没有增加 Bricks 公共 API 或 Interlace 核心职责。

[上一章：浏览器下载](04-browser-download.md) · [下一章：独立解析与批量规则](06-parsers.md) · [文档目录](README.md)
