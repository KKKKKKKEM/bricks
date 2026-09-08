# Bricks 爬虫手册

Bricks 基于 Interlace 的公开 Graph API 构建爬虫领域组件。

建议按以下顺序阅读，先明确职责和安装方式，再了解数据模型和下载流程。

1. [架构与依赖](01-architecture.md)：Bricks 与 Interlace 的职责、安装方式和开发约束。
2. [爬虫领域模型](02-crawler-models.md)：请求、响应、请求体、Cookie、cURL 和记录处理。
3. [爬虫下载](03-crawler-download.md)：下载器协议、同步与异步节点、动态选择和各传输实现边界。
4. [浏览器下载](04-browser-download.md)：Playwright/Camoufox 双模式、Tab 复用、Context 代理与生命周期。
5. [完整爬取示例](05-crawler-pipeline.md)：列表分页、详情解析、会话复用和 JSONL 输出。
6. [独立解析与批量规则](06-parsers.md)：CSS、XPath、JSON 查询、正则、连续提取、多规则收集、match 记录提取、笛卡尔组合、父子展开和解析协议扩展。

Graph、Event、Execution、Slot、Runtime 与插件协议的规范和教程由
[Interlace 手册](https://github.com/KKKKKKKEM/interlace/blob/main/docs/README.md)维护。

[返回首页](../README.md)
