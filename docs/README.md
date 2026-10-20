# Bricks 爬虫手册

Bricks 基于 Interlace 的公开 Graph API 构建爬虫领域组件。

建议按以下顺序阅读，先明确职责和安装方式，再了解数据模型和下载流程。

1. [架构与依赖](01-architecture.md)：Bricks 与 Interlace 的职责、安装方式和开发约束。
2. [爬虫领域模型](02-crawler-models.md)：请求、响应、请求体、Cookie、cURL 和记录处理。
3. [爬虫下载](03-crawler-download.md)：下载器协议、同步与异步节点、动态选择和各传输实现边界。
4. [浏览器下载](04-browser-download.md)：Playwright/Camoufox 双模式、Tab 复用、Context 代理与生命周期。
5. [完整爬取示例](05-crawler-pipeline.md)：列表分页、详情解析、会话复用和 JSONL 输出。
6. [独立解析与批量规则](06-parsers.md)：CSS、XPath、JSON 查询、正则、连续提取、多规则收集、match 记录提取、笛卡尔组合、父子展开和解析协议扩展。

7. [任务队列](07-task-queues.md)：本地与 Redis 存储、去重、交付租约、初始化断点及 Interlace 分布式消费装配。
8. [Air 爬虫基类](08-air-spider.md)：业务参数、两张 Graph、领域 Context、复用插件及分布式运行。
9. [领域插件](09-domain-plugins.md)：种子源、持久化、分页与条件动作，以及 Air 注册示例。
10. [代理管理](10-proxies.md)：固定/API/Redis/Clash 供应源、Slot 缓存、阈值、会话约束与资源生命周期。
11. [从 dev 迁移](11-dev-migration.md)：旧插件与代理工具的新入口、调用方式和行为调整。
12. [Hook 节点与 signals](12-hooks-and-signals.md)：显式阶段节点、信号路由、生命周期与领取边界。
13. [配置式爬虫](13-form-spider.md)：以 Download 切分的步骤段、逐条派发、断点接管与旧 form/template 迁移。

Graph、Event、Execution、Slot、Runtime 与插件协议的规范和教程由
[Interlace 手册](https://github.com/KKKKKKKEM/interlace/blob/main/docs/README.md)维护。

[返回首页](../README.md)
