[license]: /LICENSE
[license-badge]: https://img.shields.io/github/license/KKKKKKKEM/bricks?style=flat-square
[prs]: https://github.com/KKKKKKKEM/bricks
[prs-badge]: https://img.shields.io/badge/PRs-welcome-brightgreen.svg?style=flat-square
[issues]: https://github.com/KKKKKKKEM/bricks/issues/new
[issues-badge]: https://img.shields.io/badge/Issues-welcome-brightgreen.svg?style=flat-square
[release]: https://github.com/KKKKKKKEM/bricks/releases/latest
[release-badge]: https://img.shields.io/github/v/release/KKKKKKKEM/bricks?style=flat-square
[pypi]: https://pypi.org/project/bricks-py/
[pypi-badge]: https://img.shields.io/pypi/v/bricks-py?style=flat-square
[python-badge]: https://img.shields.io/pypi/pyversions/bricks-py?style=flat-square

<div align="center">

# 🧱 Bricks

**像搭积木一样构建爬虫**

[![license][license-badge]][license]
[![release][release-badge]][release]
[![prs][prs-badge]][prs]
[![issues][issues-badge]][issues]
![python][python-badge]

</div>

---

## 简介

`Bricks` 是一个模块化、事件驱动的 Python 爬虫框架，旨在将爬虫开发变得像搭建积木一样简单而有趣。框架提供了从 **纯代码式** 到 **零代码配置式** 的多层次开发体验，让新手快速上手，让专家灵活掌控。

无论是简单的单页抓取、多步骤链式请求，还是分布式大规模爬取，`Bricks` 都能以一致的编程模型优雅地处理。

---

## ✨ 核心特性

| 特性 | 说明 |
|---|---|
| **事件驱动架构** | 在请求前后、存储前后等生命周期节点注册事件钩子，无需修改核心逻辑即可扩展行为 |
| **三种爬虫基类** | `air`（纯代码）、`form`（自定义流程配置）、`template`（固定流程配置），按复杂度选择 |
| **丰富的解析器** | 内置 `json` / `xpath` / `jsonpath` / `regex` 解析，声明规则即可完成数据提取 |
| **多种下载器** | 默认使用 `curl-cffi`，可选 `requests` / `httpx` / `playwright` / `pycurl` 等，支持自定义 |
| **弹性调度器** | 可伸缩线程池，同步/异步任务统一调度，自动根据任务量调节 Worker 数量 |
| **多种任务队列** | 内置 `Local`（单机）和 `Redis`（分布式）队列，接口统一，支持自定义扩展 |
| **爬虫 API 化** | 内置 `rpc` 模式，一键将爬虫转化为可远程调用的 API |
| **代理管理** | 内置 `ApiProxy` / `RedisProxy` / `ClashProxy` 等代理管理器，支持自动轮换、阈值回收 |

---

## 🚀 快速开始

### 安装

```bash
# 安装正式版
pip install -U bricks-py

# 安装最新开发版
pip install -U git+https://github.com/KKKKKKKEM/bricks.git

# 安装测试版（beta）
pip install -i https://test.pypi.org/simple/ -U bricks-py
```

可选下载器依赖：

```bash
pip install bricks-py[requests]    # requests 下载器
pip install bricks-py[httpx]       # httpx 下载器
pip install bricks-py[playwright]  # playwright 下载器
```

### 最简示例（air 爬虫）

```python
from bricks import Request, const
from bricks.core import events, signals
from bricks.spider import air
from bricks.spider.air import Context


class MySpider(air.Spider):

    def make_seeds(self, context: Context, **kwargs):
        # 返回要爬取的种子列表
        return [{"page": 1}, {"page": 2}, {"page": 3}]

    def make_request(self, context: Context) -> Request:
        seeds = context.seeds
        return Request(
            url="https://api.example.com/list",
            params={"page": seeds["page"]},
        )

    def parse(self, context: Context):
        return context.response.extract(
            engine="json",
            rules={"data.list": {"id": "id", "name": "name"}},
        )

    def item_pipeline(self, context: Context):
        print(context.items)
        context.success()  # 标记种子处理完成

    @staticmethod
    @events.on(const.AFTER_REQUEST)
    def check_response(context: Context):
        if context.response.get("code") != 0:
            raise signals.Retry  # 触发重试


if __name__ == "__main__":
    spider = MySpider()
    spider.run()
```

### 配置式示例（form 爬虫）

```python
from bricks.spider import form


class MySpider(form.Spider):

    @property
    def config(self) -> form.Config:
        return form.Config(
            init=[form.Init(func=lambda: {"page": 1})],
            spider=[
                form.Download(
                    url="https://api.example.com/list",
                    params={"page": "{page}"},
                ),
                form.Parse(
                    func="json",
                    kwargs={"rules": {"data.list": {"id": "id", "name": "name"}}},
                ),
                form.Pipeline(
                    func=lambda context: print(context.items),
                    success=True,
                ),
            ],
        )


if __name__ == "__main__":
    MySpider().run()
```

---

## 📖 文档

| 文档 | 描述 |
|---|---|
| [快速入门](docs/quickstart.md) | 5 分钟了解 Bricks 的核心概念和使用方式 |
| [爬虫基类](docs/spiders.md) | `air` / `form` / `template` 三种爬虫的详细说明 |
| [事件系统](docs/events.md) | 生命周期钩子、事件注册与触发机制 |
| [解析器](docs/parsers.md) | JSON / XPath / JSONPath / Regex 解析规则详解 |
| [下载器](docs/downloaders.md) | 各类下载器的使用与自定义扩展 |
| [任务队列](docs/queues.md) | Local / Redis 队列与分布式爬虫 |
| [代理管理](docs/proxies.md) | 代理池配置与自动轮换策略 |
| [信号机制](docs/signals.md) | Retry / Success / Failure 等控制信号 |
| [RPC 模式](docs/rpc.md) | 将爬虫暴露为远程 API |
| [存储插件](docs/storage.md) | 内置 SQLite / MongoDB / Redis / CSV 存储 |

> 完整在线文档：[https://kkkkkkkem.vercel.app/bricks](https://kkkkkkkem.vercel.app/bricks)

---

## 🏗️ 架构概览

```
bricks/
├── spider/          # 爬虫基类
│   ├── air.py       #   纯代码式爬虫
│   ├── form.py      #   自定义流程配置式爬虫
│   └── template.py  #   固定流程配置式爬虫
├── core/            # 核心机制
│   ├── context.py   #   上下文 / 流程控制
│   ├── events.py    #   事件管理器
│   ├── genesis.py   #   基础类 Chaos / Pangu
│   ├── dispatch.py  #   调度器
│   └── signals.py   #   控制信号
├── downloader/      # 下载器
├── lib/             # 基础库（Request / Response / Queue / Proxy 等）
├── plugins/         # 内置插件（storage / scripts 等）
└── rpc/             # RPC 模式
```

**爬虫生命周期**：

```
make_seeds → [BEFORE_PUT_SEEDS] → put_seeds → [AFTER_PUT_SEEDS]
                                                      ↓
[BEFORE_GET_SEEDS] → get_seeds → [AFTER_GET_SEEDS]
                                        ↓
                               [BEFORE_MAKE_REQUEST] → make_request → [AFTER_MAKE_REQUEST]
                                                                              ↓
                                                               [BEFORE_REQUEST] → on_request → [AFTER_REQUEST]
                                                                                                      ↓
                                                                                              on_response (parse)
                                                                                                      ↓
                                                                                   [BEFORE_PIPELINE] → on_pipeline → [AFTER_PIPELINE]
```

---

## 🤝 贡献

欢迎提交 [Issue][issues] 和 [Pull Request][prs]。

---

## 📄 License

[MIT](LICENSE) © Kem
