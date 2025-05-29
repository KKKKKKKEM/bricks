[license]: /LICENSE

[license-badge]: https://img.shields.io/github/license/KKKKKKKEM/bricks?style=flat-square&a=1

[prs]: https://github.com/KKKKKKKEM/bricks

[prs-badge]: https://img.shields.io/badge/PRs-welcome-brightgreen.svg?style=flat-square

[issues]: https://github.com/KKKKKKKEM/bricks/issues/new

[issues-badge]: https://img.shields.io/badge/Issues-welcome-brightgreen.svg?style=flat-square

[release]: https://github.com/KKKKKKKEM/bricks/releases/latest

[release-badge]: https://img.shields.io/github/v/release/KKKKKKKEM/bricks?style=flat-square


<div align="center">

[![license][license-badge]][license]
[![prs][prs-badge]][prs]
[![issues][issues-badge]][issues]
[![release][release-badge]][release]

</div>

# 简介

`Bricks` 旨在将爬虫开发变得像搭建积木一样简单而有趣。这个框架的核心理念是提供一个直观、高效的方式来构建复杂的网络爬虫，同时保持代码的简洁和可维护性。无论您是刚入门的新手还是经验丰富的专家，
`Bricks` 都能让您轻松地搭建起强大的爬虫，满足从简单数据抓取到复杂网络爬取的各种需求。

通过精心设计的接口和模块化的结构，`Bricks` 使得组合、扩展和维护爬虫变得前所未有的容易。您可以像搭积木一样，快速组合出适合您需求的爬虫结构，无需深入底层细节，同时也能享受到定制化和控制的乐趣。使用
`Bricks`，您将体验到无与伦比的开发效率和灵活性，让爬虫开发不再是一件费时费力的任务。

# 特性

`Bricks` 拥有以下特性

- **基于事件触发的可拓展爬虫**：在定义好自己爬虫主体逻辑的情况下，可以不修改核心代码，在请求前后，存储前后等多个事件接口进行拓展，让爬虫流程更加清晰，且插槽也可拓展
- **爬虫基类丰富**：内置纯代码开发的 `air` 爬虫、流程化自定义配置式的 `form` 爬虫、固定流程配置式的 `template` 爬虫
- **丰富的解析器**：包括 `json` / `xpath` / `jsonpath` / `regex` / 自定义，简单解析 0 代码
- **丰富的下载器**：目前内置的下载器为 `curl-cffi` ，并且还有可选的 `requests` /  `requests-go` / `pycurl` /
  `Playwright` / `dp` / `httpx` / `tls_client`， 且开发者可以根据规范自己定制拓展
- **灵活的调度器**：调度器支持处理同步任务和异步任务，并且支持根据当前任务数量自动调节 `Worker` 数量（可伸缩线程池）
- **多种任务队列**：内置 `Local` 和 `Redis` 两种任务队列，以便应用单机和分布式爬虫，且开发者可以根据规范自己定制拓展
- **爬虫API化**：内置`rpc` 模式，可以将爬虫一键转化为可远程调用的 `api`，方便外部调用

# 安装

## 安装最新代码

```
pip install -U git+https://github.com/KKKKKKKEM/bricks.git
```

## 安装正式版

```
pip install -U bricks-py
```

## 安装测试版

```shell
# beta 版本全部都发布在 test.pypi.org
pip install -i https://test.pypi.org/simple/ -U bricks-py

```

# 使用文档

具体文档请查看 [Bricks Docs](https://kkkkkkkem.vercel.app/bricks)

