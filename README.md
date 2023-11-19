# bricks

Bricks 是一个为了快速构建爬虫项目并进行统一管理部署而编写的应用框架。可以应用在包括数据挖掘，数据提取，状态监控，交互控制等一系列程序中。适用于页面抓取及应用
API 数据抓取等通用网络爬虫。

Bricks 拥有以下特性

快速构建爬虫

- 丰富的事件接口，可以在预留的接口内插入自定义事件，拓展你的应用
- 强大的爬虫基类，可以纯代码式编程，也可以配置化编程
- 丰富的解析器，包括 json / xpath / jsonpath / regex / json / 自定义
- 下载器可拓展，目前内置的下载器为 curl-cffi ，你也可以自定义拓展
- 灵活的调度器，调度器支持提交同步任务和异步任务，Worker 可以自动调节与释放

安装

```
pip install bricks-py
```

# 使用文档
## 快速上手

* [1.0 简介](https://github.com/KKKKKKKEM/bricks/wiki/1.0-%E7%AE%80%E4%BB%8B)
* [1.1 需求分析](https://github.com/KKKKKKKEM/bricks/wiki/1.1-需求分析)
* [1.2 代码式开发](https://github.com/KKKKKKKEM/bricks/wiki/1.2-代码式开发)
* [1.3 配置式开发](https://github.com/KKKKKKKEM/bricks/wiki/1.3-配置式开发)

