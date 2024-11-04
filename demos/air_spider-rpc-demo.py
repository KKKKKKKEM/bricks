import math

from loguru import logger

from bricks import Request, const
from bricks.client.server import app
from bricks.core import signals, events
from bricks.spider import air
from bricks.spider.addon import Rpc, Listener
from bricks.spider.air import Context


class MySpider(air.Spider):

    def make_seeds(self, context: Context, **kwargs):
        # 因为只需要爬这个种子
        # 所以可以实现 make_seeds 接口之后直接 return 出去即可
        # 如果有多个, 你有两种方案
        # 1. 将种子全部放置在一个列表里面, yield 出去, 如 return [{"page":1}, {"page":2}, {"page":3}]
        # 2. 使用生成器, 每次生产一部分, 如 yield {"page":1}, yield {"page":2}, yield {"page":3}
        return [{"page": 1}, {"page": 2}, {"page": 3}, {"page": 4}]

    def make_request(self, context: Context) -> Request:
        # 之前定义的种子会被投放至任务队列, 之后会被取出来, 迁入至 context 对象内
        seeds = context.seeds
        if seeds.get('$config', 0) == 0:
            return Request(
                url="https://fx1.service.kugou.com/mfanxing-home/h5/cdn/room/index/list_v2",
                params={
                    "page": seeds["page"],
                    "cid": 6000
                },
                headers={
                    "User-Agent": "Mozilla/5.0 (Linux; Android 10; Redmi K30 Pro) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Mobile Safari/537.36",
                    "Content-Type": "application/json;charset=UTF-8",
                },
            )
        else:
            return Request(
                url="https://www.baidu.com/sugrec?pre=1&p=3&ie=utf-8&json=1&prod=pc&from=pc_web&wd=1&req=2&csor=1&_=1703142848459",
                headers={
                    "User-Agent": "@chrome",
                    "Content-Type": "application/json;charset=UTF-8",
                },
            )

    def parse(self, context: Context):
        response = context.response
        if context.seeds.get('$config', 0) == 0:
            return response.extract(
                engine="json",
                rules={
                    "data.list": {
                        "userId": "userId",
                        "roomId": "roomId",
                        "score": "score",
                        "startTime": "startTime",
                        "kugouId": "kugouId",
                        "status": "status",
                    }
                }
            )
        else:

            return response.extract(
                engine="json",
                rules={
                    "g": {
                        "type": "type",
                        "sa": "sa",
                        "q": "q",
                    }
                }
            )

    def item_pipeline(self, context: Context):
        items = context.items
        # 写自己的存储逻辑
        logger.debug(f'存储: {items}')
        # 确认种子爬取完毕后删除, 不删除的话后面又会爬取
        context.success()

    @staticmethod
    @events.on(const.AFTER_REQUEST)
    def is_success(context: Context):
        """
        判断相应是否成功

        :param context:
        :return:
        """
        if context.seeds.get('$config', 0) == 0:
            # 不成功 -> 返回 False
            if context.response.get('code') != 0:
                # 重试信号
                raise signals.Retry


# 写好一个爬虫快速转换为一个外部可调用的接口，可以分为两种模式

# 【 推荐 】1. 使用 rpc 模式，直接调用spider的核心方法，消耗种子，得到数据后返回接口
# 导入 api 服务类

# 请求中间件
# @app.on("request")
# def hook(request: sanic.Request):
#     # 判断客户端是否已经断开了连接
#     print(request, request.transport.is_closing())
#
#     # 获取url参数
#     print(request.args)
#
#     # 获取请求json body
#     print(request.json)
#
#
# # 响应中间件
# @app.on("response")
# def hook(response: sanic.HTTPResponse):
#     # 修改响应头
#     print(response.headers)
#     response.headers["aaa"] = 1
#
#     # 获取请求body -》 是bytes类型
#     print(response.body)


# 绑定api

# 转为 rpc 模型，还可以传入一些参数定制爬虫
# 这里可以指定一些参数，查看源码可知
# :param concurrency: 接口并发数量，超出该数量时会返回429
# :param form: 接口返回类型, $response-> 响应; $items -> items
# :param max_retry: 种子最大重试次数
# :param tags: 接口标签
# :param obj: 需要绑定的 Rpc
# :param path: 访问路径
# :param method: 访问方法
# :param adapter: 自定义视图函数
app.bind_addon(Rpc.wrap(MySpider), path="/demo/rpc", concurrency=200)  # rpc模式，并发限制为1

# 2. 是用 listener 模式
# 转为 listener 模型，还可以传入一些参数定制爬虫
# app.bind_addon(Listener.wrap(MySpider), path="/demo/listener")  # listener模式

# 启动api服务，data 就是你需要爬取的种子
# 访问： curl --location '127.0.0.1:8888/demo/rpc' --header 'Content-Type: application/json'  --data '{"page":1}'
# 访问： curl --location '127.0.0.1:8888/demo/listener' --header 'Content-Type: application/json'  --data '{"page":1}'
if __name__ == '__main__':
    app.run()
