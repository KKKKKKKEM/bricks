import hmac
import json
from hashlib import sha256

from loguru import logger

import bricks
from bricks import Request, const
from bricks.core import events, signals
from bricks.spider import air
from bricks.spider.addon import Rpc
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
        if seeds.get("$config", 0) == 0:
            return Request(
                url="https://fx1.service.kugou.com/mfanxing-home/h5/cdn/room/index/list_v2",
                params={"page": seeds["page"], "cid": 6000},
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
        if context.seeds.get("$config", 0) == 0:
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
                },
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
                },
            )

    def item_pipeline(self, context: Context):
        items = context.items
        # 写自己的存储逻辑
        logger.debug(f"存储: {items}")
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
        if context.seeds.get("$config", 0) == 0:
            # 不成功 -> 返回 False
            if context.response.get("code") != 0:
                # 重试信号
                raise signals.Retry

        # context.response.status_code = 429

        # raise signals.Retry


# 写好一个爬虫快速转换为一个外部可调用的接口，可以分为两种模式

# 导入 api 服务类
# from bricks.client.server.starlette_ import app
from bricks.client.server.sanic_ import app


# 也可以使用 sanic 的app, 效率更高, 逼近 golang, 可是没有 starlette_ 稳定
# from bricks.client.server.sanic_ import app


# 添加回调
def callback(fu, request, context, seeds):
    """
    成功回调测试, 参数可以随意增减

    :param fu: future, 可以根据 fu.cancelled() 来判断是不是客户端提前断开了连接
    :param request: 格式化的请求, 为 bricks.Request 类型
    :param context: 成功的话这个就是响应结果, 需要response 就是用content.response
    :param seeds: 这个是真正的请求的种子
    :return:
    """
    print(f"""
    类型: 成功回调
    future: {fu}
    连接已断开: {fu.cancelled()}
    请求: {request}
    种子: {seeds}
    请求类型: {type(request)}
    处理后的种子: {context.seeds}
    响应: {context.response}
""")


def errback(fu, request, seeds, context):
    """
    错误回调测试

    :param fu: future, 可以根据 fu.cancelled() 来判断是不是客户端提前断开了连接
    :param request: 格式化的请求, 为 bricks.Request 类型
    :param context: 如果是被取消的, 那么context为空, 超过了最大重试次数也会走这里, 但是此时context是存在的
    :param seeds: 这个是真正的请求的种子
    :return:
    """
    print(f"""
    类型: 错误回调
    future: {fu}
    连接已断开: {fu.cancelled()}
    请求: {request}
    种子: {seeds}
    context: {context}
    请求类型: {type(request)}
""")


def fix_404(seeds, context: air.Context):
    token = seeds.get("token")
    body = context.response.text

    if not context.response.content or context.response.status_code not in [404, 200]:
        context.response.content = json.dumps({"code": -1, "error": "-2"})
        context.response.status_code = 403
        return

    print("token: ", token)
    count = 1
    context.response.status_code = 200
    context.response.content = json.dumps({"count": count, "data": body})


# 绑定api
# 【 推荐 】1. 使用 rpc 模式，直接调用spider的核心方法，消耗种子，得到数据后返回接口
# 转为 rpc 模型，还可以传入一些参数定制爬虫
app.bind_addon(
    Rpc.wrap(MySpider),  # 需要绑定的爬虫, 如果要传实例化参数, 则写到wrap 里面
    path="/demo/rpc",  # 请求路径
    concurrency=200,  # 设置接口最大并发 200
    # callback=[callback],  # 成功回调
    # errback=[errback],  # 失败回调 -> 如请求被取消
    max_retry=1,  # 接口只重试三次
    timeout=5,  # 5s还没跑完, 直接返回超时
    methods=["POST"],
)

# 2. 是用 listener 模式 [不推荐, 请使用rpc, listener作为api爬虫]
# 转为 listener 模型，还可以传入一些参数定制爬虫
# app.bind_addon(Listener.wrap(MySpider), path="/demo/listener")  # listener模式

# # 假设这是你的共享密钥
# SECRET_KEY = "your_secret_key"


# def generate_signature(data: dict) -> str:
#     """根据数据生成签名"""
#     data_str = json.dumps(data, sort_keys=True)
#     signature = hmac.new(SECRET_KEY.encode(), data_str.encode(), sha256).hexdigest()
#     return signature


# async def verify_signature(request: Request):
#     from sanic.exceptions import Forbidden

#     # 获取请求体
#     try:
#         request_data = request.body
#     except Exception as e:
#         raise Forbidden("Invalid request body")

#     print(generate_signature(request_data))

#     # 获取请求头中的签名
#     provided_signature = request.headers.get('X-Signature')
#     if not provided_signature:
#         raise Forbidden("Missing X-Signature header")

#     # 生成签名并比较
#     expected_signature = generate_signature(request_data)
#     if not hmac.compare_digest(provided_signature, expected_signature):
#         raise Forbidden("Signature verification failed")

import sanic


async def pr(request: bricks.Request, response: bricks.Response):
    body = json.loads(request.body)
    token, site_id, good_id = body.get("token"), body.get("site_id"), body.get("good_id")
    ip = request.get_options("client_ip")
    print("ip: ",ip)
    print(response, type(response), response.json())


# 请求中间件
# app.add_middleware("request", verify_signature)

# 响应中间件
app.add_middleware("response", pr)

# 注册一个自定义视图可以使用这种方法，有点类似 bind_addon，但是部分参数不支持
# @app.route("/my_view", methods=["GET"], callback=[callback])
# async def my_view(request):
#     print(request)
#     return {"code": 0, "msg": "success"}

# 启动api服务，data 就是你需要爬取的种子
# 访问： curl --location '127.0.0.1:8888/demo/rpc' --header 'Content-Type: application/json'  --data '{"page":1}'
# 访问： curl --location '127.0.0.1:8888/demo/listener' --header 'Content-Type: application/json'  --data '{"page":1}'
if __name__ == "__main__":
    app.run()
