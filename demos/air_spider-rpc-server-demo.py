import threading
import time

from loguru import logger

from bricks import Request, const
from bricks.core import events, signals
from bricks.db.redis_ import Redis
from bricks.lib.queues import RedisQueue
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

        context.response.status_code = 429

        # raise signals.Retry


def demo1():
    def on_finish(ctx: MySpider.Context, err: Exception):
        # 当种子被消耗完成, 就会回调这个方法
        # 用法:
        # 1. 你可以将你的结果写入到数据库
        # 2. 通过 url 回调回传结果, 是用于 golang 搭建的 rpc 服务, 因为 python 的性能实在是太差了, 无法支撑高并发, 所以才有了这个方法
        if err:
            logger.debug(f"发生错误 -> {err}")
        else:
            logger.debug(f"跑完了 -> {ctx.response.text}")

    def submit_task():
        while True:
            for _ in range(10):
                # 提交任务, 如果服务是 golang 这类跨语言的, 可以使用 redis 队列, 然后那边只用往 redis 里面塞种子就可以了
                listener.submit({"page": 1})

            logger.debug("提交 10 个种子")
            time.sleep(2)

    # 这里配置爬虫属性, 就 2 个线程
    listener = Rpc.listen(MySpider, attrs={"concurrency": 2, "queue_name": "test"}, on_finish=on_finish)
    # 这边模拟一个后台线程发布任务
    threading.Thread(target=submit_task).start()

    # 运行 listener 服务
    listener.run()


def demo2():
    def on_finish(ctx: MySpider.Context, err: Exception):
        # 当种子被消耗完成, 就会回调这个方法
        # 用法:
        # 你可以将你的结果写入到数据库

        if err:
            logger.debug(f"发生错误 -> {err}")
            # 把结果写到了 Redis 队列里了
            redis.add(result_queue_name, {"seeds": ctx.seeds, "error": str(err)})
        else:
            logger.debug(f"跑完了 -> {ctx.response.text}")
            # 把结果写到了 Redis 队列里了
            redis.add(result_queue_name, {"seeds": ctx.seeds, "data": ctx.response.text})

    redis = Redis(password="0boNLgeuiPIxv7")
    # 这里创建一个 Redis 队列
    task_queue = RedisQueue.from_redis(redis)
    seeds_queue_name = "task-to-submit"
    result_queue_name = "task-result"
    # 这里配置爬虫属性, 就 2 个线程
    listener = Rpc.listen(MySpider, attrs={"concurrency": 2, "queue_name": seeds_queue_name, "task_queue": task_queue},
                          on_finish=on_finish)

    # 运行 listener 服务, 需要跑任务的时候, 往 Redis 里面放一个种子就可以了
    listener.run()


def demo3():
    def on_finish(ctx: MySpider.Context, err: Exception):
        # 当种子被消耗完成, 就会回调这个方法
        # 用法:
        # 你可以将你的结果通过回调 url 传回去, 例如你在种子里写一个 callback_url, 然后这里就可以通过 requests 回传结果了
        # 这种方式适用于使用 golang 开发 api 服务 / 使用方开了 api 服务(使用方通过你的提交接口发布任务, 然后通过这边调用他的回调接口回传结果)
        callback_uri = ctx.seeds.get("$callbackUri")
        if callback_uri:
            if err:
                logger.debug(f"发生错误 -> {err}")
                listener.spider.downloader.fetch(
                    {"url": callback_uri, "method": "POST", "body": {"seeds": ctx.seeds, "error": str(err)}})
            else:
                logger.debug(f"跑完了 -> {ctx.response.text}")
                listener.spider.downloader.fetch(
                    {"url": callback_uri, "method": "POST", "body": {"seeds": ctx.seeds, "data": ctx.response.text}})

    redis = Redis(password="0boNLgeuiPIxv7")
    # 这里创建一个 Redis 队列
    task_queue = RedisQueue.from_redis(redis)
    seeds_queue_name = "task-to-submit"
    # 这里配置爬虫属性, 就 2 个线程
    listener = Rpc.listen(MySpider, attrs={"concurrency": 2, "queue_name": seeds_queue_name, "task_queue": task_queue},
                          on_finish=on_finish)

    # 运行 listener 服务, 需要跑任务的时候, 往 Redis 里面放一个种子就可以了
    listener.run()


if __name__ == "__main__":
    demo2()
