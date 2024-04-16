import threading

from loguru import logger

from bricks import Request, const
from bricks.core import signals, events
from bricks.core.context import Error
from bricks.spider import air
from bricks.spider.air import Context


class MySpider(air.Spider):

    def make_seeds(self, context: Context, **kwargs):
        # 因为只需要爬这个种子
        # 所以可以实现 make_seeds 接口之后直接 return 出去即可
        # 如果有多个, 你有两种方案
        # 1. 将种子全部放置在一个列表里面, yield 出去, 如 return [{"page":1}, {"page":2}, {"page":3}]
        # 2. 使用生成器, 每次生产一部分, 如 yield {"page":1}, yield {"page":2}, yield {"page":3}
        return [{"page": 1}]

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
    @events.on(const.BEFORE_PIPELINE)
    def turn_page():
        context: Context = Context.get_context()
        # 判断是否存在下一页
        has_next = context.response.get('data.hasNextPage')
        if has_next == 1:
            context.submit({**context.seeds, "$config": 1})
            # 提交翻页的种子
            context.submit({**context.seeds, "page": context.seeds["page"] + 1})

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

    def catch(self, exception: Error):
        super().catch(exception)


@events.on(const.BEFORE_WORKER_CLOSE)
def test():
    print(f'{threading.current_thread()} 要关闭啦')


@events.on(const.BEFORE_WORKER_START)
def test2():
    print(f'{threading.current_thread()} 启动啦')


if __name__ == '__main__':
    spider = MySpider()
    spider.run()
