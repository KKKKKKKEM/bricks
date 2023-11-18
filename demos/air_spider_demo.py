from loguru import logger

from bricks import Request, const
from bricks.core import signals
from bricks.spider import air
from bricks.spider.air import Context


class MySpider(air.Spider):

    def make_seeds(self, context: Context, **kwargs):
        # 因为只需要爬这个种子
        # 所以可以实现 make_seeds 接口之后直接 return 出去即可
        # 如果有多个, 你有两种方案
        # 1. 将种子全部放置在一个列表里面, yield 出去, 如 return [{"page":1}, {"page":2}, {"page":3}]
        # 2. 使用生成器, 每次生产一部分, 如 yield {"page":1}, yield {"page":2}, yield {"page":3}
        return {"page": 1}

    def make_request(self, context: Context) -> Request:
        # 之前定义的种子会被投放至任务队列, 之后会被取出来, 迁入至 context 对象内
        seeds = context.seeds
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

    def parse(self, context: Context):
        response = context.response
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

    def item_pipeline(self, context: Context):
        items = context.items
        # 写自己的存储逻辑
        logger.debug(f'存储: {items}')
        # 确认种子爬取完毕后删除, 不删除的话后面又会爬取
        context.success()

    @staticmethod
    def turn_page(context: Context):
        # 判断是否存在下一页
        has_next = context.response.get('data.hasNextPage')
        if has_next == 1:
            # 提交翻页的种子
            context.submit({**context.seeds, "page": context.seeds["page"] + 1})

    @staticmethod
    def is_success(context: Context):
        """
        判断相应是否成功

        :param context:
        :return:
        """
        # 不成功 -> 返回 False
        if context.response.get('code') != 0:
            # 重试信号
            raise signals.Retry

    def before_start(self):
        super().before_start()
        self.use(const.BEFORE_PIPELINE, {"func": self.turn_page})
        self.use(const.AFTER_REQUEST, {"func": self.is_success})


if __name__ == '__main__':
    spider = MySpider(
        # # 设置代理模式 1, 该模式适用于: 你已经将代理提取至 Redis 的 proxy 里面
        # # 这样设置的话就会自动去取
        proxy={
            "ref": "bricks.lib.proxies.RedisProxy",  # 指向 Redis
            "key": "proxy",  # 指向代理 Key
            # 这个不写默认指向本地 Redis, 无密码的
            "options": {
                "host": "127.0.0.1",
                "port": 6379,
                # "password": "xsxsxax"
            },
            "threshold": 100,  # 一个代理最多使用多少次, 到这个次数之后就会归还到Redis, 然后重新拿, 默认不归还
            "scheme": "socks5"  # 代理协议, 默认是 http
        }

        # # 设置代理模式 2, 该模式适用于: 指向固定代理, 如 http://127.0.0.1:7890
        # # 这样设置的话就会自动去取
        # proxy={
        #     "ref": "bricks.lib.proxies.CustomProxy",  # 指向 Redis
        #     "key": "127.0.0.1:7890",  # 指向代理 Key
        #     "threshold": 100,  # 一个代理最多使用多少次, 到这个次数之后就会归还到Redis, 然后重新拿, 默认不归还
        #     # "scheme": "http"  # 代理协议, 默认是 http
        # },

        # # 设置代理模式 3, 该模式适用于: 你有一个提取 api,访问就会获取代理
        # # 这样设置的话就会自动去取
        # proxy={
        #     "ref": "bricks.lib.proxies.ApiProxy",  # 指向 Redis
        #     "key": "http://你的提取代理的网址",  # 指向代理 Key
        #     "threshold": 100,  # 一个代理最多使用多少次, 到这个次数之后就会归还到Redis, 然后重新拿, 默认不归还
        #     "scheme": "http"  # 代理协议, 默认是 http
        # }
    )
    spider.run()
