from loguru import logger

from bricks import Request, const
from bricks.core import events, signals
from bricks.spider import air
from bricks.spider.air import Context


# 编写一个 spider
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
        if seeds.get("$config", 0) == 0:
            return Request(
                url="https://fx1.service.kugou.com/mfanxing-home/h5/cdn/room/index/list_v2",
                params={"page": seeds["page"], "cid": 6000},
                headers={
                    'User-Agent': "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0",
                    'Accept': "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
                    'accept-language': "zh-CN,zh;q=0.9,fr-FR;q=0.8,fr;q=0.7,en;q=0.6,en-GB;q=0.5,en-US;q=0.4",
                    'cache-control': "no-cache",
                    'dnt': "1",
                    'pragma': "no-cache",
                    'priority': "u=0, i",
                    'sec-ch-ua': "\"Microsoft Edge\";v=\"131\", \"Chromium\";v=\"131\", \"Not_A Brand\";v=\"24\"",
                    'sec-ch-ua-mobile': "?0",
                    'sec-ch-ua-platform': "\"macOS\"",
                    'sec-fetch-dest': "document",
                    'sec-fetch-mode': "navigate",
                    'sec-fetch-site': "none",
                    'sec-fetch-user': "?1",
                    'upgrade-insecure-requests': "1"
                },
                options={
                    "impersonate": "safari17_2_ios",
                }
            )
        else:
            return Request(
                url="https://www.baidu.com/sugrec?pre=1&p=3&ie=utf-8&json=1&prod=pc&from=pc_web&wd=1&req=2&csor=1&_=1703142848459",
                headers={
                    'User-Agent': "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0",
                    'Accept': "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
                    'accept-language': "zh-CN,zh;q=0.9,fr-FR;q=0.8,fr;q=0.7,en;q=0.6,en-GB;q=0.5,en-US;q=0.4",
                    'cache-control': "no-cache",
                    'dnt': "1",
                    'pragma': "no-cache",
                    'priority': "u=0, i",
                    'sec-ch-ua': "\"Microsoft Edge\";v=\"131\", \"Chromium\";v=\"131\", \"Not_A Brand\";v=\"24\"",
                    'sec-ch-ua-mobile': "?0",
                    'sec-ch-ua-platform': "\"macOS\"",
                    'sec-fetch-dest': "document",
                    'sec-fetch-mode': "navigate",
                    'sec-fetch-site': "none",
                    'sec-fetch-user': "?1",
                    'upgrade-insecure-requests': "1"
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

        # raise signals.Retry


if __name__ == '__main__':
    spider = MySpider(
        proxy="http://127.0.0.1:9000"
    )
    spider.run()
