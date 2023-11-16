# -*- coding: utf-8 -*-
# @Time    : 2023-11-15 18:17
# @Author  : Kem
# @Desc    :
import time

from bricks import Request
from bricks.spider import air
from bricks.spider.air import Context


class MySpider(air.Spider):

    def make_seeds(self, context: Context, **kwargs):
        for i in range(10):
            yield {"id": i}

    def make_request(self, context: air.Context) -> Request:
        return Request(
            url=f"https://www.baidu.com/s?wd={context.seeds['id']}"
        )

    def parse(self, context: Context):
        print(context.response)
        # time.sleep(5)
        yield context.seeds

    def item_pipline(self, context: Context):
        super().item_pipline(context)
        context.background({
            "next": lambda name: (time.sleep(1), print(name)),
            "kwargs": context.seeds
        })
        context.success()
        # if context.seeds['id'] < 10:
        #     # 提交新请求
        #     context.submit(
        #         {**context.seeds, "id": context.seeds['id'] + 10},
        #         # call_later=True
        #     )


if __name__ == '__main__':
    spider = MySpider(concurrency=2)
    spider.run_init()
    spider.run_spider()
