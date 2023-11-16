# -*- coding: utf-8 -*-
# @Time    : 2023-11-15 18:17
# @Author  : Kem
# @Desc    :
from bricks import Request, Response

from bricks.spider import air
from bricks.spider.air import Context


class MySpider(air.Spider):

    def make_seeds(self):
        for i in range(10):
            yield {"id": i}

    def make_request(self, context: air.Context) -> Request:
        return Request(
            url=f"https://www.baidu.com/s?wd={context.seeds['id']}"
        )

    def parse(self, response: Response):
        yield [{"name": 1}]
        yield [{"name": 2}]

    def item_pipline(self, context: Context):
        super().item_pipline(context)
        context.success()


if __name__ == '__main__':
    spider = MySpider()
    spider.run_init()
    spider.run_spider()
