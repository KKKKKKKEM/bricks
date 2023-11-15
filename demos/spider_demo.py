# -*- coding: utf-8 -*-
# @Time    : 2023-11-15 18:17
# @Author  : Kem
# @Desc    :

from bricks.spider import air


class MySpider(air.Spider):

    def make_seeds(self):
        for i in range(100):
            yield {"id": i}


if __name__ == '__main__':
    spider = MySpider()
    spider.run_init()
