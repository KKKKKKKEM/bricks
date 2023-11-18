# -*- coding: utf-8 -*-
# @Time    : 2023-11-15 18:17
# @Author  : Kem
# @Desc    :
import time

from bricks import Request, const, plugins
from bricks.core import signals
from bricks.lib.queues import RedisQueue
from bricks.spider import air
from bricks.spider.air import Context


class MySpider(air.Spider):

    def before_start(self):
        # self.use(const.BEFORE_REQUEST, {"func": lambda context: print(context.request)})
        self.use(const.BEFORE_REQUEST, {"func": plugins.set_proxy})
        self.use(
            const.AFTER_REQUEST,
            {"func": plugins.show_response},
            {"func": plugins.is_success},
        )

        super().before_start()

    def before_request(self, context: Context):
        if "done" not in context.seeds:
            # 切换去做点别的事情
            context.flow({"next": self.do_something})
            raise signals.Switch
        if context.seeds['id'] == 5:
            time.sleep(10)
            raise signals.Failure

    @staticmethod
    def do_something(context: Context):
        context.seeds['done'] = 1
        # 回滚回去
        context.rollback()

    def make_seeds(self, context: Context, **kwargs):
        yield [{"id": i} for i in range(10)]
        # for i in range(10):
        #     yield {"id": i}

    def make_request(self, context: air.Context) -> Request:
        return Request(
            url=f"https://www.baidu.com/s?wd={context.seeds['id']}"
        )

    def parse(self, context: Context):
        # print(context.response)
        # time.sleep(5)
        yield context.seeds

    def item_pipeline(self, context: Context):
        super().item_pipeline(context)
        # context2 = context.background(
        #     {
        #         "next": lambda name: (time.sleep(1), print(name)),
        #         "kwargs": context.seeds
        #     },
        # )
        # ret = context2.future.result()
        # print(ret)
        context.success(shutdown=True)
        # if context.seeds['id'] < 10:
        #     # 提交新请求
        #     context.submit(
        #         {**context.seeds, "id": context.seeds['id'] + 10},
        #         # call_later=True
        #     )


if __name__ == '__main__':
    # redis = Redis()
    # redis.add("proxy", "127.0.0.1:7890")
    spider = MySpider(
        concurrency=1,
        task_queue=RedisQueue(),
        # proxy={
        #     "ref": "bricks.lib.proxies.CustomProxy",
        #     "key": "127.0.0.1:7890",
        #     "threshold": 5,
        # },
        # proxy={
        #     "ref": "bricks.lib.proxies.RedisProxy",
        #     "key": "proxy",
        #     "threshold": 5,  # 代理使用阈值, 用五次就归还
        #     # "recover": False  # 回收函数
        # },
    )
    spider.run(task_name="init")
