import time

from loguru import logger

from bricks import const
from bricks.client.runner import RpcProxy
from bricks.core import events, signals
from bricks.spider import air


class MySpider(air.Spider):
    def make_seeds(self, context: air.Context, **kwargs):
        # 模拟很耗时的爬虫, 没五秒爬一次
        while True:
            yield {"page": 1}
            time.sleep(5)

    def make_request(self, context: air.Context) -> air.Request:
        # 之前定义的种子会被投放至任务队列, 之后会被取出来, 迁入至 context 对象内
        return air.Request(
            url="https://fx1.service.kugou.com/mfanxing-home/h5/cdn/room/index/list_v2",
            params={"page": context.seeds["page"], "cid": 6000},
            headers={
                "User-Agent": "Mozilla/5.0 (Linux; Android 10; Redmi K30 Pro) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Mobile Safari/537.36",
                "Content-Type": "application/json;charset=UTF-8",
            },
        )

    def parse(self, context: air.Context):
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

    def item_pipeline(self, context: air.Context):
        items = context.items
        # 写自己的存储逻辑
        logger.debug(f"存储: {len(items)}")
        # 确认种子爬取完毕后删除, 不删除的话后面又会爬取
        context.success()

    @staticmethod
    @events.on(const.AFTER_REQUEST)
    def is_success(context: air.Context):
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

    def get_info(self):
        return {
            "name": "MySpider",
            "version": "1.0.0",
            "concurrency": self.concurrency,
            "number_of_seeds_obtained": self.number_of_seeds_obtained.value,
            "number_of_new_seeds": self.number_of_new_seeds.value,
            "number_of_total_requests": self.number_of_total_requests.value,
            "number_of_failure_requests": self.number_of_failure_requests.value,
        }



"""
启动命令: python manager.py run_task -m example.cmd.rpc_spider.run_spider -rpc mode=http ident=8080

模式选择和那边的 rpc 是一样的: http / websocket / socket / grpc / redis

调用:
通过 rpc 来获取当前的爬取情况
curl -X POST 'http://localhost:56557/rpc' -H 'Content-Type: application/json' -H 'Cookie: BDSVRTM=4; BD_HOME=1' -d '{
  "method":"get_info"
}'

method: 函数名, 自带的方法: stop -> 关闭; reload -> 重启


"""

def run_spider(rpc: RpcProxy = None, **kwargs):
    logger.debug(f'爬虫要启动了, 参数是: {kwargs}')

    spider = MySpider(**kwargs)
    if rpc:
        rpc.bind(spider)
    spider.run()



