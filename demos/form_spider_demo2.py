from loguru import logger

from bricks.lib.queues import RedisQueue
from bricks.plugins import scripts
from bricks.spider import form


class MySpider(form.Spider):

    @property
    def config(self) -> form.Config:
        return form.Config(
            init=[
                form.Init(func=lambda: {"page": 1})
            ],
            spider=[
                form.Download(
                    url="https://www.baidu.com",
                    headers={
                        "User-Agent": "Mozilla/5.0 (Linux; Android 10; Redmi K30 Pro) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Mobile Safari/537.36",
                    },
                ),
                form.Task(
                    func=lambda: print(1),
                ),
                form.Download(
                    url="https://sp1.baidu.com/5LMDcjW6BwF3otqbppnN2DJv/finance.pae.baidu.com/selfselect/getlatestprice",
                    params={
                        "stock": '[{"code":"BIDU","market":"us","type":"stock"}]',
                    },
                    headers={
                        "User-Agent": "Mozilla/5.0 (Linux; Android 10; Redmi K30 Pro) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Mobile Safari/537.36",
                        "Content-Type": "application/json;charset=UTF-8",
                    },
                    archive=True
                ),
                form.Task(
                    func=lambda: print(2),
                ),
                form.Task(
                    func=scripts.is_success,
                    kwargs={
                        "match": [
                            "1==2"
                        ]
                    }
                ),
                form.Parse(
                    func="json",
                    kwargs={
                        "rules": {
                            "Result.stock": {
                                "type": "type",
                                "market": "market",
                                "code": "code",
                                "sort_index": "sort_index",
                                "amount": "amount",
                                "price": "price",
                            }
                        }
                    }
                ),

                form.Pipeline(
                    func=lambda context: logger.debug(f'context.items: {context.items}'),
                    success=True
                )
            ],
            events={
                # const.AFTER_GET_SEEDS: [
                #     form.Task(
                #         func=lambda context: logger.debug(f'[AFTER_GET_SEEDS] :{context.seeds}'),
                #     )
                # ],
                # const.BEFORE_GET_SEEDS: [
                #     form.Task(
                #         func=lambda context: logger.debug(f'[BEFORE_GET_SEEDS] :{context.seeds}'),
                #     )
                # ],
                # const.AFTER_PUT_SEEDS: [
                #     form.Task(
                #         func=lambda context: logger.debug(f'[AFTER_PUT_SEEDS] :{context.seeds}'),
                #     )
                # ],
                # const.BEFORE_PUT_SEEDS: [
                #     form.Task(
                #         func=lambda context: logger.debug(f'[BEFORE_PUT_SEEDS] :{context.seeds}'),
                #     )
                # ],
            }
        )


if __name__ == '__main__':
    spider = MySpider(
        # downloader=playwright_.Downloader(mode='api', reuse=True, headless=True),
        concurrency=1,
        task_queue=RedisQueue()
    )
    # 使用调度器运行
    # spider.launch({"form": "interval", "exprs": "seconds=1"})
    # # 单次运行
    spider.run()
    # # survey 运行 -> 可以获取到执行的 Context
    # spider.survey({"page": 5})
