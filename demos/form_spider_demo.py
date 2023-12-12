from bricks.db.mongo import Mongo
from bricks.db.sqlite import Sqlite
from bricks.lib.queues import RedisQueue
from bricks.plugins import scripts
from bricks.spider import form

sqlite = Sqlite("test")
sqlite.create_table("user_info", structure={
    "userId": int,
    "roomId": int,
    "score": float,
    "startTime": float,
    "kugouId": int,
    "status": int,
})
mongo = Mongo()


class MySpider(form.Spider):

    @property
    def config(self) -> form.Config:
        return form.Config(
            init=[
                form.Init(func=lambda: ({"page": i} for i in range(100)))
            ],
            spider=[
                form.Download(
                    url="https://fx1.service.kugou.com/mfanxing-home/h5/cdn/room/index/list_v2",
                    params={
                        "page": "{page}",
                        "cid": 6000
                    },
                    headers={
                        "User-Agent": "Mozilla/5.0 (Linux; Android 10; Redmi K30 Pro) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Mobile Safari/537.36",
                        "Content-Type": "application/json;charset=UTF-8",
                    },
                ),
                form.Task(
                    func=scripts.is_success,
                    kwargs={
                        "match": [
                            "context.response.get('code') == 0"
                        ]
                    }
                ),
                form.Parse(
                    func="json",
                    kwargs={
                        "rules": {
                            "data.list": {
                                "userId": "userId",
                                "roomId": "roomId",
                                "score": "score",
                                "startTime": "startTime",
                                "kugouId": "kugouId",
                                "status": "status",
                            }
                        }
                    }
                ),
                form.Task(
                    func=scripts.turn_page,
                    kwargs={
                        "match": [
                            "context.response.get('data.hasNextPage') == 1"
                        ],
                        "call_later": True
                    }
                ),
                form.Task(
                    func=scripts.inject,
                    kwargs={
                        "flows": [
                            "context = Context.get_context()",
                            "logger.debug(context.seeds)"
                        ]
                    }
                ),
                # form.Pipeline(
                #     func="bricks.plugins.storage.to_sqlite",
                #     kwargs={
                #         "conn": sqlite,
                #         "path": "user_info"
                #     },
                #     success=True
                # )

                # form.Pipeline(
                #     func="bricks.plugins.storage.to_mongo",
                #     kwargs={
                #         "conn": mongo,
                #         "path": "user_info",
                #         "database": "live",
                #         "row_keys": ['userId']
                #     },
                #     success=True
                # )

                form.Pipeline(
                    func=lambda context: print(context.items),
                    success=True
                )
            ]
        )


if __name__ == '__main__':
    spider = MySpider(
        # task_queue=RedisQueue()
    )
    # 使用调度器运行
    # spider.launch({"form": "interval", "exprs": "seconds=1"})
    # # 单次运行
    spider.run()
    # # survey 运行 -> 可以获取到执行的 Context
    # spider.survey({"page": 5})
