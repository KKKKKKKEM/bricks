from bricks.core import signals
from bricks.db.mongo import Mongo
from bricks.db.sqllite import SqlLite
from bricks.spider import form

sqllite = SqlLite("test")
sqllite.create_table("user_info", structure={
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
                form.Init(func=lambda: {"page": 1})
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
                form.Task(func=self.is_success),
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
                form.Task(func=self.turn_page),
                # form.Pipeline(
                #     func="bricks.plugins.storage.to_sqllite",
                #     kwargs={
                #         "conn": sqllite,
                #         "path": "user_info"
                #     },
                #     success=True
                # )

                form.Pipeline(
                    func="bricks.plugins.storage.to_mongo",
                    kwargs={
                        "conn": mongo,
                        "path": "user_info",
                        "database": "live",
                        "row_keys": ['userId']
                    },
                    success=True
                )

            ]
        )

    @staticmethod
    def turn_page(context: form.Context):
        # 判断是否存在下一页
        has_next = context.response.get('data.hasNextPage')
        if has_next == 1:
            # 提交翻页的种子
            context.submit({**context.seeds, "page": context.seeds["page"] + 1})

    @staticmethod
    def is_success(context: form.Context):
        """
        判断相应是否成功

        :param context:
        :return:
        """
        # 不成功 -> 返回 False
        if context.response.get('code') != 0:
            # 重试信号
            raise signals.Retry


if __name__ == '__main__':
    spider = MySpider()
    spider.run()
    # print(list(sqllite.find("select * from user_info")))
