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
                    url="https://fx1.service.kugou.com/mfanxing-home/h5/cdn/room/index/list_v2",
                    params={
                        "page": "{page}",
                        "cid": 6000
                    },
                    headers={
                        "User-Agent": "@chrome",
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
                    }
                ),

                form.Pipeline(
                    func=lambda context: print(context.items),
                    success=True
                ),
                # 第二个下载配置
                form.Download(
                    url="https://www.baidu.com/sugrec?pre=1&p=3&ie=utf-8&json=1&prod=pc&from=pc_web&wd=1&req=2&csor=1&_=1703142848459",
                    headers={
                        "User-Agent": "@chrome",
                        "Content-Type": "application/json;charset=UTF-8",
                    },
                ),
                form.Parse(
                    func="json",
                    kwargs={
                        "rules": {
                            "g": {
                                "type": "type",
                                "sa": "sa",
                                "q": "q",
                            }
                        }
                    }
                ),

                form.Pipeline(
                    func=lambda context: print(context.items),
                    success=True
                )
            ]
        )


if __name__ == '__main__':
    spider = MySpider()
    spider.run()
