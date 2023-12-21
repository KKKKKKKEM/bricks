import time

from bricks import const
from bricks.plugins import scripts
from bricks.spider import template
from bricks.spider.template import Config


class Spider(template.Spider):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @property
    def config(self) -> Config:
        return Config(
            init=[
                template.Init(
                    func=lambda: {"page": 1},
                    layout=template.Layout(
                        factory={
                            "time": lambda: time.time()
                        }
                    ),

                )
            ],
            events={
                const.AFTER_REQUEST: [
                    # context.signpost 为 0 的时候 -> 判断请求是否成功
                    template.Task(
                        match="context.signpost == 0",
                        func=scripts.is_success,
                        kwargs={
                            "match": [
                                "context.response.get('code') == 0"
                            ]
                        }
                    ),
                ],
                const.BEFORE_PIPELINE: [
                    # context.signpost 为 0 的时候 -> 请求第二个配置
                    template.Task(func=lambda context: context.next_step(), match="context.signpost == 0"),
                    # context.signpost 为 0 的时候 -> 进行翻页
                    template.Task(
                        match="context.signpost == 0",
                        func=scripts.turn_page,
                        kwargs={
                            "match": [
                                "context.response.get('data.hasNextPage') == 1"
                            ],
                        }
                    ),
                ],

            },
            download=[
                # 第一个下载配置
                template.Download(
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
                # 第二个下载配置
                template.Download(
                    url="https://www.baidu.com/sugrec?pre=1&p=3&ie=utf-8&json=1&prod=pc&from=pc_web&wd=1&req=2&csor=1&_=1703142848459",
                    headers={
                        "User-Agent": "@chrome",
                        "Content-Type": "application/json;charset=UTF-8",
                    },
                ),
            ],
            parse=[
                # 第一个解析配置
                template.Parse(
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
                    },
                    layout=template.Layout(
                        rename={"userId": "user_id"},
                        default={"modify_at": time.time(), "page": "{page}", "seeds_time": "{time}"}
                    )
                ),
                # 第二个解析配置
                template.Parse(
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
            ],
            pipeline=[
                template.Pipeline(
                    func=lambda context: print(context.items),
                    success=True
                )
            ]

        )


if __name__ == '__main__':
    spider = Spider()
    spider.run()
