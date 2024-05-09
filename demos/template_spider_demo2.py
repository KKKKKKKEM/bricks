import time

from bricks import const
from bricks.db.redis_ import Redis
from bricks.lib.queues import RedisQueue
from bricks.plugins import scripts
from bricks.plugins.make_seeds import by_redis
from bricks.spider import template
from bricks.spider.template import Config
from no_views.conn import redis_config


class Spider(template.Spider):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.redis = Redis(
            **redis_config
        )

    @property
    def config(self) -> Config:
        return Config(
            init=[
                template.Init(
                    func=by_redis,
                    kwargs={
                        # 需自行配置 redis
                        'conn': self.redis,
                        'path': '*|*',
                        'key_type': 'string',
                    },
                    layout=template.Layout(
                        factory={
                            "time": lambda: time.time()
                        }
                    )
                )
            ],
            download=[
                template.Download(
                    url="https://fx1.service.kugou.com/mfanxing-home/h5/cdn/room/index/list_v2",
                    params={
                        "page": "{page}",
                        "cid": 6000
                    },
                    headers={
                        "User-Agent": "@chrome",
                        "Content-Type": "application/json;charset=UTF-8",
                    }
                )
            ],
            parse=[
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
                )
            ],
            pipeline=[
                template.Pipeline(
                    func=lambda context: print(context.items),
                    success=True
                )
            ],
            events={
                const.AFTER_REQUEST: [
                    template.Task(
                        func=scripts.is_success,
                        kwargs={
                            "match": [
                                "context.response.get('code') == 0"
                            ]
                        }
                    )
                ],
                const.BEFORE_PIPELINE: [
                    template.Task(
                        func=scripts.turn_page,
                        kwargs={
                            "match": [
                                "context.response.get('data.hasNextPage') == 1"
                            ]
                        }
                    )
                ]
            }
        )


if __name__ == '__main__':
    spider = Spider(
        task_queue=RedisQueue(**redis_config),
    )
    spider.run(
        task_name='all',
    )
