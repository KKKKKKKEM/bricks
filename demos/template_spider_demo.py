# -*- coding: utf-8 -*-
# @Time    : 2023-12-05 20:29
# @Author  : Kem
# @Desc    :
from bricks import const
from bricks.core import signals
from bricks.plugins.make_seeds import by_csv
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
                    func=by_csv,
                    kwargs={
                        'path': r'D:\yintian\myProject\bricks\files\e.csv',
                    }
                )
            ],
            events={
                const.AFTER_REQUEST: [
                    template.Task(func=self.is_success),
                ],
                const.BEFORE_PIPELINE: [
                    template.Task(func=self.turn_page),
                ]
            },
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
                    },
                ),
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
                    }
                )
            ],
            pipeline=[
                template.Pipeline(
                    func=lambda context: print(context.items),
                    success=True
                )
            ]

        )

    @staticmethod
    def turn_page(context: template.Context):
        # 判断是否存在下一页
        has_next = context.response.get('data.hasNextPage')
        if has_next == 1:
            # 提交翻页的种子
            context.submit({**context.seeds, "page": context.seeds["page"] + 1})

    @staticmethod
    def is_success(context: template.Context):
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
    spider = Spider()
    spider.run(
        task_name='init'
    )
