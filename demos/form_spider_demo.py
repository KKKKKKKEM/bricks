# -*- coding: utf-8 -*-
# @Time    : 2023-11-18 14:16
# @Author  : Kem
# @Desc    :
from bricks import const
from bricks.spider import form


class MySpider(form.Spider):

    @property
    def config(self) -> form.Config:
        return form.Config(
            init=[
                form.Init(func=lambda: [{"id": i} for i in range(10)])
            ],
            spider=[
                form.Download(
                    url="https://www.baidu.com",
                    params={"kw": '{id}'}
                ),
                form.Parse(
                    func=lambda context: context.seeds
                ),
                form.Task(
                    func=lambda context: print(context.response),
                ),
                form.Pipeline(
                    func=lambda context: print(context.items),
                    success=True
                )
            ],
            events={
                const.BEFORE_REQUEST: [
                    form.Task(func=lambda: print('要开始请求啦!'))
                ]
            }
        )


class MySubSpidr(MySpider):

    @property
    def config(self) -> form.Config:
        config = super().config
        config.events[const.BEFORE_REQUEST].append(
            form.Task(func=lambda: print('子类要开始请求啦!'))
        )
        return config


if __name__ == '__main__':
    spider = MySubSpidr()
    spider.run()
