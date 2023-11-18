# -*- coding: utf-8 -*-
# @Time    : 2023-11-18 14:16
# @Author  : Kem
# @Desc    :


from bricks.spider import form


class MySpider(form.Spider):

    @property
    def config(self) -> form.Config:
        return form.Config(
            init=[
                form.Init(func=lambda: {"id": 1})
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
            ]
        )


if __name__ == '__main__':
    spider = MySpider()
    spider.run()
