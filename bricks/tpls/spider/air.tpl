from loguru import logger

from bricks import Request, const
from bricks.core import events, signals
from bricks.spider import air
from bricks.spider.air import Context


class {SPIDER}(air.Spider):
    def make_seeds(self, context: Context, **kwargs):
        # 修改为你自己的初始化逻辑
        yield {{"page": 1}}

    def make_request(self, context: Context) -> Request:
        seeds = context.seeds
        return Request(
            url={URL!r},
            method={METHOD!r},
            params={PARAMS!r},
            body={BODY!r},
            headers={HEADERS!r},
            cookies={COOKIES!r},
            options={OPTIONS!r},
            allow_redirects={ALLOW_REDIRECTS!r},
            proxies={PROXIES!r},
            proxy={PROXY!r},
            max_retry={MAX_RETRY!r},
            use_session={USE_SESSION!r},
        )

    def parse(self, context: Context):
        response = context.response
        # 编写你的解析逻辑, 返回字典或列表
        return {{
            "status_code": response.status_code,
            "url": response.url,
        }}

    def item_pipeline(self, context: Context):
        items = context.items
        # 编写你的存储逻辑
        logger.debug(f'存储: {{items}}')
        # 确认种子爬取完毕后删除
        context.success()

    @staticmethod
    @events.on(const.AFTER_REQUEST)
    def is_success(context: Context):
        if not context.response.ok:
            raise signals.Retry


if __name__ == '__main__':
    spider = {SPIDER}()
    spider.run()
