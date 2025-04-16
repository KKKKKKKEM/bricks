import collections
import concurrent.futures
from typing import Type, Union

from loguru import logger

from bricks.core import signals
from bricks.core.events import REGISTERED_EVENTS
from bricks.lib.items import Items
from bricks.lib.queues import Item, LocalQueue, TaskQueue
from bricks.spider.air import Context, Spider

_futures = collections.defaultdict(concurrent.futures.Future)


def report(future_id: str, retval: any, form: str = "result"):
    """
    接收结果

    :param form:
    :param future_id:
    :param retval:
    :return:
    """
    try:
        future: concurrent.futures.Future = _futures.pop(future_id, None)
        if form == "result":
            future and future.set_result(retval)
        else:
            future and future.set_exception(retval)
    except concurrent.futures.InvalidStateError:
        pass

    except Exception as e:
        logger.warning(f"[error] 放弃存储: {future_id}, error: {e}")


class Mocker:
    @staticmethod
    def failure(ctx: Context, shutdown=False):
        future_max_retry = ctx.seeds.get("$futureMaxRetry")
        future_retry = ctx.seeds.get("$futureRetry") or 0
        future_id = ctx.seeds.get("$futureID")
        if future_retry >= future_max_retry:
            report(future_id, RuntimeError(f"超出最大重试次数: {future_max_retry} 次"), form="error")
            return ctx.success(shutdown)
        else:
            return super(ctx.__class__, ctx).retry()

    @staticmethod
    def error(ctx: Context, e: Exception, shutdown=True):
        if shutdown:
            future_id = ctx.seeds.get("$futureID")
            report(future_id, e, form="error")
            return ctx.success(shutdown)
        else:
            return super(ctx.__class__, ctx).error(e, shutdown)

    @staticmethod
    def success(ctx: Context, shutdown=False):
        future_id = ctx.seeds.get("$futureID")
        report(future_id, ctx)
        return super(ctx.__class__, ctx).success(shutdown)

    @staticmethod
    def on_request(self, context: Context):

        future_type = context.seeds.get("$futureType", "$response")
        if future_type == "$request":
            raise signals.Success
        return super(self.__class__, self).on_request(context)

    @staticmethod
    def on_retry(self, context: Context):
        try:
            return super(self.__class__, self).on_retry(context)
        except signals.Success:
            future_id = context.seeds.get("$futureID")
            report(future_id, RuntimeError("超出最大重试次数"), form="error")
            raise

    @staticmethod
    def on_response(self, context: Context):
        future_type = context.seeds.get("$futureType", "$response")

        if future_type == "$response":
            raise signals.Success

        items: Items = super(self.__class__, self).on_response(context)

        if future_type == "$items":
            raise signals.Success

        return items


class Rpc:
    def __init__(self, spider: Spider):
        self.spider: Spider = spider
        self.running: bool = False

    def run(self):
        self.spider.dispatcher.start()
        self.running = True
        return self

    def stop(self):
        self.running = False

    def execute(self, seeds: Union[dict, Item], timeout: int = None) -> Context:
        """
        给 rpc 一个种子, 然后获取种子的消耗结果
        种子内特殊键值对说明:

        $futureType 表示需要的类型, 可以自己设置, 默认为 $response
            $request -> 表示只需要 request, 也就是消耗到了请求之前就会告知结果
            $response -> 表示只要 response, 也就是消耗到了解析之前就会告知结果
            $items -> 表示只要 items, 也就是消耗到了存储之前就会告知结果


        :param timeout:
        :param seeds: 需要消耗的种子
        :return:
        """
        future = concurrent.futures.Future()
        future_id = f"future-{id(future)}"
        seeds.update({"$futureID": future_id})
        _futures[future_id] = future
        task_queue: TaskQueue = self.spider.get("spider.task_queue") or self.spider.task_queue
        queue_name: str = self.spider.get("spider.queue_name") or self.spider.queue_name
        context: Context = self.spider.make_context(task_queue=task_queue, queue_name=queue_name, seeds=seeds)  # noqa
        context.flow({"next": self.spider.on_consume, "seeds": seeds})
        self.spider.on_consume(context=context)
        return future.result(timeout)

    @classmethod
    def wrap(
            cls,
            spider: Type[Spider],
            attrs: dict = None,
            modded: dict = None,
            ctx_modded: dict = None,
    ):
        """
        将 Spider 转化为 RPC, 等于后台运行爬虫, 支持动态添加种子, 然后获取该种子消耗的结果
        实现类似将爬虫转化为 API 的效果, 避免维护多份代码

        但是, 部分事件会失效:
        1. 如果只要 response, 那么生效的事件只有: before request, after request
        2. 如果只要 request, 那么生效的事件只有: before request
        3. 如果只要 items, 那么生效的事件只有: before request, after request

        也就是说: 如果你有翻页事件, 且在 pipeline, 则不会生效 (但是我们一般都不需要这个, 建议自己注释掉)

        :param spider: 爬虫类
        :param modded: 魔改 class
        :param attrs: 初始化参数
        :param ctx_modded: context 魔改
        :return:
        """
        attrs = attrs or {}
        modded = modded or {}
        ctx_modded = ctx_modded or {}

        modded.setdefault("on_request", Mocker.on_request)
        modded.setdefault("on_response", Mocker.on_response)
        modded.setdefault("on_retry", Mocker.on_retry)
        ctx_modded.setdefault("failure", Mocker.failure)
        ctx_modded.setdefault("error", Mocker.error)
        ctx_modded.setdefault("success", Mocker.success)

        local = LocalQueue()
        key = f"{spider.__module__}.{spider.__name__}"
        spider = type("RPC", (spider,), modded)
        REGISTERED_EVENTS.lazy_loading[f"{spider.__module__}.{spider.__name__}"] = (
            REGISTERED_EVENTS.lazy_loading[key].copy()
        )

        spider.Context = type("RPCContext", (spider.Context,), ctx_modded)
        ins: Spider = spider(**attrs)
        default_attrs = {
            "task_queue": local,
            "spider.task_queue": local,
            "queue_name": f"{cls.__module__}.{cls.__name__}:rpc",
            "forever": True,
        }
        for k, v in default_attrs.items():
            ins.set(k, v)
        ins.disable_statistics()
        return cls(ins)
