import asyncio
import collections
import concurrent.futures
import time
from typing import Type, Union

from loguru import logger

from bricks.core import dispatch, signals
from bricks.core.events import REGISTERED_EVENTS
from bricks.lib.items import Items
from bricks.lib.queues import Item, LocalQueue, TaskQueue
from bricks.spider.air import Context, Spider

_futures = collections.defaultdict(concurrent.futures.Future)


def task_done(future_id: str, ctx: Context):
    """
    接收结果

    :param future_id:
    :param ctx:
    :return:
    """
    try:
        # ptr = ctypes.cast(future_id, ctypes.py_object)
        future: Union[asyncio.Future, concurrent.futures.Future] = _futures.pop(
            future_id, None
        )
        future and future.set_result(ctx)

    except Exception as e:
        logger.warning(f"future id 不存在, 放弃存储: {future_id}, error: {e}")


class Listener:
    def __init__(self, spider: Spider):
        self.spider: Spider = spider
        self.running: bool = False

    def run(self):
        self.spider.dispatcher.start()
        self.spider.forever = True
        self.spider.active(
            dispatch.Task(func=self.spider.run, kwargs={"task_name": "spider"})
        )
        self.running = True
        return self

    def stop(self):
        self.spider.forever = False
        self.running = False

    def execute(self, seeds: Union[dict, Item], timeout: int = None) -> Context:
        """
        给 listener 一个种子, 然后获取种子的消耗结果
        种子内特殊键值对说明:
        $futureID 当前任务 ID, 自动生成

        :param seeds: 需要消耗的种子
        :param timeout: 超时时间, 超时后还没有获取到结果则退出, 如果为 None 则表示必须等待一个结果才会结束
        :return:
        """
        future = concurrent.futures.Future()
        future_id = f"future-{id(future)}"
        seeds.update({"$futureID": future_id})
        _futures[future_id] = future
        self.spider.put_seeds(seeds)
        return future.result(timeout=timeout)

    @classmethod
    def wrap(
        cls,
        spider: Type[Spider],
        attrs: dict = None,
        modded: dict = None,
        ctx_modded: dict = None,
    ):
        """
        将 Spider 转化为 Listener, 等于后台运行爬虫, 支持动态添加种子, 然后获取该种子消耗的结果
        实现类似将爬虫转化为 API 的效果, 避免维护多份代码

        :param spider: 爬虫类
        :param modded: 魔改 class
        :param attrs: 初始化参数
        :param ctx_modded: context 魔改
        :return:
        """
        attrs = attrs or {}
        modded = modded or {}
        ctx_modded = ctx_modded or {}

        def mock_success(self, shutdown=False):
            future_id = self.seeds.get("$futureID")
            task_done(future_id, self)
            return super(self.__class__, self).success(shutdown)

        key = f"{spider.__module__}.{spider.__name__}"
        spider = type("Listen", (spider,), modded)
        REGISTERED_EVENTS.lazy_loading[f"{spider.__module__}.{spider.__name__}"] = (
            REGISTERED_EVENTS.lazy_loading[key].copy()
        )

        spider.Context = type(
            "ListenContext", (spider.Context,), {"success": mock_success, **ctx_modded}
        )
        ins: Spider = spider(**attrs)
        default_attrs = {
            "forever": True,
        }
        for k, v in default_attrs.items():
            ins.set(k, v)
        ins.disable_statistics()

        return cls(ins)


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

    def execute(self, seeds: Union[dict, Item]) -> Context:
        """
        给 listener 一个种子, 然后获取种子的消耗结果
        种子内特殊键值对说明:

        $futureType 表示需要的类型, 可以自己设置, 默认为 $response
            $request -> 表示只需要 request, 也就是消耗到了请求之前就会告知结果
            $response -> 表示只要 response, 也就是消耗到了解析之前就会告知结果
            $items -> 表示只要 items, 也就是消耗到了存储之前就会告知结果


        :param seeds: 需要消耗的种子
        :return:
        """
        seeds.update({"$spiderStart": time.time()})
        task_queue: TaskQueue = (
            self.spider.get("spider.task_queue") or self.spider.task_queue
        )
        queue_name: str = self.spider.get("spider.queue_name") or self.spider.queue_name
        context: Context = self.spider.make_context(
            task_queue=task_queue, queue_name=queue_name, seeds=seeds
        )
        context.flow({"next": self.spider.on_consume, "seeds": seeds})
        self.spider.on_consume(context=context)
        context.seeds.update({"$spiderFinish": time.time()})
        return context

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

        def mock_failure(self: Context, shutdown=False):
            future_max_retry = self.seeds.get("$futureMaxRetry")
            future_retry = self.seeds.get("$futureRetry") or 0
            if future_retry >= future_max_retry:
                self.seeds.update(
                    {"$msg": f"超出最大重试次数: {future_max_retry} 次", "$status": -1}
                )
                return self.success(shutdown)
            else:
                return super(self.__class__, self).retry()

        def mock_error(self, e: Exception, shutdown=True):
            if shutdown:
                self.seeds.update(
                    {
                        "$msg": f"请求遇到意料之外的异常: <{e.__class__.__name__} {e}>",
                        "$status": -1,
                    }
                )
                return self.success(shutdown)
            else:
                return super(self.__class__, self).error(shutdown)

        def mock_on_request(self, context: Context):
            future_type = context.seeds.get("$futureType", "$response")
            if future_type == "$request":
                raise signals.Success
            return super(self.__class__, self).on_request(context)

        def mock_on_retry(self, context: Context):
            try:
                return super(self.__class__, self).on_retry(context)
            except signals.Success:
                context.seeds.update({"$msg": "超出最大重试次数", "$status": -1})
                raise

        def mock_on_response(self, context: Context):
            future_type = context.seeds.get("$futureType", "$response")

            if future_type == "$response":
                raise signals.Success

            items: Items = super(self.__class__, self).on_response(context)

            if future_type == "$items":
                raise signals.Success

            return items

        modded.setdefault("on_request", mock_on_request)
        modded.setdefault("on_response", mock_on_response)
        modded.setdefault("on_retry", mock_on_retry)
        local = LocalQueue()
        key = f"{spider.__module__}.{spider.__name__}"
        spider = type("RPC", (spider,), modded)
        REGISTERED_EVENTS.lazy_loading[f"{spider.__module__}.{spider.__name__}"] = (
            REGISTERED_EVENTS.lazy_loading[key].copy()
        )

        spider.Context = type(
            "RPCContext",
            (spider.Context,),
            {"failure": mock_failure, "error": mock_error, **ctx_modded},
        )
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
