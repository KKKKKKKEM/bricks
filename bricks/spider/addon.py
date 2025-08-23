import asyncio
import collections
import concurrent.futures
import json
import uuid
from typing import Type, Union, Callable, Optional, List

from loguru import logger

from bricks.core import signals, dispatch
from bricks.core.events import REGISTERED_EVENTS
from bricks.lib.items import Items
from bricks.lib.queues import Item, LocalQueue, TaskQueue
from bricks.rpc.common import serve_async, MODE
from bricks.spider.air import Context, Spider


def ctx2json(ctx: Context):
    seeds = ctx.seeds
    future_type = seeds.get("$futureType", "$response")
    if future_type == "$request":
        return json.dumps({"data": ctx.request.curl, "type": future_type, "seeds": seeds}, default=str)
    elif future_type == "$response":
        return json.dumps({"data": ctx.response.text, "type": future_type, "seeds": seeds}, default=str)
    elif future_type == "$items":
        return json.dumps({"data": list(ctx.items), "type": future_type, "seeds": seeds}, default=str)
    else:
        return json.dumps({"data": "", "type": future_type, "seeds": seeds}, default=str)


class Mocker:

    def __init__(self, on_finish: Callable[[Context, Optional[Exception]], None]):
        self.on_finish = on_finish

    def create_hooks(self):
        def failure(ctx: Context, shutdown=False):
            future_max_retry = ctx.seeds.get("$futureMaxRetry")
            future_retry = ctx.seeds.get("$futureRetry") or 0
            if future_retry >= future_max_retry:
                self.on_finish(ctx, RuntimeError(f"超出最大重试次数: {future_max_retry} 次"))
                return ctx.success(shutdown)
            else:
                return super(ctx.__class__, ctx).retry()

        def error(ctx: Context, e: Exception, shutdown=True):
            if shutdown:
                self.on_finish(ctx, e)
                return ctx.success(shutdown)
            else:
                return super(ctx.__class__, ctx).error(e, shutdown)

        def success(ctx: Context, shutdown=False):
            self.on_finish(ctx, None)
            return super(ctx.__class__, ctx).success(shutdown)

        def on_request(spider, context: Context):

            future_type = context.seeds.get("$futureType", "$response")
            if future_type == "$request":
                raise signals.Success
            return super(spider.__class__, spider).on_request(context)

        def on_retry(spider, context: Context):
            try:
                return super(spider.__class__, spider).on_retry(context)
            except signals.Success:
                self.on_finish(context, RuntimeError("超出最大重试次数"))
                raise

        def on_response(spider, context: Context):
            future_type = context.seeds.get("$futureType", "$response")

            if future_type == "$response":
                raise signals.Success

            items: Items = super(spider.__class__, spider).on_response(context)

            if future_type == "$items":
                raise signals.Success

            return items

        return {
            "failure": failure,
            "error": error,
            "success": success,
            "on_request": on_request,
            "on_response": on_response,
            "on_retry": on_retry,
        }


class Rpc:
    def __init__(self):
        self.spider: Spider = ...
        self.running: bool = False
        self._futures = collections.defaultdict(concurrent.futures.Future)
        self._on_finish: List[Callable[[Context, Optional[Exception]], None]] = []

    def run(self):
        spider_name = self.spider.__class__.__name__
        queue_name = self.spider.queue_name
        queue_type = self.spider.task_queue.__class__.__name__
        logger.info(f'[开始监听] {spider_name}, queueName: {queue_name}, queueType: {queue_type}')
        self.spider.run_spider()

    def stop(self):
        self.running = False

    def execute(self, seeds: Union[dict, Item]) -> Context:
        """
        给 rpc 一个种子, 然后获取种子的消耗结果
        种子内特殊键值对说明:

        $futureType 表示需要的类型, 可以自己设置, 默认为 $response
            $request -> 表示只需要 request, 也就是消耗到了请求之前就会告知结果
            $response -> 表示只要 response, 也就是消耗到了解析之前就会告知结果
            $items -> 表示只要 items, 也就是消耗到了存储之前就会告知结果


        :param seeds: 需要消耗的种子
        :return:
        """
        future_id = f"future-{uuid.uuid4()}"
        future = self._futures[future_id]
        seeds.update({"$futureID": future_id})
        task_queue: TaskQueue = self.spider.get("spider.task_queue") or self.spider.task_queue
        queue_name: str = self.spider.get("spider.queue_name") or self.spider.queue_name
        context: Context = self.spider.make_context(task_queue=task_queue, queue_name=queue_name, seeds=seeds)  # noqa
        context.flow({"next": self.spider.on_consume, "seeds": seeds})
        self.spider.on_consume(context=context)
        return future.result()

    def submit(self, seeds: Union[dict, Item]):
        """
        给 rpc 一个种子, 然后获取种子的消耗结果
        种子内特殊键值对说明:

        $futureType 表示需要的类型, 可以自己设置, 默认为 $response
            $request -> 表示只需要 request, 也就是消耗到了请求之前就会告知结果
            $response -> 表示只要 response, 也就是消耗到了解析之前就会告知结果
            $items -> 表示只要 items, 也就是消耗到了存储之前就会告知结果


        :param seeds: 需要消耗的种子
        :return:
        """
        self.spider.put_seeds(seeds)

    @classmethod
    def wrap(
            cls,
            spider: Type[Spider],
            attrs: dict = None,
            modded: dict = None,
            ctx_modded: dict = None,
            mocker: Mocker = None,
            ensure_local: bool = True
    ):
        """
        将 Spider 转化为 RPC, 等于后台运行爬虫, 支持动态添加种子, 然后获取该种子消耗的结果
        实现类似将爬虫转化为 API 的效果, 避免维护多份代码

        但是, 部分事件会失效:
        1. 如果只要 response, 那么生效的事件只有: before request, after request
        2. 如果只要 request, 那么生效的事件只有: before request
        3. 如果只要 items, 那么生效的事件只有: before request, after request

        也就是说: 如果你有翻页事件, 且在 pipeline, 则不会生效 (但是我们一般都不需要这个, 建议自己注释掉)

        :param ensure_local: 确保使用本地队列
        :param mocker: 默认的 mocker
        :param spider: 爬虫类
        :param modded: 魔改 class
        :param attrs: 初始化参数
        :param ctx_modded: context 魔改
        :return:
        """
        attrs = attrs or {}
        attrs.update(concurrency=1)
        modded = modded or {}
        ctx_modded = ctx_modded or {}
        rpc = cls()

        mocker = mocker or Mocker(rpc.on_finish)
        hooks = mocker.create_hooks()

        modded.setdefault("on_request", hooks["on_request"])
        modded.setdefault("on_response", hooks["on_response"])
        modded.setdefault("on_retry", hooks["on_retry"])
        ctx_modded.setdefault("failure", hooks["failure"])
        ctx_modded.setdefault("error", hooks["error"])
        ctx_modded.setdefault("success", hooks["success"])
        ctx_modded.setdefault("to_json", ctx2json)

        local = LocalQueue()
        spider = type("RPC", (spider,), modded)

        spider.Context = type("RPCContext", (spider.Context,), ctx_modded)
        rpc.spider = spider(**attrs)
        default_attrs = {"forever": True}

        if ensure_local:
            default_attrs.update(**{
                "task_queue": local,
                "spider.task_queue": local,
                "queue_name": f"{cls.__module__}.{cls.__name__}:rpc"
            })

        for k, v in default_attrs.items():
            rpc.spider.set(k, v)
        rpc.spider.disable_statistics()
        return rpc

    def on_finish(self, ctx: Context, error: Optional[Exception]):
        """
        接收结果

        :param error:
        :param ctx:
        :return:
        """
        future_id: str = ctx.seeds.get("$futureID")
        try:
            future: concurrent.futures.Future = self._futures.pop(future_id, None)
            if error:
                future and future.set_exception(error)
            else:
                future and future.set_result(ctx)
        except concurrent.futures.InvalidStateError:
            pass

        except Exception as e:
            logger.warning(f"[error] 放弃存储: {future_id}, error: {e}")

        for on_finish in self._on_finish:
            on_finish(ctx, error)

    def with_callback(self, *on_finish: Callable[[Context, Optional[Exception]], None]):
        self._on_finish.extend(on_finish)
        return self

    def serve(
            self,
            concurrency: int = 10,
            ident: any = 0,
            on_server_started: Callable[[int], None] = None,
            mode: MODE = "http",
            **kwargs
    ):
        """
        启动 RPC 服务器

        :param mode: rpc 模式: http, websocket, socket, grpc
        :param concurrency: 并发数
        :param ident: 监听端口 / 标识(redis)，默认随机
        :param on_server_started: 当服务启动完后的回调
        """
        self.spider.dispatcher = dispatch.Dispatcher(max_workers=concurrency)
        coro = serve_async(self, mode=mode, concurrency=concurrency, ident=ident, on_server_started=on_server_started, **kwargs)
        try:
            with self.spider.dispatcher:
                asyncio.run_coroutine_threadsafe(coro, loop=self.spider.dispatcher.loop)
                self.run()
        except (KeyboardInterrupt, SystemExit):
            return
