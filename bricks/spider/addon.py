import asyncio
import math
from typing import Any, Callable, List, Optional, Type, Union

from loguru import logger

from bricks import state
from bricks.core import dispatch, signals
from bricks.lib.queues import Item, LocalQueue, TaskQueue
from bricks.rpc.common import MODE, start_rpc_server
from bricks.spider.air import Context, Spider


def ctx2json(ctx: Context):
    seeds = ctx.seeds
    future_type = seeds.get("$futureType", "$response")
    if future_type == "$request":
        data = ctx.request.curl if ctx.request else ""
    elif future_type == "$response":
        data = ctx.response.text if ctx.response else ""
    elif future_type == "$items":
        data = list(ctx.items or [])
    else:
        data = ""
    return {"data": data, "type": future_type, "seeds": dict(seeds)}


class Mocker:

    def __init__(self, on_finish: Callable[[Context, Optional[Exception]], None]):
        self.on_finish = on_finish

    def create_hooks(self):
        def get_future_type(ctx: Context) -> str:
            return ctx.seeds.get("$futureType", "$response")

        def before_request(ctx: Context):
            if get_future_type(ctx) == "$request":
                raise signals.Success

        def after_request(ctx: Context):
            if get_future_type(ctx) == "$response":
                raise signals.Success

        def before_pipeline(ctx: Context):
            if get_future_type(ctx) == "$items":
                raise signals.Success

        def failure(ctx: Context, shutdown=False):
            future_max_retry = ctx.seeds.get("$futureMaxRetry") or math.inf
            future_retry = ctx.seeds.get("$futureRetry") or ctx.request.retry
            if future_retry >= future_max_retry:
                self.on_task_done(
                    ctx, RuntimeError(f"超出最大重试次数: {future_max_retry} 次")
                )
                return ctx.success(shutdown)
            else:
                return super(ctx.__class__, ctx).retry()

        def error(ctx: Context, e: Exception, shutdown=True):
            if shutdown:
                self.on_task_done(ctx, e)
                return ctx.success(shutdown)
            else:
                return super(ctx.__class__, ctx).error(e, shutdown)

        def success(ctx: Context, shutdown=False):
            self.on_task_done(ctx, None)
            shutdown and ctx.flow({"next": None})  # type: ignore

        def submit(
            ctx: Context,
            *obj: Union[Item, dict],
            call_later=False,
            attrs: Optional[dict] = None,
        ):
            return super(ctx.__class__, ctx).submit(*obj, call_later=False, attrs=attrs)

        def put_seeds(spider: Spider, seeds: Union[dict, Item], *_, **kwargs):
            logger.debug("[RPC] put_seeds ignored outside active context")
            return 0

        return {
            "before_request": before_request,
            "after_request": after_request,
            "before_pipeline": before_pipeline,
            "failure": failure,
            "error": error,
            "success": success,
            "submit": submit,
            "put_seeds": put_seeds,
        }

    def on_task_done(self, context: Context, error: Optional[Exception]):
        if not hasattr(context, "$finish"):
            setattr(context, "$finish", True)
            setattr(context, "$error", error)
            context.doing.clear()
            self.on_finish(context, error)


class Rpc:
    def __init__(self):
        self.spider: Spider = ...
        self.running: bool = False
        self._dispatcher_owned: bool = False
        self._dispatcher_context_depth: int = 0
        self._on_finish: List[Callable[[Context, Optional[Exception]], None]] = []

    def _ensure_dispatcher(self):
        dispatcher = getattr(self.spider, "dispatcher", None)
        if not dispatcher or dispatcher.is_running():
            return

        dispatcher.start()
        self._dispatcher_owned = True

    def __enter__(self):
        if self._dispatcher_context_depth == 0:
            self._ensure_dispatcher()
        self._dispatcher_context_depth += 1
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._dispatcher_context_depth = max(0, self._dispatcher_context_depth - 1)
        if self._dispatcher_context_depth == 0 and self._dispatcher_owned:
            self.stop()

    def stop(self):
        self.running = False
        if getattr(self.spider, "dispatcher", None):
            self.spider.dispatcher.stop()
        self._dispatcher_owned = False
        self._dispatcher_context_depth = 0


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
        self._ensure_dispatcher()

        seeds = Item(seeds)
        task_queue: TaskQueue = (
            self.spider.get("spider.task_queue") or self.spider.task_queue
        )
        queue_name: str = self.spider.get("spider.queue_name") or self.spider.queue_name
        context: Context = self.spider.make_context(
            task_queue=task_queue, queue_name=queue_name, seeds=seeds
        )  # noqa
        context.flow({"next": self.spider.on_consume, "seeds": seeds})
        self.spider.on_consume(context=context)
        error = getattr(context, "$error", None)
        if error:
            raise error
        return context

    @classmethod
    def wrap(
        cls,
        spider: Type[Spider],
        attrs: dict = None,
        modded: dict = None,
        ctx_modded: dict = None,
        mocker: Mocker = None,
        ensure_local: bool = True,
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

        modded.setdefault("put_seeds", hooks["put_seeds"])
        ctx_modded.setdefault("submit", hooks["submit"])
        ctx_modded.setdefault("failure", hooks["failure"])
        ctx_modded.setdefault("error", hooks["error"])
        ctx_modded.setdefault("success", hooks["success"])
        ctx_modded.setdefault("on_json_dump", ctx2json)

        local = LocalQueue()
        spider = type("RPC", (spider,), modded)

        spider.Context = type("RPCContext", (spider.Context,), ctx_modded)
        rpc.spider = spider(**attrs)
        default_attrs = {"forever": True}

        if ensure_local:
            default_attrs.update(
                **{
                    "task_queue": local,
                    "spider.task_queue": local,
                    "queue_name": f"{cls.__module__}.{cls.__name__}:rpc",
                }
            )

        for k, v in default_attrs.items():
            rpc.spider.set(k, v)
        for form, name, index in (
            (state.const.BEFORE_REQUEST, "before_request", math.inf),
            (state.const.AFTER_REQUEST, "after_request", math.inf),
            (state.const.BEFORE_PIPELINE, "before_pipeline", -math.inf),
        ):
            hook = hooks.get(name)
            hook and rpc.spider.use(form, {"func": hook, "index": index})
        rpc.spider.disable_statistics()
        return rpc

    def on_finish(self, ctx: Context, error: Optional[Exception]):
        """
        接收结果

        :param error:
        :param ctx:
        :return:
        """
        for on_finish in self._on_finish:
            try:
                on_finish(ctx, error)
            except Exception as e:
                logger.warning(f"[error] 放弃回调: {ctx.seeds}, error: {e}")

    def with_callback(self, *on_finish: Callable[[Context, Optional[Exception]], None]):
        self._on_finish.extend(on_finish)
        return self

    def serve(
        self,
        concurrency: int = 10,
        ident: Any = 0,
        on_server_started: Callable[[Any], None] = None,
        mode: MODE = "http",
        **kwargs,
    ):
        """
        启动 RPC 服务器

        :param mode: rpc 模式: http, websocket, socket, grpc
        :param concurrency: 并发数
        :param ident: 监听端口 / 标识(redis)，默认随机
        :param on_server_started: 当服务启动完后的回调
        """
        self.spider.dispatcher = dispatch.Dispatcher(max_workers=concurrency)
        rpc_server = start_rpc_server(
            self,
            mode=mode,
            concurrency=concurrency,
            ident=ident,
            on_server_started=on_server_started,
            **kwargs,
        )
        try:
            with self.spider.dispatcher:
                self.running = True
                server = asyncio.run_coroutine_threadsafe(
                    rpc_server, loop=self.spider.dispatcher.loop
                )
                server.result()
        except (KeyboardInterrupt, SystemExit):
            return
        finally:
            self.running = False
