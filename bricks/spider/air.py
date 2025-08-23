# -*- coding: utf-8 -*-
# @Time    : 2023-11-15 14:09
# @Author  : Kem
# @Desc    :
import contextlib
import datetime
import functools
import inspect
import json
import math
import queue
import re
import time
import uuid
from typing import Callable, Iterable, List, Optional, Union

from loguru import logger

from bricks import state
from bricks.core import dispatch, events, signals
from bricks.core.context import Error, Flow
from bricks.core.events import REGISTERED_EVENTS, EventManager
from bricks.core.genesis import Pangu
from bricks.downloader import AbstractDownloader, cffi
from bricks.lib.counter import FastWriteCounter
from bricks.lib.items import Items
from bricks.lib.proxies import BaseProxy, manager
from bricks.lib.queues import Item, LocalQueue, TaskQueue
from bricks.lib.request import Request
from bricks.lib.response import Response
from bricks.plugins import on_request
from bricks.utils import pandora

IGNORE_RETRY_PATTERN = re.compile("ProxyError", re.IGNORECASE)
_fetchers_cache = {}


class Context(Flow):
    def __init__(
            self,
            target: "Spider",
            form: str = state.const.ON_CONSUME,
            division: bool = False,
            request: Request = None,
            response: Response = None,
            seeds: Item = None,
            items: Items = None,
            task_queue: TaskQueue = None,
            **kwargs,
    ) -> None:
        self.request: Request = request
        self.response: Response = response
        self.seeds: Item = seeds
        self.items: Items = items
        self.task_queue: TaskQueue = task_queue
        self.queue_name: str = kwargs.pop(
            "queue_name", f"{self.__class__.__module__}.{self.__class__.__name__}"
        )
        super().__init__(form, target, **kwargs)
        self.target: Spider = target
        self.division: bool = division

    def __setattr__(self, key, value):
        if key == "seeds" and type(value) is not Item:
            if isinstance(value, list):
                value = [Item(i) for i in value]
            else:
                value = Item(value)

        elif key == "items" and type(value) is not Items:
            value = Items(value)

        super().__setattr__(key, value)

    def success(self, shutdown=False):
        ret = self.task_queue.remove(self.queue_name, *pandora.iterable(self.seeds))
        shutdown and self.flow({"next": None})  # type: ignore
        return ret

    def retry(self):
        self.flow({"next": self.target.on_retry})

    def failure(self, shutdown=False):
        if self.seeds:
            ret = self.task_queue.remove(
                self.queue_name, *pandora.iterable(self.seeds), backup="failure"
            )
        else:
            ret = 0
        shutdown and self.flow({"next": None})  # type: ignore
        return ret

    def replace(self, new: dict, qtypes=("current", "temp", "failure")):
        """
        替换种子

        :return:
        """
        ret = self.task_queue.replace(self.queue_name, (self.seeds, new), qtypes=qtypes)
        self.seeds = new  # type: ignore
        return ret

    def save(self):
        return self.replace({**self.seeds})

    def divisive(self, qtypes=("temp",)):
        new = {**self.seeds, "$division": str(uuid.uuid4())}
        self.task_queue.put(self.queue_name, new, qtypes=qtypes)
        self.seeds = new  # type: ignore
        return new

    def submit(
            self, *obj: Union[Item, dict], call_later=False, attrs: Optional[dict] = None
    ) -> List["Context"]:
        """
        专门为新请求分支封装的方法, 会自动将请求放入队列
        传入的对象是新的种子

        :param attrs: 属性
        :param obj: 需要提交的对象，必须是一个种子
        :param call_later: 是否延迟调用, 如果延迟调用, 就是将种子放到当前队列, 等待其他机器获取消费, 否则
        :return:
        """
        ret = []
        attrs = attrs or {}
        for o in obj:
            assert o.__class__ in [Item, dict], TypeError(
                f"不支持的类型: {o.__class__}"
            )

            if not call_later:
                stuff = self.branch({"seeds": o, "next": self.target.on_seeds, **attrs})
                ret.append(stuff)
        else:
            if call_later:
                qtypes = "current"
            else:
                qtypes = "temp"

            self.target.put_seeds(
                obj,  # type: ignore
                task_queue=self.task_queue,
                queue_name=self.queue_name,
                qtypes=qtypes,
            )

        return ret

    def clear_proxy(self):
        manager.clear(self.request.proxy or self.target.proxy)
        self.request.proxies = None

    def error(self, e: Exception, shutdown=False):
        return self.failure(shutdown)


class InitContext(Flow):
    def __init__(
            self, target: "Spider", form: str = state.const.ON_INIT,
            seeds: List[Item] = None,
            task_queue: TaskQueue = None,
            maxsize: int = None,
            priority: bool = False,
            **kwargs
    ) -> None:
        self.seeds: List[Item] = seeds
        self.task_queue: TaskQueue = task_queue
        self.queue_name: str = kwargs.pop(
            "queue_name", f"{self.__class__.__module__}.{self.__class__.__name__}"
        )
        self.maxsize: int = maxsize
        self.priority: bool = priority
        super().__init__(form, target, **kwargs)
        self.target: Spider = target

    def success(self, shutdown=False):
        if self.form == state.const.AFTER_PUT_SEEDS:
            self.task_queue.remove(self.queue_name, self.seeds)

        shutdown and self.flow({"next": None})  # type: ignore

    def failure(self, shutdown=False):
        # 在种子投放之后, 如果收到失败信息, 就将种子移动至 failure 队列
        if self.form == state.const.AFTER_PUT_SEEDS:
            self.seeds and self.task_queue.remove(
                self.queue_name, self.seeds, backup="failure"
            )  # type: ignore

        shutdown and self.flow({"next": None})  # type: ignore

    def retry(self):
        pass

    def error(self, e: Exception, shutdown=False):
        shutdown and self.flow({"next": None})  # type: ignore


class Spider(Pangu):
    Context = Context
    InitContext = InitContext

    def __init__(self, **kwargs) -> None:
        self.concurrency = kwargs.pop("concurrency", 1)
        self.downloader: AbstractDownloader = kwargs.pop(
            "downloader", cffi.Downloader()
        )
        self.task_queue: Optional[TaskQueue] = (
                kwargs.pop("task_queue", LocalQueue()) or LocalQueue()
        )
        self.queue_name: Optional[str] = (
                kwargs.pop("queue_name", "")
                or f"{self.__class__.__module__}.{self.__class__.__name__}"
        )
        self.proxy: Optional[
            Union[dict, BaseProxy, str, List[Union[dict, BaseProxy, str]]]
        ] = kwargs.pop("proxy", None) or None
        self.forever = kwargs.pop("forever", False) or False
        super().__init__(**kwargs)
        self.number_of_total_requests = FastWriteCounter()  # 发起请求总数量
        self.number_of_failure_requests = FastWriteCounter()  # 发起请求失败数量
        self.number_of_new_seeds = (
            FastWriteCounter()
        )  # 动态新增的种子数量(翻页/拆分等等)
        self.number_of_seeds_obtained = FastWriteCounter()  # 获取得到的种子数量
        self.number_of_seeds_pending = 0  # 待处理的种子数量

    is_master = property(
        fget=lambda self: getattr(self, "$isMaster", False),
        fset=lambda self, v: setattr(self, "$isMaster", v),
        fdel=lambda self: setattr(self, "$isMaster", False),
    )

    @property
    def flows(self):
        return {
            self.on_consume: self.on_seeds,
            self.on_seeds: self.on_request,
            self.on_retry: self.on_request,
            self.on_request: self.on_response,
            self.on_response: self.on_pipeline,
            self.on_pipeline: None,
        }

    def run_init(self):
        """
        初始化

        :return:
        """

        task_queue: TaskQueue = self.get("init.task_queue", self.task_queue)  # type: ignore
        queue_name: str = self.get("init.queue_name", self.queue_name)  # type: ignore
        # 判断是否有初始化权限
        permission_info: dict = task_queue.command(
            queue_name, {"action": task_queue.COMMANDS.GET_PERMISSION, "interval": 10}
        )
        if not permission_info["state"]:
            logger.debug(
                f"[停止投放] 当前机器 ID: {state.MACHINE_ID}, 原因: {permission_info['msg']}"
            )
            return

        self.is_master = True
        task_queue.command(
            queue_name, {"action": task_queue.COMMANDS.SET_INIT, "interval": 5}
        )

        logger.debug(f"[开始投放] 获取初始化权限成功, MACHINE_ID: {state.MACHINE_ID}")
        # 本地的初始化记录 -> 启动传入的
        local_init_record: dict = self.get("init.record") or {}
        # 云端的初始化记录 -> 初始化的时候会存储(如果支持的话)
        remote_init_record = (
                task_queue.command(
                    queue_name, {"action": task_queue.COMMANDS.GET_RECORD, "filter": 1}
                )
                or {}
        )

        # 初始化记录信息
        record: dict = {
            **local_init_record,
            **remote_init_record,
            "queue_name": queue_name,
            "task_queue": task_queue,
            "identifier": state.MACHINE_ID,
        }

        # 设置一个启动时间, 防止被覆盖
        record.setdefault("start", str(datetime.datetime.now()))

        # 获取已经初始化的总量
        total = int(record.setdefault("total", 0))
        # 获取已经初始化的去重数量
        success = int(record.setdefault("success", 0))

        # 初始化总数量阈值 -> 大于这个数量停止初始化
        total_size = self.get("init.total.size", math.inf)
        # 当前初始化总量阈值 -> 大于这个数量停止初始化
        count_size = self.get("init.count.size", math.inf)
        # 初始化成功数量阈值 (去重) -> 大于这个数量停止初始化
        success_size = self.get("init.success.size", math.inf)
        # 初始化队列最大数量 -> 大于这个数量暂停初始化
        queue_size: int = self.get("init.queue.size", 100000)  # type: ignore
        is_continue: bool = self.get("init.continue", False)  # type: ignore

        # 历史记录的 TTL时间, 默认为 -1, 0->立即删除, -1->不过期, 其他->过期时间
        history_ttl = self.get("init.history.ttl", -1)  # type: ignore

        # record 的 TTL时间, 默认为 -1, 0->立即删除, -1->不过期, 其他->过期时间
        record_ttl = self.get("init.record.ttl", -1)  # type: ignore

        settings = {
            "total": total,
            "success": success,
            "count": 0,
            "record": record,
            "queue_size": queue_size,
            "total_size": total_size,
            "success_size": success_size,
            "count_size": count_size,
        }

        context = self.make_context(
            self.InitContext,
            task_queue=task_queue,
            queue_name=queue_name,
            settings=settings,
        )
        record: dict = settings.get("record") or {}  # type: ignore

        gen = pandora.invoke(
            func=self.make_seeds,
            kwargs={"record": record},
            annotations={Context: context},
            namespace={"context": context},
        )

        if not inspect.isgenerator(gen):
            gen = [gen]

        need_skip = min(success, total)
        for seeds in gen:
            if is_continue and need_skip > 0:
                seeds = pandora.iterable(seeds)
                raw_size = len(seeds)
                seeds = seeds[need_skip:]
                need_skip -= raw_size - len(seeds)

            if not seeds:
                continue

            ctx = context.copy()
            ctx.flow({"next": self.produce_seeds, "seeds": seeds})
            self.on_consume(ctx)

        record.update(finish=str(datetime.datetime.now()))
        task_queue.command(
            queue_name,
            {
                "action": task_queue.COMMANDS.RELEASE_INIT,
                "time": int(time.time() * 1000),
                "history_ttl": history_ttl,
                "record_ttl": record_ttl,
            },
        )
        return record

    def produce_seeds(self, context: InitContext):
        """
        生产种子

        :param context:
        :return:
        """
        settings: dict = context.obtain("settings") or {}

        seeds = context.seeds
        seeds = pandora.iterable(seeds)

        seeds = seeds[
                0: min(
                    [
                        settings["count_size"] - settings["count"],
                        settings["total_size"] - settings["total"],
                        settings["success_size"] - settings["success"],
                        len(seeds),
                    ]
                )
                ]
        context.update(
            {
                "maxsize": settings["queue_size"],
                "seeds": seeds,
            }
        )
        fettle = pandora.invoke(
            func=self.put_seeds,
            args=[context.seeds],
            kwargs={"where": "init"},
            annotations={InitContext: context},
            namespace={"context": context},
        )

        size = len(pandora.iterable(seeds))
        settings["total"] += size
        settings["success"] += fettle
        settings["count"] += fettle
        output = f"[投放成功] 总量: {settings['success']}/{settings['total']}; 当前: {fettle}/{size}; 目标: {context.queue_name}"
        settings["record"].update(
            {
                "total": settings["total"],
                "success": settings["success"],
                "output": output,
                "update": str(datetime.datetime.now()),
            }
        )

        context.task_queue.command(
            context.queue_name,
            {
                "action": context.task_queue.COMMANDS.SET_RECORD,
                "record": settings["record"],
            },
        )
        logger.debug(output)

        if (
                settings["total"] >= settings["total_size"]
                or settings["count"] >= settings["count_size"]
                or settings["success"] >= settings["success_size"]
        ):
            raise signals.Exit
        else:
            context.flow()

    def put_seeds(
            self,
            seeds: Union[dict, Item, Iterable[Item], Iterable[dict]],
            task_queue: Optional[TaskQueue] = ...,
            queue_name: Optional[str] = ...,
            maxsize: Optional[int] = None,
            priority: Optional[bool] = False,
            **kwargs,
    ):
        """
        将种子放入容器

        :param priority: 种子投放优先级, 需要队列支持
        :param maxsize: 种子限制数量, 到达这个数量会被阻塞
        :param queue_name: 队列名
        :param task_queue: 队列
        :param seeds: 种子
        :param kwargs: 其他参数
        :return:
        """

        if task_queue is ...:
            task_queue = self.task_queue
        if queue_name is ...:
            queue_name = self.queue_name
        seeds = seeds or {}
        task_queue.continue_(queue_name, maxsize=maxsize, interval=1)  # type: ignore
        return task_queue.put(  # type: ignore
            queue_name,  # type: ignore
            *pandora.iterable(seeds),
            priority=priority,
            **kwargs,  # type: ignore
        )

    def _when_put_seeds(self, raw_method):  # noqa
        @functools.wraps(raw_method)
        def wrapper(
                seeds: Union[dict, Item, List[Item], List[dict]],
                task_queue: Optional[TaskQueue] = ...,
                queue_name: Optional[str] = ...,
                maxsize: Optional[int] = None,
                priority: Optional[bool] = False,
                **kwargs,
        ):
            where = kwargs.pop("where", None)
            if where == "init":
                context: InitContext = InitContext.get_context()  # type: ignore
                context.form = state.const.BEFORE_PUT_SEEDS
                events.EventManager.invoke(context)
                args = [context.seeds]
                kws = {
                    "task_queue": context.task_queue,
                    "queue_name": context.queue_name,
                    "maxsize": context.maxsize,
                    "priority": context.priority,
                    **kwargs,
                }
            else:
                args = [seeds]
                kws = {
                    "task_queue": task_queue,
                    "queue_name": queue_name,
                    "maxsize": maxsize,
                    "priority": priority,
                    **kwargs,
                }

            prepared = pandora.prepare(func=raw_method, args=args, kwargs=kws)
            ret = prepared.func(*prepared.args, **prepared.kwargs)

            if where == "init":
                context: InitContext = InitContext.get_context()  # type: ignore
                context.form = state.const.AFTER_PUT_SEEDS
                events.EventManager.invoke(context)
            else:
                self.number_of_new_seeds.increment(ret)

            return ret

        return wrapper

    def run_spider(self):
        """
        start run spider
        get seeds and convert it to Request for submit

        :return:
        """
        task_queue: TaskQueue = self.get("spider.task_queue", self.task_queue)  # type: ignore
        queue_name: str = self.get("spider.queue_name", self.queue_name)  # type: ignore
        output = time.time()

        while True:
            context: Context = self.make_context(task_queue=task_queue, queue_name=queue_name)  # type: ignore
            with context:
                prepared = pandora.prepare(
                    func=self.get_seeds,
                    annotations={Context: context},
                    namespace={"context": context},
                )

                try:
                    fettle = prepared.func(*prepared.args, **prepared.kwargs)

                except signals.Wait as sig:
                    time.sleep(sig.duration)

                except signals.Success:
                    if context.form == state.const.BEFORE_GET_SEEDS:
                        time.sleep(1)

                    elif context.form == state.const.AFTER_GET_SEEDS:
                        context.success()

                    else:
                        raise

                except signals.Failure:
                    if context.form == state.const.BEFORE_GET_SEEDS:
                        time.sleep(1)

                    elif context.form == state.const.AFTER_GET_SEEDS:
                        pass

                    else:
                        raise

                except signals.Empty:
                    # 判断是否应该停止爬虫
                    #  没有初始化 + 本地没有运行的任务 + 任务队列为空 -> 退出
                    if (
                            not task_queue.command(
                                queue_name, {"action": task_queue.COMMANDS.IS_INIT}
                            )
                            and not self.forever
                            and self.dispatcher.running == 0
                            and task_queue.is_empty(
                        queue_name,
                        threshold=self.get("spider.threshold", default=0),
                    )
                    ):
                        number_of_seeds_obtained = self.number_of_seeds_obtained.value
                        number_of_new_seeds = self.number_of_new_seeds.value
                        number_of_total_requests = self.number_of_total_requests.value
                        number_of_failure_requests = (
                            self.number_of_failure_requests.value
                        )
                        number_of_success_requests = (
                                number_of_total_requests - number_of_failure_requests
                        )
                        if number_of_total_requests:
                            rate_of_success_requests = round(
                                number_of_success_requests
                                / number_of_total_requests
                                * 100,
                                2,
                            )
                        else:
                            rate_of_success_requests = 0

                        logger.debug(
                            f'[爬取完毕] '
                            f'队列名称: {queue_name} '
                            f'关闭阈值: {self.get("spider.threshold", default=0)} '
                            f'已获取的种子数量: {number_of_seeds_obtained} '
                            f'新增的种子数量: {number_of_new_seeds} '
                            f'总的请求数量: {number_of_total_requests} '
                            f'请求成功率: {rate_of_success_requests}% '
                        )

                        return {
                            "number_of_seeds_obtained": number_of_seeds_obtained,
                            "number_of_new_seeds": number_of_new_seeds,
                            "number_of_total_requests": number_of_total_requests,
                            "number_of_failure_requests": number_of_failure_requests,
                            "number_of_success_requests": number_of_success_requests,
                            "request_success_rate": rate_of_success_requests,
                        }

                    else:
                        if task_queue.smart_reverse(
                                queue_name, status=self.dispatcher.running
                        ):
                            logger.debug(f"[翻转队列] 队列名称: {queue_name}")

                        else:
                            if time.time() - output > 3600:
                                logger.debug(f"[等待任务] 队列名称: {queue_name}")
                                output = time.time()

                            time.sleep(1)

                except (KeyboardInterrupt, SystemExit):
                    raise
                except Exception as e:
                    EventManager.invoke(
                        Error(context=context, error=e), errors="output"
                    )

                else:
                    self.number_of_seeds_pending += len(pandora.iterable(fettle))
                    for seeds in pandora.iterable(fettle):
                        stuff = context.copy()
                        stuff.flow({"next": self.on_consume, "seeds": seeds})
                        self.submit(dispatch.Task(stuff.next.root, [stuff]))
                        self.number_of_seeds_obtained.increment()
                        self.number_of_seeds_pending -= 1

    def _when_run_spider(self, raw_method):
        @functools.wraps(raw_method)
        def wrapper(*args, **kwargs):
            with self.dispatcher:
                task_queue: TaskQueue = self.get("spider.task_queue", self.task_queue)  # type: ignore
                queue_name: str = self.get("spider.queue_name", self.queue_name)  # type: ignore
                server = task_queue.command(
                    queue_name,
                    {
                        "action": task_queue.COMMANDS.RUN_SUBSCRIBE,
                        "target": {
                            "collect-status": lambda: self.dispatcher.running
                                                      + self.number_of_seeds_pending
                        },
                    },
                )

                try:
                    return raw_method(*args, **kwargs)
                finally:
                    hasattr(server, "stop") and server.stop()  # type: ignore

        return wrapper

    def run_all(self):
        with self.dispatcher:
            t1 = int(time.time() * 1000)
            future = self.active(dispatch.Task(func=self.run_init))
            task_queue: TaskQueue = self.get("init.task_queue", self.task_queue)  # type: ignore
            queue_name: str = self.get("init.queue_name", self.queue_name)  # type: ignore # type: ignore
            task_queue.command(
                queue_name, {"action": task_queue.COMMANDS.WAIT_INIT, "time": t1}
            )
            return {"spider": self.run_spider(), "init": future.result()}

    @staticmethod
    def get_seeds(**kwargs) -> Union[Iterable[Item], Item]:
        """
        获取种子的真实方法

        :return:
        """
        context: Context = Context.get_context()  # type: ignore
        task_queue: TaskQueue = kwargs.pop("task_queue", None) or context.task_queue
        queue_name: str = kwargs.pop("queue_name", None) or context.queue_name
        return task_queue.get(name=queue_name, **kwargs)

    def _when_get_seeds(self, raw_method):
        @functools.wraps(raw_method)
        def wrapper(**kwargs):
            context: Context = Context.get_context()  # type: ignore
            context.form = state.const.BEFORE_GET_SEEDS
            events.EventManager.invoke(context)
            count = self.dispatcher.max_workers - self.dispatcher.running
            kwargs.setdefault("count", 1 if count <= 0 else count)
            prepared = pandora.prepare(
                func=raw_method,
                kwargs=kwargs,
                annotations={Context: context},
                namespace={"context": context},
            )
            ret = prepared.func(*prepared.args, **prepared.kwargs)
            # 没有种子返回空信号
            if ret is None:
                raise signals.Empty
            context.seeds = ret
            context.form = state.const.AFTER_GET_SEEDS
            events.EventManager.invoke(
                context,
                annotations={Context: context},
                namespace={"context": context, "seeds": context.seeds},
            )

            return ret

        return wrapper

    def on_seeds(self, context: Context):
        for index, stuff in enumerate(pandora.iterable(self.make_request(context))):
            if index == 0:
                ctx = context
            else:
                ctx = context.branch()
                ctx.division and ctx.divisive()  # type: ignore

            ctx.flow({"request": stuff})

    def on_retry(self, context: Context):
        """
        重试前

        :return:
        """
        self.number_of_failure_requests.increment()
        request: Request = context.request
        response: Response = context.response
        error: str = response.error if response else ""
        del context.response
        context.response = None  # type: ignore

        max_retry = request.get_options("$maxRetry", math.inf)
        if request.retry < min(max_retry, request.max_retry):
            # 如果是代理错误, 则不计算重试次数
            if not IGNORE_RETRY_PATTERN.search(error):
                request.retry += 1

            # 保留代理标志
            retain_proxy = request.get_options("$retainProxy", 0)
            if retain_proxy > 0:
                request.put_options("$retainProxy", retain_proxy - 1)
            else:
                context.clear_proxy()

            context.flow()

        else:
            msg = f'[超过重试次数] {f"SEEDS: {context.seeds}, " if context.seeds else ""} URL: {request.real_url}'
            logger.warning(msg)

            if request.retry >= max_retry:
                raise signals.Success

            else:
                raise signals.Failure

    def _when_on_retry(self, raw_method):  # noqa
        @functools.wraps(raw_method)
        def wrapper(context: Context, *args, **kwargs):
            self.number_of_failure_requests.increment()
            context.form = state.const.BEFORE_RETRY
            events.EventManager.invoke(context)
            prepared = pandora.prepare(
                func=raw_method,
                args=args,
                kwargs=kwargs,
                annotations={
                    type(context): context,
                    Response: context.response,
                    Item: context.seeds,
                },
                namespace={
                    "context": context,
                    "response": context.response,
                    "seeds": context.seeds,
                },
            )
            ret = prepared.func(*prepared.args, **prepared.kwargs)
            return ret

        return wrapper

    def on_request(self, context: Context):
        """
        发送请求，获取响应

        :param context:
        :return:
        """
        downloader = context.request.get_options("$downloader") or self.downloader
        if inspect.isclass(downloader):
            downloader = downloader()

        if inspect.iscoroutinefunction(downloader.fetch):
            future = self.active(
                dispatch.Task(func=downloader.fetch, args=[context.request])
            )
            response: Response = future.result()
        else:
            response: Response = downloader.fetch(context.request)  # noqa

        return response

    def _when_on_request(self, raw_method):  # noqa
        @functools.wraps(raw_method)
        def wrapper(context: Context, *args, **kwargs):
            context.form = state.const.BEFORE_REQUEST
            try:
                events.EventManager.invoke(
                    context,
                    annotations={
                        type(context): context,
                        Item: context.seeds,
                        Request: context.request,
                    },
                    namespace={
                        "context": context,
                        "seeds": context.seeds,
                        "request": context.request,
                    },
                )
            except signals.Switch as jump:
                if jump.by == "func":
                    raise
                else:
                    gen = context.response

            except signals.Skip:
                gen = context.response

            else:
                context.form = state.const.ON_REQUEST
                self.number_of_total_requests.increment()
                gen = pandora.invoke(
                    func=raw_method,
                    args=args,
                    kwargs=kwargs,
                    annotations={
                        type(context): context,
                        Item: context.seeds,
                        Request: context.request,
                    },
                    namespace={
                        "context": context,
                        "seeds": context.seeds,
                        "request": context.request,
                    },
                )
            for index, stuff in enumerate(pandora.iterable(gen)):
                stuff.request = (
                    context.request if stuff.request is ... else stuff.request
                )
                if index == 0:
                    ctx = context
                else:
                    ctx = context.branch()
                    ctx.division and ctx.divisive()  # type: ignore

                ctx.flow(
                    {"response": stuff, "request": stuff.request},
                    ctx.next == self.on_request,
                )
                events.EventManager.next(
                    ctx,
                    form=state.const.AFTER_REQUEST,
                    annotations={
                        type(ctx): ctx,
                        Item: ctx.seeds,
                        Request: ctx.request,
                        Response: ctx.response,
                    },
                    namespace={
                        "context": ctx,
                        "seeds": ctx.seeds,
                        "request": ctx.request,
                        "response": ctx.response,
                    },
                    callback=lambda mctx: mctx.rollback(recursion=False),
                )

        return wrapper

    def on_response(self, context: Context):
        """
        解析中

        :param context:
        :return:
        """
        callback: Callable = context.response.callback or self.parse
        prepared = pandora.prepare(
            func=callback,
            args=[context],
            annotations={
                Response: context.response,
                Request: context.request,
                Item: context.seeds,
                Context: context,
            },
            namespace={
                "context": context,
                "request": context.request,
                "response": context.response,
                "seeds": context.seeds,
            },
        )

        if inspect.iscoroutinefunction(prepared.func):
            future = self.active(
                dispatch.Task(
                    func=prepared.func, args=prepared.args, kwargs=prepared.kwargs
                )
            )
            items = future.result()

        else:
            items = prepared.func(*prepared.args, **prepared.kwargs)

        return items

    def _when_on_response(self, raw_method):  # noqa
        @functools.wraps(raw_method)
        def wrapper(context: Context, *args, **kwargs):
            context.form = state.const.ON_PARSE
            gen = pandora.invoke(
                func=raw_method,
                args=args,
                kwargs=kwargs,
                annotations={
                    Response: context.response,
                    Request: context.request,
                    Item: context.seeds,
                    Context: context,
                },
                namespace={
                    "context": context,
                    "request": context.request,
                    "response": context.response,
                    "seeds": context.seeds,
                },
            )
            gen = gen or []
            if isinstance(gen, (list, Items)):
                gen = [gen]
            for index, stuff in enumerate(pandora.iterable(gen)):
                if index == 0:
                    ctx = context
                else:
                    ctx = context.branch()
                    ctx.division and ctx.divisive()  # type: ignore

                ctx.flow({"items": stuff}, flag=ctx.next == self.on_response)

        return wrapper

    def on_pipeline(self, context: Context):
        """
        管道中

        :param context:
        :return:
        """
        items: Items = context.items
        callback: Callable = items.callback or self.item_pipeline
        prepared = pandora.prepare(
            func=callback,
            args=[items],
            annotations={
                Response: context.response,
                Request: context.request,
                Items: context.items,
                Item: context.seeds,
                Context: context,
            },
            namespace={
                "context": context,
                "request": context.request,
                "response": context.response,
                "seeds": context.seeds,
                "items": context.items,
            },
        )

        if inspect.iscoroutinefunction(callback):
            future = self.active(
                dispatch.Task(
                    func=prepared.func, args=prepared.args, kwargs=prepared.kwargs
                )
            )
            future.result()
        else:
            prepared.func(*prepared.args, **prepared.kwargs)

    def _when_on_pipeline(self, raw_method):  # noqa
        @functools.wraps(raw_method)
        def wrapper(context: Context, *args, **kwargs):
            context.form = state.const.BEFORE_PIPELINE
            try:
                events.EventManager.invoke(
                    context,
                    annotations={
                        Response: context.response,
                        Request: context.request,
                        Items: context.items,
                        Item: context.seeds,
                        Context: context,
                    },
                    namespace={
                        "context": context,
                        "request": context.request,
                        "response": context.response,
                        "seeds": context.seeds,
                        "items": context.items,
                    },
                )
            except signals.Skip:
                pass

            else:
                context.form = state.const.ON_PIPELINE
                prepared = pandora.prepare(
                    func=raw_method,
                    args=args,
                    kwargs=kwargs,
                    annotations={
                        Response: context.response,
                        Request: context.request,
                        Items: context.items,
                        Item: context.seeds,
                        Context: context,
                    },
                    namespace={
                        "context": context,
                        "request": context.request,
                        "response": context.response,
                        "seeds": context.seeds,
                        "items": context.items,
                    },
                )
                prepared.func(*prepared.args, **prepared.kwargs)

            context.form = state.const.AFTER_PIPELINE
            events.EventManager.invoke(
                context,
                annotations={
                    Response: context.response,
                    Request: context.request,
                    Items: context.items,
                    Item: context.seeds,
                    Context: context,
                },
                namespace={
                    "context": context,
                    "request": context.request,
                    "response": context.response,
                    "seeds": context.seeds,
                    "items": context.items,
                },
            )

            context.flow(flag=context.next == self.on_pipeline)

        return wrapper

    def make_seeds(self, context: Context, **kwargs):
        raise NotImplementedError

    def make_request(self, context: Context) -> Request:
        return pandora.invoke(Request, kwargs=context.seeds)  # type: ignore

    def _when_make_request(self, raw_method):  # noqa
        @functools.wraps(raw_method)
        def wrapper(context: Context):
            context.form = state.const.BEFORE_MAKE_REQUEST
            events.EventManager.invoke(
                context,
                annotations={Item: context.seeds, Context: context},
                namespace={
                    "context": context,
                    "seeds": context.seeds,
                },
            )
            context.form = state.const.ON_MAKE_REQUEST
            prepared = pandora.prepare(
                func=raw_method,
                annotations={Item: context.seeds, Context: context},
                namespace={
                    "context": context,
                    "seeds": context.seeds,
                },
            )
            future_max_retry = context.seeds.get("$futureMaxRetry")
            future_retry: int = context.seeds.get("$futureRetry") or 0  # type: ignore
            request = prepared.func(*prepared.args, **prepared.kwargs)
            if future_max_retry:
                request.max_retry = future_max_retry
                request.put_options("$maxRetry", future_max_retry)
                request.retry = future_retry + 1
                context.seeds["$futureRetry"] = request.retry

            context.form = state.const.AFTER_MAKE_REQUEST
            events.EventManager.invoke(
                context,
                annotations={Request: request, Item: context.seeds, Context: context},
                namespace={
                    "context": context,
                    "request": request,
                    "seeds": context.seeds,
                },
            )

            return request

        return wrapper

    def parse(self, context: Context) -> Union[List[dict], Items]:
        return Items(
            {
                "request": context.request.curl,
                "response": {
                    "url": context.response.url,
                    "headers": context.response.headers,
                    "status_code": context.response.status_code,
                    "text": context.response.text,
                },
            }
        )

    def item_pipeline(self, context: Context):
        context.items and logger.debug(context.items)  # type: ignore
        context.success()

    def install(self):
        super().install()
        self.use(
            state.const.BEFORE_REQUEST,
            {"func": on_request.Before.fake_ua},
            {"func": on_request.Before.set_proxy, "index": math.inf},
        )
        self.use(
            state.const.AFTER_REQUEST,
            {"func": on_request.After.show_response},
            {"func": on_request.After.conditional_scripts},
            {"func": on_request.After.bypass},
        )

    @pandora.Method
    def survey(
            binding,  # noqa
            *seeds: dict,
            attrs: dict = None,  # type: ignore
            modded: dict = None,  # type: ignore
            extract: list = None,  # type: ignore
    ) -> List[Context]:
        """
        调查种子, collect 会收集产生的 Context
        用户可以从 collect 的结果中根据 Context 获取到当时的 response, items, request, seeds 等等

        但是: 如果在运行过程中用户修改了里面的结果, 那会被覆盖掉
        survey 会屏蔽原来的 make_seeds 和 item_pipeline 方法, 会使用用户传入的 seeds, 并且仅仅输出结果 (不会存储)

        :param extract:
        :param modded: 魔改 class
        :param attrs: 初始化参数
        :param seeds: 需要调查的种子
        :return:
        """
        attrs = attrs or {}
        modded = modded or {}
        extract = extract or []

        if isinstance(binding, type):
            cls = binding
        else:
            cls = binding.__class__
            attrs.setdefault("proxy", binding.proxy)
            attrs.setdefault("concurrency", binding.concurrency)
            attrs.setdefault("downloader", binding.downloader)

        collect = queue.Queue()

        def mock_make_seeds(self):  # noqa
            return seeds

        def mock_on_request(self, context: Context):
            collect.put(context)
            return super(self.__class__, self).on_request(context)

        def mock_item_pipeline(self, context: Context):  # noqa
            logger.debug(context.items)
            context.success()

        modded.setdefault("make_seeds", mock_make_seeds)
        modded.setdefault("on_request", mock_on_request)
        modded.setdefault("item_pipeline", mock_item_pipeline)
        attrs.setdefault("task_queue", LocalQueue())
        attrs.setdefault("queue_name", f"{cls.__module__}.{cls.__name__}:survey")
        clazz = type("Survey", (cls,), modded)
        survey: Spider = clazz(**attrs)  # type: ignore
        survey.run()
        return (
            list(collect.queue)
            if not extract
            else [{k: getattr(c, k) for k in extract} for c in collect.queue]
        )  # type: ignore

    def create_fetcher(
            self,
            downloader: Optional[AbstractDownloader] = None,
            plugins: Union[dict, None] = ...,
            **options,
    ):
        key = json.dumps(
            {"downloader": id(downloader), "plugins": plugins, **options},
            default=str,
            separators=(",", ":"),
            sort_keys=True,
        )
        if key not in _fetchers_cache:
            options.setdefault("downloader", downloader or self.downloader)
            spider = Spider(**options)

            # 不需要任何插件
            if not plugins:
                for plugin in spider.plugins:
                    plugin.unregister()

            # 使用默认插件
            elif plugins is ...:
                pass

            else:
                for plugin in spider.plugins:
                    plugin.unregister()
                for form, plugin in plugins:
                    spider.use(form, *pandora.iterable(plugin))

            if inspect.iscoroutinefunction(spider.downloader.fetch):
                ctx = spider.dispatcher
            else:
                ctx = contextlib.nullcontext()

            _fetchers_cache[key] = ctx, spider

        return _fetchers_cache[key]

    def fetch(
            self,
            request: Union[Request, dict],
            downloader: Optional[AbstractDownloader] = None,
            proxy: Optional[Union[dict, BaseProxy]] = ...,
            plugins: Union[dict, None] = ...,
            **options,
    ) -> Response:
        """
        发送请求获取响应

        默认情况下, 只要response 的状态码不为-1( 框架内部错误/ 异常) 就会结束
        如果失败五次, 也会结束, 所以需要一定成功可以将 request.max_retry = math.inf

        :param plugins: 插件, 默认使用 fake_ua 和 set_proxy; 传入 None 表示什么都不用, 自定义则使用自定义的
        :param proxy: 请求代理 Key(Rules), 不传的时候默认使用 self.proxy
        :param downloader: 下载器, 不传的时候默认使用  self.downloader
        :param request: 需要请求的 request, 可以是字典(key value 需要对应 request 对象的实例参数)
        :param options: custom spider 实例化的其他选项
        :return:
        """
        if isinstance(request, dict):
            request = Request(**request)

        if request.ok is ...:
            request.ok = "response.status_code != -1"

        if not request.proxy:
            if proxy is ...:
                request.proxy = self.proxy
            elif proxy:
                request.proxy = proxy  # type: ignore
            else:
                pass

        ctx, spider = self.create_fetcher(downloader, plugins, **options)

        with ctx:
            context = spider.make_context(
                request=request,
                next=spider.on_request,
                flows={spider.on_request: None, spider.on_retry: spider.on_request},
            )
            context.failure = lambda shutdown: context.flow({"next": None})
            spider.on_consume(context=context)
            return context.response

    def disable_statistics(self):
        counters = [
            "number_of_total_requests",
            "number_of_failure_requests",
            "number_of_new_seeds",
            "number_of_seeds_obtained",
        ]
        for count_name in counters:
            self.get(count_name).disable()  # type: ignore
