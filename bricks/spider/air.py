# -*- coding: utf-8 -*-
# @Time    : 2023-11-15 14:09
# @Author  : Kem
# @Desc    :
import contextlib
import dataclasses
import datetime
import functools
import inspect
import math
import re
import time
from typing import Optional, List, Union, Iterable, Callable, Dict

from loguru import logger

from bricks import state
from bricks.core import dispatch, signals, events
from bricks.core.genesis import Pangu
from bricks.downloader import genesis, cffi
from bricks.lib.context import Flow
from bricks.lib.counter import FastWriteCounter
from bricks.lib.headers import Header
from bricks.lib.items import Items
from bricks.lib.nodes import RenderNode
from bricks.lib.proxies import manager
from bricks.lib.queues import TaskQueue, LocalQueue, Item
from bricks.lib.request import Request
from bricks.lib.response import Response
from bricks.plugins import on_request
from bricks.utils import pandora

IP_REGEX = re.compile(r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}:\d+')


@dataclasses.dataclass
class Download(RenderNode):
    url: str
    params: Optional[dict] = None
    method: str = 'GET'
    body: Union[str, dict] = None
    headers: Union[Header, dict] = None
    cookies: Dict[str, str] = None
    options: dict = None
    timeout: int = ...
    allow_redirects: bool = True
    proxies: Optional[str] = None
    proxy: Optional[dict] = None
    status_codes: Optional[dict] = ...
    retry: int = 0
    max_retry: int = 5
    strict: str = "fix"

    def to_request(self) -> Request:
        return Request(
            url=self.url,
            params=self.params,
            method=self.method,
            body=self.body,
            headers=self.headers,
            cookies=self.cookies,
            options=self.options,
            timeout=self.timeout,
            allow_redirects=self.allow_redirects,
            proxies=self.proxies,
            proxy=self.proxy,
            status_codes=self.status_codes,
            retry=self.retry,
            max_retry=self.max_retry
        )

    def to_response(self, spider: "Spider" = None) -> Response:
        dispatcher = contextlib.nullcontext()
        if not spider:
            spider = Spider()

        if inspect.iscoroutinefunction(spider.downloader.fetch):
            dispatcher = spider.dispatcher

        with dispatcher:
            request = self.to_request()
            context = spider.make_context(
                request=request,
                next=spider.on_request,
                flows={
                    spider.on_request: None,
                    spider.on_retry: spider.on_request
                },
            )
            context.failure = lambda shutdown: context.flow({"next": None})
            spider.on_consume(context=context)
            return context.response


class Context(Flow):
    def __init__(
            self,
            target: "Spider",
            form: str = state.const.ON_CONSUME,
            **kwargs

    ) -> None:
        self.request: Request = kwargs.pop("request", None)
        self.response: Response = kwargs.pop("response", None)
        self.seeds: Item = kwargs.pop("seeds", None)
        self.items: Items = kwargs.pop("items", None)
        self.task_queue: TaskQueue = kwargs.pop("task_queue", None)
        self.queue_name: str = kwargs.pop("queue_name", None)
        super().__init__(form, target, **kwargs)
        self.target: Spider = target

    def __setattr__(self, key, value):
        maps = {
            "seeds": Item,
            "items": Items,
        }

        if key in maps and type(value) != maps[key]:
            value = maps[key](value)

        super().__setattr__(key, value)

    def success(self, shutdown=False):
        ret = self.task_queue.remove(self.queue_name, self.seeds)
        shutdown and self.flow({"next": None})
        return ret

    def retry(self):
        self.flow({"next": self.target.on_retry})

    def failure(self, shutdown=False):
        ret = self.task_queue.remove(self.queue_name, self.seeds, backup='failure')
        shutdown and self.flow({"next": None})
        return ret

    def submit(self, obj: Union[Request, Item, dict], call_later=False) -> "Context":
        """
        专门为新请求分支封装的方法, 会自动将请求放入队列
        传入的对象可以是新的种子 / 新的 request

        :param obj:
        :param call_later: 是否延迟调用, 如果延迟调用, 就是将种子放到当前队列, 等待其他机器获取消费, 否则
        :return:
        """
        assert obj.__class__ in [Request, Item, dict], f"不支持的类型: {obj.__class__}"
        if obj.__class__ in [Item, dict]:
            if call_later:
                self.task_queue.put(self.queue_name, obj)
                return self
            else:
                self.task_queue.put(self.queue_name, obj, qtypes="temp")
                return self.branch({"seeds": obj, "next": self.target.on_seeds})
        else:
            return self.branch({"request": obj, "next": self.target.on_request})

    def clear_proxy(self):
        manager.clear_proxy(self.request.proxy or self.target.proxy)
        self.request.proxies = None


class Spider(Pangu):
    Context = Context

    def __init__(
            self,
            concurrency: Optional[int] = 1,
            survey: Optional[Union[dict, List[dict]]] = None,
            downloader: Optional[Union[str, genesis.Downloader]] = None,
            task_queue: Optional[TaskQueue] = None,
            queue_name: Optional[str] = "",
            proxy: Optional[dict] = None,
            forever: Optional[bool] = False,
            **kwargs
    ) -> None:

        self.concurrency = concurrency
        self.survey = survey
        self.downloader = downloader or cffi.Downloader()
        self.task_queue = LocalQueue() if not task_queue or survey else task_queue
        self.proxy = proxy
        self.queue_name = queue_name or self.__class__.__name__
        self.forever = forever
        super().__init__(**kwargs)
        self._total_number_of_requests = FastWriteCounter()  # 发起请求总数量
        self._number_of_failure_requests = FastWriteCounter()  # 发起请求失败数量

    @property
    def flows(self):
        return {
            self.on_consume: self.on_seeds,
            self.on_seeds: self.on_request,
            self.on_retry: self.on_request,
            self.on_request: self.on_response,
            self.on_response: self.on_pipeline,
            self.on_pipeline: None
        }

    def run_init(self):
        """
        初始化

        :return:
        """

        task_queue: TaskQueue = self.get("init.task_queue", self.task_queue)
        queue_name: str = self.get("init.queue_name", self.queue_name)
        # 判断是否有初始化权限
        pinfo: dict = task_queue.command(queue_name, {"action": task_queue.COMMANDS.GET_PERMISSION})
        if not pinfo['state']:
            logger.debug(f"[停止投放] 当前机器 ID: {state.MACHINE_ID}, 原因: {pinfo['msg']}")
            return

        task_queue.command(queue_name, {"action": task_queue.COMMANDS.SET_INIT})

        logger.debug(f"[开始投放] 获取初始化权限成功, MACHINE_ID: {state.MACHINE_ID}")
        # 本地的初始化记录 -> 启动传入的
        local_init_record: dict = self.get('init.record') or {}
        # 云端的初始化记录 -> 初始化的时候会存储(如果支持的话)
        remote_init_record = task_queue.command(
            queue_name,
            {"action": task_queue.COMMANDS.GET_RECORD, "filter": 1}
        ) or {}

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
        total = int(record.setdefault('total', 0))
        # 获取已经初始化的去重数量
        success = int(record.setdefault('success', 0))

        # 初始化总数量阈值 -> 大于这个数量停止初始化
        total_size = self.get('init.total.size', math.inf)
        # 当前初始化总量阈值 -> 大于这个数量停止初始化
        count_size = self.get('init.count.size', math.inf)
        # 初始化成功数量阈值 (去重) -> 大于这个数量停止初始化
        success_size = self.get('init.success.size', math.inf)
        # 初始化队列最大数量 -> 大于这个数量暂停初始化
        queue_size: int = self.get("init.queue.size", 100000)

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

        context_ = self.make_context(
            task_queue=task_queue,
            queue_name=queue_name,
            next=self.produce_seeds,
            settings=settings
        )
        self.on_consume(context_)
        record.update(finish=str(datetime.datetime.now()))
        task_queue.command(queue_name, {"action": task_queue.COMMANDS.RELEASE_INIT, "time": int(time.time() * 1000)})
        return record

    def produce_seeds(self, context: Context):
        """
        生产种子

        :param context:
        :return:
        """
        settings: dict = context.obtain("settings", {})
        record: dict = settings.get("record") or {}

        gen = pandora.invoke(
            func=self.make_seeds,
            kwargs={"record": record},
            annotations={Context: context},
            namespace={'context': context},
        )

        if not inspect.isgenerator(gen):
            gen = [gen]

        for seeds in gen:
            seeds = pandora.iterable(seeds)

            seeds = seeds[0:min([
                settings['count_size'] - settings['count'],
                settings['total_size'] - settings['total'],
                settings['success_size'] - settings['success'],
                len(seeds)
            ])]
            context.seeds = seeds
            fettle = pandora.invoke(
                func=self.put_seeds,
                kwargs={
                    "seeds": seeds,
                    "maxsize": settings['queue_size'],
                    "where": "init",
                    "task_queue": context.task_queue,
                    "queue_name": context.queue_name,

                },
                annotations={Context: context},
                namespace={"context": context},
            )

            size = len(pandora.iterable(seeds))
            settings['total'] += size
            settings['success'] += fettle
            settings['count'] += fettle
            output = f"[投放成功] 总量: {settings['success']}/{settings['total']}; 当前: {fettle}/{size}; 目标: {context.queue_name}"
            settings["record"].update({
                "total": settings['total'],
                "success": settings['success'],
                "output": output,
                "update": str(datetime.datetime.now()),
            })

            context.task_queue.command(context.queue_name, {
                "action": context.task_queue.COMMANDS.SET_RECORD,
                "record": settings["record"]
            })
            logger.debug(output)

            if (
                    settings['total'] >= settings['total_size'] or
                    settings['count'] >= settings['count_size'] or
                    settings['success'] >= settings['success_size']
            ):
                raise signals.Exit
        else:
            context.flow()

    def put_seeds(self, **kwargs):
        """
        将种子放入容器

        :param kwargs:
        :return:
        """
        task_queue: TaskQueue = kwargs.pop('task_queue', None) or self.task_queue
        queue_name: str = kwargs.pop('queue_name', None) or self.queue_name
        seeds = kwargs.pop('seeds', {})

        maxsize = kwargs.pop('maxsize', None)
        priority = kwargs.pop('priority', False)
        task_queue.continue_(queue_name, maxsize=maxsize, interval=1)
        return task_queue.put(queue_name, *pandora.iterable(seeds), priority=priority, **kwargs)

    def run_spider(self):
        """
        start run spider
        get seeds and convert it to Request for submit

        :return:
        """
        count = 0
        task_queue: TaskQueue = self.get("spider.task_queue", self.task_queue)
        queue_name: str = self.get("spider.queue_name", self.queue_name)
        output = time.time()

        while True:
            context = self.make_context(
                task_queue=task_queue,
                queue_name=queue_name
            )

            prepared = pandora.prepare(
                func=self.get_seeds,
                annotations={Context: context},
                namespace={"context": context}
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
                        not task_queue.command(queue_name, {"action": task_queue.COMMANDS.IS_INIT}) and
                        not self.forever and
                        task_queue.is_empty(queue_name, threshold=self.get("spider.threshold", default=0))
                ):
                    if self.dispatcher.running == 0:
                        return count
                    else:
                        time.sleep(1)

                else:

                    if task_queue.smart_reverse(queue_name, status=self.dispatcher.running):
                        logger.debug(f"[翻转队列] 队列名称: {self.queue_name}")

                    else:
                        if time.time() - output > 60:
                            logger.debug(f"[等待任务] 队列名称: {self.queue_name}")
                            output = time.time()

                        time.sleep(1)

            else:
                for seeds in pandora.iterable(fettle):
                    stuff = context.copy()
                    stuff.seeds = seeds
                    stuff.flow({"next": self.on_consume})
                    self.submit(dispatch.Task(stuff.next.root, [stuff]))
                    count += 1

    def _when_run_spider(self, raw_method):
        @functools.wraps(raw_method)
        def wrapper(*args, **kwargs):
            with self.dispatcher:
                task_queue: TaskQueue = self.get("spider.task_queue", self.task_queue)
                queue_name: str = self.get("spider.queue_name", self.queue_name)
                task_queue.command(
                    queue_name,
                    {
                        "action": task_queue.COMMANDS.RUN_SUBSCRIBE,
                        "target": self
                    }
                )

                return raw_method(*args, **kwargs)

        return wrapper

    def run_all(self):
        with self.dispatcher:
            t1 = int(time.time() * 1000)
            init_task = dispatch.Task(func=self.run_init)
            self.active(init_task)
            task_queue: TaskQueue = self.get("init.task_queue", self.task_queue)
            queue_name: str = self.get("init.queue_name", self.queue_name)
            task_queue.command(queue_name, {"action": task_queue.COMMANDS.WAIT_INIT, "time": t1})
            self.run_spider()

    @staticmethod
    def get_seeds(context: Context) -> Union[Iterable[Item], Item]:
        """
        获取种子的真实方法

        :param context:
        :return:
        """
        task_queue: TaskQueue = context.task_queue
        queue_name: str = context.queue_name
        return task_queue.get(queue_name)

    def _when_get_seeds(self, raw_method):
        @functools.wraps(raw_method)
        def wrapper(context: Context, *args, **kwargs):
            context.form = state.const.BEFORE_GET_SEEDS
            events.EventManger.invoke(context)
            count = self.dispatcher.max_workers - self.dispatcher.running
            kwargs.setdefault('count', 1 if count <= 0 else count)
            prepared = pandora.prepare(
                func=raw_method,
                args=args,
                kwargs=kwargs,
                annotations={Context: context},
                namespace={"context": context}
            )
            ret = prepared.func(*prepared.args, **prepared.kwargs)

            # 没有种子返回空信号
            if ret is None: raise signals.Empty

            context.form = state.const.AFTER_GET_SEEDS
            events.EventManger.invoke(context)

            return ret

        return wrapper

    def on_seeds(self, context: Context):
        request = self.make_request(context)
        context.flow({"request": request})

    def on_retry(self, context: Context):
        """
        重试前

        :return:
        """
        self._number_of_failure_requests.increment()
        request: Request = context.request
        response: Response = context.response

        options: dict = request.options
        if request.retry < request.max_retry - 1:

            if '$requestId' in options:
                request.retry += 1

            elif "proxyerror" not in str(response.error).lower():
                request.retry += 1

            if '$retry' not in options:
                context.clear_proxy()

            else:
                options.pop('$retry', None)

            if IP_REGEX.match((str(request.proxy) or "")):
                request.proxy = ""

        else:
            logger.warning(
                f'[超过重试次数] {f"SEEDS: {context.seeds}, " if context.seeds else ""} URL: {request.real_url}')

            if request.retry > options.get('$requestRetry', math.inf):
                raise signals.Success

            else:
                raise signals.Failure

    def _when_on_retry(self, raw_method):  # noqa
        @functools.wraps(raw_method)
        def wrapper(context: Context, *args, **kwargs):
            context.form = state.const.BEFORE_RETRY
            prepared = pandora.prepare(
                func=raw_method,
                args=args,
                kwargs=kwargs,
                annotations={
                    Context: context,
                    Response: context.response,
                    Item: context.seeds
                },
                namespace={
                    "context": context,
                    "response": context.response,
                    "seeds": context.seeds,
                }
            )
            ret = prepared.func(*prepared.args, **prepared.kwargs)
            context.flow()
            return ret

        return wrapper

    def on_request(self, context: Context):
        """
        发送请求，获取响应

        :param context:
        :return:
        """
        if inspect.iscoroutinefunction(self.downloader.fetch):
            future = self.active(
                dispatch.Task(
                    func=self.downloader.fetch,
                    args=[context.request]
                )
            )
            response: Response = future.result()
        else:
            response: Response = self.downloader.fetch(context.request)

        return response

    def _when_on_request(self, raw_method):  # noqa
        @functools.wraps(raw_method)
        def wrapper(context: Context, *args, **kwargs):
            context.form = state.const.BEFORE_REQUEST
            events.EventManger.invoke(context)
            context.form = state.const.ON_REQUEST
            prepared = pandora.prepare(
                func=raw_method,
                args=args,
                kwargs=kwargs,
                annotations={
                    Context: context,
                    Item: context.seeds,
                    Request: context.request,
                },
                namespace={
                    "context": context,
                    "seeds": context.seeds,
                    "request": context.request
                }
            )
            response: Response = prepared.func(*prepared.args, **prepared.kwargs)

            context.form = state.const.AFTER_REQUEST
            context.response = response

            events.EventManger.invoke(context)
            context.flow()

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
                Context: context
            },
            namespace={
                "context": context,
                "request": context.request,
                "response": context.response,
                "seeds": context.seeds,
            }
        )

        if inspect.iscoroutinefunction(prepared.func):
            future = self.active(
                dispatch.Task(
                    func=prepared.func,
                    args=prepared.args,
                    kwargs=prepared.kwargs
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
            prepared = pandora.prepare(
                func=raw_method,
                args=args,
                kwargs=kwargs,
                annotations={
                    Response: context.response,
                    Request: context.request,
                    Item: context.seeds,
                    Context: context
                },
                namespace={
                    "context": context,
                    "request": context.request,
                    "response": context.response,
                    "seeds": context.seeds,
                }
            )
            products = prepared.func(*prepared.args, **prepared.kwargs)
            if not inspect.isgenerator(products):
                products = [products]

            for index, product in enumerate(products):

                if index == 0:
                    context.flow({"items": product})
                else:
                    # 有多个 items, 直接开一个新 branch
                    context.branch({"items": product})

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
                Context: context
            },
            namespace={
                "context": context,
                "request": context.request,
                "response": context.response,
                "seeds": context.seeds,
                "items": context.items,
            }
        )

        if inspect.iscoroutinefunction(callback):
            future = self.active(
                dispatch.Task(
                    func=prepared.func,
                    args=prepared.args,
                    kwargs=prepared.kwargs
                )
            )
            future.result()
        else:
            prepared.func(*prepared.args, **prepared.kwargs)

    def _when_on_pipeline(self, raw_method):  # noqa
        @functools.wraps(raw_method)
        def wrapper(context: Context, *args, **kwargs):
            context.form = state.const.BEFORE_PIPELINE
            events.EventManger.invoke(context)
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
                    Context: context
                },
                namespace={
                    "context": context,
                    "request": context.request,
                    "response": context.response,
                    "seeds": context.seeds,
                    "items": context.items,
                }
            )
            prepared.func(*prepared.args, **prepared.kwargs)
            context.form = state.const.AFTER_PIPELINE
            events.EventManger.invoke(context)
            context.flow()

        return wrapper

    def make_seeds(self, context: Context, **kwargs):
        raise NotImplementedError

    def make_request(self, context: Context) -> Request:
        raise NotImplementedError

    def parse(self, context: Context):
        raise NotImplementedError

    def item_pipeline(self, context: Context):
        context.items and logger.debug(context.items)

    def install(self):
        super().install()
        self.use(
            state.const.BEFORE_REQUEST,
            {"func": on_request.Before.set_proxy, "index": math.inf}
        )
        self.use(
            state.const.AFTER_REQUEST,
            {"func": on_request.After.show_response},
            {"func": on_request.After.is_success}
        )
