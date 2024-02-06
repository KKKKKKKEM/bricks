# -*- coding: utf-8 -*-
# @Time    : 2023-11-15 14:09
# @Author  : Kem
# @Desc    :
import datetime
import functools
import inspect
import math
import queue
import re
import time
from typing import Optional, Union, Iterable, Callable, List

from loguru import logger

from bricks import state
from bricks.core import dispatch, signals, events
from bricks.core.context import Flow
from bricks.core.genesis import Pangu
from bricks.downloader import cffi, AbstractDownloader
from bricks.lib.counter import FastWriteCounter
from bricks.lib.items import Items
from bricks.lib.proxies import manager, BaseProxy
from bricks.lib.queues import TaskQueue, LocalQueue, Item
from bricks.lib.request import Request
from bricks.lib.response import Response
from bricks.plugins import on_request
from bricks.utils import pandora

IGNORE_RETRY_PATTERN = re.compile("ProxyError", re.IGNORECASE)


class Context(Flow):
    def __init__(
            self,
            target: "Spider",
            form: str = state.const.ON_CONSUME,
            **kwargs

    ) -> None:
        self.request: Request = kwargs.pop("request", None)
        self.response: Response = kwargs.pop("response", None)
        self.seeds: Union[Item, List[Item]] = kwargs.pop("seeds", None)
        self.items: Items = kwargs.pop("items", None)
        self.task_queue: TaskQueue = kwargs.pop("task_queue", None)
        self.queue_name: str = kwargs.pop("queue_name", f'{self.__class__.__module__}.{self.__class__.__name__}')
        super().__init__(form, target, **kwargs)
        self.target: Spider = target

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
        shutdown and self.flow({"next": None})
        return ret

    def retry(self):
        self.flow({"next": self.target.on_retry})

    def failure(self, shutdown=False):
        if self.seeds:
            ret = self.task_queue.remove(self.queue_name, *pandora.iterable(self.seeds), backup='failure')
        else:
            ret = 0
        shutdown and self.flow({"next": None})
        return ret

    def replace(self, new: dict, qtypes=("current", "temp", "failure")):
        """
        替换种子

        :return:
        """
        ret = self.task_queue.replace(self.queue_name, (self.seeds, new), qtypes=qtypes)
        self.seeds.fingerprint = new
        return ret

    def submit(self, *obj: Union[Item, dict], call_later=False, attrs: dict = None) -> List["Context"]:
        """
        专门为新请求分支封装的方法, 会自动将请求放入队列
        传入的对象可以是新的种子 / 新的 request

        :param attrs:
        :param obj:
        :param call_later: 是否延迟调用, 如果延迟调用, 就是将种子放到当前队列, 等待其他机器获取消费, 否则
        :return:
        """
        ret = []
        attrs = attrs or {}
        for o in obj:
            assert o.__class__ in [Item, dict], TypeError(f"不支持的类型: {o.__class__}")

            if not call_later:
                stuff = self.branch({"seeds": o, "next": self.target.on_seeds, **attrs})
                ret.append(stuff)
        else:
            if call_later:
                self.task_queue.put(self.queue_name, *obj)
            else:
                self.task_queue.put(self.queue_name, *obj, qtypes="temp")

        return ret

    def clear_proxy(self):
        manager.clear(self.request.proxy or self.target.proxy)
        self.request.proxies = None

    error = failure


class InitContext(Flow):

    def __init__(
            self,
            target: "Spider",
            form: str = state.const.ON_INIT,
            **kwargs

    ) -> None:
        self.seeds: List[Item] = kwargs.pop("seeds", None)
        self.task_queue: TaskQueue = kwargs.pop("task_queue", None)
        self.queue_name: str = kwargs.pop("queue_name", f'{self.__class__.__module__}.{self.__class__.__name__}')
        super().__init__(form, target, **kwargs)
        self.target: Spider = target

    def success(self, shutdown=False):
        if self.form == state.const.AFTER_PUT_SEEDS:
            self.task_queue.remove(self.queue_name, self.seeds)

        shutdown and self.flow({"next": None})

    def failure(self, shutdown=False):
        # 在种子投放之后, 如果收到失败信息, 就将种子移动至 failure 队列
        if self.form == state.const.AFTER_PUT_SEEDS:
            self.seeds and self.task_queue.remove(self.queue_name, self.seeds, backup='failure')

        shutdown and self.flow({"next": None})

    def retry(self):
        pass

    def error(self, shutdown=False):
        shutdown and self.flow({"next": None})


class Spider(Pangu):
    Context = Context
    InitContext = InitContext

    def __init__(
            self,
            concurrency: Optional[int] = 1,
            downloader: Optional[Union[str, AbstractDownloader]] = None,
            task_queue: Optional[TaskQueue] = None,
            queue_name: Optional[str] = "",
            proxy: Optional[Union[dict, BaseProxy]] = None,
            forever: Optional[bool] = False,
            **kwargs
    ) -> None:

        self.concurrency = concurrency
        self.downloader = downloader or cffi.Downloader()
        self.task_queue = LocalQueue() if not task_queue else task_queue
        self.proxy = proxy
        self.queue_name = queue_name or f'{self.__class__.__module__}.{self.__class__.__name__}'
        self.forever = forever
        super().__init__(**kwargs)
        self.number_of_total_requests = FastWriteCounter()  # 发起请求总数量
        self.number_of_failure_requests = FastWriteCounter()  # 发起请求失败数量

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

        context = self.make_context(
            task_queue=task_queue,
            queue_name=queue_name,
            _Context=self.InitContext,
            settings=settings
        )
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
            stuff: InitContext = context.copy()
            stuff.seeds = seeds
            stuff.flow({"next": self.produce_seeds})
            self.on_consume(stuff)

        record.update(finish=str(datetime.datetime.now()))
        task_queue.command(queue_name, {"action": task_queue.COMMANDS.RELEASE_INIT, "time": int(time.time() * 1000)})
        return record

    def produce_seeds(self, context: InitContext):
        """
        生产种子

        :param context:
        :return:
        """
        settings: dict = context.obtain("settings")

        seeds = context.seeds
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
            annotations={InitContext: context},
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

    def _when_put_seeds(self, raw_method):  # noqa
        @functools.wraps(raw_method)
        def wrapper(*args, **kwargs):
            context: InitContext = InitContext.get_context()
            context.form = state.const.BEFORE_PUT_SEEDS
            events.EventManager.invoke(context)
            prepared = pandora.prepare(
                func=raw_method,
                args=args,
                kwargs=kwargs,
                annotations={InitContext: context},
                namespace={"context": context}
            )
            ret = prepared.func(*prepared.args, **prepared.kwargs)
            context.form = state.const.AFTER_PUT_SEEDS
            events.EventManager.invoke(context)
            return ret

        return wrapper

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
            context: Context = self.make_context(
                task_queue=task_queue,
                queue_name=queue_name
            )
            with context:
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
                            self.dispatcher.running == 0 and
                            task_queue.is_empty(queue_name, threshold=self.get("spider.threshold", default=0))
                    ):

                        number_of_total_requests = self.number_of_total_requests.value
                        number_of_failure_requests = self.number_of_failure_requests.value
                        number_of_success_requests = number_of_total_requests - number_of_failure_requests
                        if number_of_total_requests:
                            rate_of_success_requests = round(
                                number_of_success_requests / number_of_total_requests * 100, 2
                            )
                        else:
                            rate_of_success_requests = 0

                        logger.debug(
                            f'[爬取完毕] '
                            f'队列名称: {queue_name} '
                            f'关闭阈值: {self.get("spider.threshold", default=0)} '
                            f'提交种子的数量: {count} '
                            f'总请求的数量: {number_of_total_requests} '
                            f'请求成功率: {rate_of_success_requests}% '
                        )

                        return {
                            "seeds_count": count,
                            "number_of_total_requests": number_of_total_requests,
                            "number_of_failure_requests": number_of_failure_requests,
                            "number_of_success_requests": number_of_success_requests,
                            "request_success_rate": rate_of_success_requests,

                        }

                    else:

                        if task_queue.smart_reverse(queue_name, status=self.dispatcher.running):
                            logger.debug(f"[翻转队列] 队列名称: {queue_name}")

                        else:
                            if time.time() - output > 60:
                                logger.debug(f"[等待任务] 队列名称: {queue_name}")
                                output = time.time()

                            time.sleep(1)

                else:
                    for seeds in pandora.iterable(fettle):
                        stuff = context.copy()
                        stuff.flow({"next": self.on_consume, "seeds": seeds})
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
            future = self.active(init_task)
            task_queue: TaskQueue = self.get("init.task_queue", self.task_queue)
            queue_name: str = self.get("init.queue_name", self.queue_name)
            task_queue.command(queue_name, {"action": task_queue.COMMANDS.WAIT_INIT, "time": t1})
            return {
                "spider": self.run_spider(),
                "init": future.result()
            }

    @staticmethod
    def get_seeds(**kwargs) -> Union[Iterable[Item], Item]:
        """
        获取种子的真实方法

        :return:
        """
        context: Context = Context.get_context()
        task_queue: TaskQueue = kwargs.pop('task_queue', None) or context.task_queue
        queue_name: str = kwargs.pop('queue_name', None) or context.queue_name
        return task_queue.get(name=queue_name, **kwargs)

    def _when_get_seeds(self, raw_method):
        @functools.wraps(raw_method)
        def wrapper(**kwargs):
            context: Context = Context.get_context()
            context.form = state.const.BEFORE_GET_SEEDS
            events.EventManager.invoke(context)
            count = self.dispatcher.max_workers - self.dispatcher.running
            kwargs.setdefault('count', 1 if count <= 0 else count)
            prepared = pandora.prepare(
                func=raw_method,
                kwargs=kwargs,
                annotations={Context: context},
                namespace={"context": context}
            )
            ret = prepared.func(*prepared.args, **prepared.kwargs)
            # 没有种子返回空信号
            if ret is None: raise signals.Empty
            context.seeds = ret
            context.form = state.const.AFTER_GET_SEEDS
            events.EventManager.invoke(context)

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
        self.number_of_failure_requests.increment()
        request: Request = context.request
        response: Response = context.response

        if request.retry < request.max_retry - 1:

            # 如果是代理错误, 则不计算重试次数
            if not IGNORE_RETRY_PATTERN.search(response.error):
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

            if request.retry > request.get_options("$maxRetry", math.inf):
                raise signals.Success

            else:
                raise signals.Failure

    def _when_on_retry(self, raw_method):  # noqa
        @functools.wraps(raw_method)
        def wrapper(context: Context, *args, **kwargs):
            context.form = state.const.BEFORE_RETRY
            self.number_of_failure_requests.increment()
            prepared = pandora.prepare(
                func=raw_method,
                args=args,
                kwargs=kwargs,
                annotations={
                    self.Context: context,
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

        context.flow({"response": response})
        return response

    def _when_on_request(self, raw_method):  # noqa
        @functools.wraps(raw_method)
        def wrapper(context: Context, *args, **kwargs):
            context.form = state.const.BEFORE_REQUEST
            events.EventManager.invoke(context)
            context.form = state.const.ON_REQUEST
            self.number_of_total_requests.increment()
            pandora.invoke(
                func=raw_method,
                args=args,
                kwargs=kwargs,
                annotations={
                    self.Context: context,
                    Item: context.seeds,
                    Request: context.request,
                },
                namespace={
                    "context": context,
                    "seeds": context.seeds,
                    "request": context.request
                }
            )
            context.form = state.const.AFTER_REQUEST
            events.EventManager.invoke(context)

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

        context.flow({"items": items})
        return items

    def _when_on_response(self, raw_method):  # noqa
        @functools.wraps(raw_method)
        def wrapper(context: Context, *args, **kwargs):
            context.form = state.const.ON_PARSE
            pandora.invoke(
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

        context.flow()

    def _when_on_pipeline(self, raw_method):  # noqa
        @functools.wraps(raw_method)
        def wrapper(context: Context, *args, **kwargs):
            context.form = state.const.BEFORE_PIPELINE
            events.EventManager.invoke(context)
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
            events.EventManager.invoke(context)

        return wrapper

    def make_seeds(self, context: Context, **kwargs):
        raise NotImplementedError

    def make_request(self, context: Context) -> Request:
        raise NotImplementedError

    def parse(self, context: Context) -> Union[List[dict], Items]:
        raise NotImplementedError

    def item_pipeline(self, context: Context):
        context.items and logger.debug(context.items)

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
            {"func": on_request.After.is_success}
        )

    @pandora.Method
    def survey(
            binding,  # noqa
            *seeds: dict,
            attrs: dict =
            None, modded:
            dict = None,
            extract: list = None
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
        survey: Spider = clazz(**attrs)
        survey.run()
        return list(collect.queue) if not extract else [{k: getattr(c, k) for k in extract} for c in collect.queue]
