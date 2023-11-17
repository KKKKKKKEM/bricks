# -*- coding: utf-8 -*-
# @Time    : 2023-11-15 14:09
# @Author  : Kem
# @Desc    :
import datetime
import functools
import inspect
import math
import re
import time
from typing import Optional, List, Union, Iterable, Callable

from loguru import logger

from bricks import const
from bricks.core import dispatch, signals, events
from bricks.core.genesis import Pangu
from bricks.downloader import genesis, cffi
from bricks.lib.context import Flow
from bricks.lib.counter import FastWriteCounter
from bricks.lib.items import Items
from bricks.lib.queues import TaskQueue, LocalQueue, Item
from bricks.lib.request import Request
from bricks.lib.response import Response
from bricks.utils import pandora

IP_REGEX = re.compile(r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}:\d+')


class Context(Flow):
    target: "Spider"
    request: "Request"
    response: "Response"
    seeds: "Item"
    items: "Items"
    task_queue: "TaskQueue"
    queue_name: "str"

    def __init__(
            self,
            target,
            form: str = const.ON_CONSUME,
            **kwargs

    ) -> None:
        super().__init__(form, target, **kwargs)

    response: Response = property(
        fget=lambda self: getattr(self, "_response", None),
        fset=lambda self, value: setattr(self, "_response", pandora.ensure_type(value, Response)),
        fdel=lambda self: setattr(self, "_response", None),
        doc="rtype: Response"
    )

    request: Request = property(
        fget=lambda self: getattr(self, "_request", None),
        fset=lambda self, value: setattr(self, "_request", pandora.ensure_type(value, Request)),
        fdel=lambda self: setattr(self, "_request", None)
    )
    seeds: Item = property(
        fget=lambda self: getattr(self, "_seeds", None),
        fset=lambda self, value: setattr(self, "_seeds", pandora.ensure_type(value, Item)),
        fdel=lambda self: setattr(self, "_seeds", None)
    )

    items: Items = property(
        fget=lambda self: getattr(self, "_items", None),
        fset=lambda self, value: setattr(self, "_items", pandora.ensure_type(value, Items)),
        fdel=lambda self: setattr(self, "_items", None)
    )

    task_queue: TaskQueue = property(
        fget=lambda self: getattr(self, "_task_queue", None),
        fset=lambda self, value: setattr(self, "_task_queue", value),
        fdel=lambda self: setattr(self, "_task_queue", None)
    )

    queue_name: str = property(
        fget=lambda self: getattr(self, "_queue_name", None),
        fset=lambda self, value: setattr(self, "_queue_name", value),
        fdel=lambda self: setattr(self, "_queue_name", None)
    )

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


class Spider(Pangu):
    task_queue: TaskQueue
    queue_name: str
    downloader: genesis.Downloader

    def __init__(
            self,
            concurrency: Optional[int] = 1,
            survey: Optional[Union[dict, List[dict]]] = None,
            downloader: Optional[Union[str, genesis.Downloader]] = None,
            task_queue: Optional[TaskQueue] = None,
            queue_name: Optional[str] = "",
            proxy: Optional[str] = "",
            **kwargs
    ) -> None:
        settings = {
            "concurrency": concurrency,
            "survey": survey,
            "downloader": downloader or cffi.Downloader(),
            "task_queue": LocalQueue() if not task_queue or survey else task_queue,
            "proxy": proxy,
            "queue_name": queue_name or self.__class__.__name__,
        }
        super().__init__(**settings, **kwargs)

        self._total_number_of_requests = FastWriteCounter()  # 发起请求总数量
        self._number_of_failure_requests = FastWriteCounter()  # 发起请求失败数量

    def run_init(self):
        """
        初始化
        :return:
        """

        task_queue: TaskQueue = self.get_attr("init.task_queue", self.task_queue)
        queue_name: str = self.get_attr("init.queue_name", self.queue_name)
        task_queue.command(queue_name, {"action": "set-init-start"})
        task_queue.command(queue_name, {"action": "set-init-status"})

        try:
            # 本地的初始化记录 -> 启动传入的
            local_init_record: dict = self.get_attr('init.record') or {}
            # 云端的初始化记录 -> 初始化的时候会存储(如果支持的话)
            remote_init_record = task_queue.command(queue_name, {"action": "get-init-record"}) or {}

            # 初始化记录信息
            init_record: dict = {
                **local_init_record,
                **remote_init_record,
                "queue_name": queue_name,
                "task_queue": task_queue,
                "identifier": const.MACHINE_ID,
            }

            # 设置一个启动时间, 防止被覆盖
            init_record.setdefault("start", str(datetime.datetime.now()))

            # 获取已经初始化的总量
            total = int(init_record.setdefault('total', 0))
            # 获取已经初始化的去重数量
            success = int(init_record.setdefault('success', 0))
            # 获取机器的唯一标识
            identifier = const.MACHINE_ID

            # 判断是否有初始化权限
            has_permission = task_queue.command(queue_name, {"action": "get-permission", "identifier": identifier})
            if not has_permission:
                return

            # 初始化模式: ignore/reset/continue
            init_mode = self.get_attr('init.mode')
            # 初始化总数量阈值 -> 大于这个数量停止初始化
            init_total_size = self.get_attr('init.total.size', math.inf)
            # 当前初始化总量阈值 -> 大于这个数量停止初始化
            init_count_size = self.get_attr('init.count.size', math.inf)
            # 初始化成功数量阈值 (去重) -> 大于这个数量停止初始化
            init_success_size = self.get_attr('init.success.size', math.inf)
            # 初始化队列最大数量 -> 大于这个数量暂停初始化
            init_queue_size: int = self.get_attr("init.queue.size", 100000)

            # 当前已经初始化的数量, 从 0 开始
            count = 0
            counter = {
                "total": total,
                "success": success,
                "count": count,
            }

            if init_mode == 'ignore':
                # 忽略模式, 什么都不做
                logger.debug("[停止投放] 忽略模式, 不进行种子初始化")
                return init_record

            elif init_mode == 'reset':
                # 重置模式, 清空初始化记录和队列种子
                task_queue.command(queue_name, {"action": "reset-init-record"})
                logger.debug("[开始投放] 重置模式, 清空初始化队列及记录")

            elif init_mode == 'continue':
                # 继续模式, 从上次初始化的位置开始
                logger.debug("[开始投放] 继续模式, 从上次初始化的位置开始")
                task_queue.command(queue_name, {"action": "continue-init-record"})

            else:
                # 默认模式, 队列内种子数量小于等于阈值, 且 get-permission 成功才进行初始化
                init_threshold = self.get_attr('init.threshold', 0)
                if task_queue.size(queue_name) <= init_threshold:
                    logger.debug("[开始投放] 默认模式, 开始投放")
                else:
                    logger.debug(f"[停止投放] 默认模式, 队列内种子大于阈值: {init_threshold}")

            def generator(context: Context):
                for seeds in pandora.invoke(
                        func=self.make_seeds,
                        kwargs={"record": init_record},
                        annotations={Context: context},
                        namespace={'context': context},
                ):
                    seeds = pandora.iterable(seeds)

                    seeds = seeds[0:min([
                        init_count_size - count,
                        init_total_size - total,
                        init_success_size - success,
                        len(seeds)
                    ])]

                    context.seeds = seeds
                    yield seeds
                else:
                    context.flow({"next": None})

            def consumer(seeds, context: Context = None):
                fettle = pandora.invoke(
                    func=self.put_seeds,
                    kwargs={
                        "seeds": seeds,
                        "maxsize": init_queue_size,
                        "where": "init",
                        "task_queue": task_queue,
                        "queue_name": queue_name,

                    },
                    annotations={Context: context},
                    namespace={"context": context},
                )

                size = len(pandora.iterable(seeds))
                counter['total'] += size
                counter['success'] += fettle
                counter['count'] += fettle
                output = f"[投放成功] 总量: {counter['success']}/{counter['total']}; 当前: {fettle}/{size}; 目标: {queue_name}"
                init_record.update({
                    "total": counter['total'],
                    "success": counter['success'],
                    "output": output,
                    "update": str(datetime.datetime.now()),
                })

                task_queue.command(queue_name, {"action": "set-init-record", "record": init_record})
                logger.debug(output)

                if (
                        counter['total'] >= init_total_size or
                        counter['count'] >= init_count_size or
                        counter['success'] >= init_success_size
                ):
                    raise signals.Exit

            context_ = self.get_context(task_queue=task_queue, queue_name=queue_name)
            context_.flow({"next": generator, "callback": consumer})
            self.on_consume(context_)
            init_record.update(finish=str(datetime.datetime.now()))
            task_queue.command(queue_name, {"action": "backup-init-record", "record": init_record})
            return init_record

        finally:
            task_queue.command(queue_name, {"action": "release-init-status"})

    def put_seeds(self, context: Context = None, **kwargs):
        """
        将种子放入容器

        :param context:
        :param kwargs:
        :return:
        """

        if context:
            task_queue = context.task_queue
            queue_name = context.queue_name
            seeds = context.seeds

        else:
            task_queue: TaskQueue = kwargs.pop('task_queue', None) or self.task_queue
            queue_name: str = kwargs.pop('queue_name', None) or self.queue_name
            seeds = kwargs.pop('seeds', {})

        maxsize = kwargs.pop('maxsize', None)
        priority = kwargs.pop('priority', False)
        task_queue.continue_(queue_name, maxsize=maxsize, interval=1)
        return task_queue.put(queue_name, *pandora.iterable(seeds), priority=priority, **kwargs)

    def get_context(self, **kwargs) -> Context:
        flows = {
            self.on_consume: self.on_seeds,
            self.on_seeds: self.on_request,
            self.on_request: self.on_response,
            self.on_response: self.on_pipline,
            self.on_pipline: None
        }
        context = Context(target=self, flows=flows, **kwargs)
        return context

    def run_spider(self):
        """
        start run spider
        get seeds and convert it to Request for submit

        :return:
        """
        count = 0
        self.dispatcher.start()
        task_queue: TaskQueue = self.get_attr("spider.task_queue", self.task_queue)
        queue_name: str = self.get_attr("spider.queue_name", self.queue_name)
        output = time.time()

        while True:
            context = self.get_context(
                task_queue=task_queue,
                queue_name=queue_name,
            )
            prepared = pandora.prepare(
                func=self.get_seeds,
                annotations={Context: context},
                namespace={"context": context}
            )
            try:
                fettle = prepared.func(*prepared.args, **prepared.kwargs)

            except signals.Wait:
                time.sleep(1)

            except signals.Success:

                if context.form == const.BEFORE_GET_SEEDS:
                    time.sleep(1)

                elif context.form == const.AFTER_GET_SEEDS:
                    context.success()

                else:
                    raise

            except signals.Failure:

                if context.form == const.BEFORE_GET_SEEDS:
                    time.sleep(1)

                elif context.form == const.AFTER_GET_SEEDS:
                    pass

                else:
                    raise

            except signals.Empty:
                # 判断是否应该停止爬虫
                #  没有初始化 + 本地没有运行的任务 + 任务队列为空 -> 退出
                if (
                        not task_queue.command(queue_name, {"action": "is-init-status"}) and
                        # not self.forever and
                        task_queue.is_empty(queue_name, threshold=self.get_attr("spider.threshold", default=0))
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
                    context = self.get_context(
                        seeds=seeds,
                        task_queue=task_queue,
                        queue_name=queue_name,
                    )
                    context.flow({"next": self.on_consume})
                    self.submit(dispatch.Task(context.next, [context]))
                    count += 1

    def run_all(self):
        self.dispatcher.start()
        init_task = dispatch.Task(func=self.run_init)
        self.active(init_task)
        task_queue: TaskQueue = self.get_attr("init.task_queue", self.task_queue)
        queue_name: str = self.get_attr("init.queue_name", self.queue_name)
        task_queue.command(queue_name, {"action": "wait-for-init-start"})
        task_queue.command(queue_name, {"action": "release-init-start"})
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
            context.form = const.BEFORE_GET_SEEDS
            events.Event.invoke(context)
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

            context.form = const.AFTER_GET_SEEDS
            events.Event.invoke(context)

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

            elif "proxyerror" in str(response.error).lower():
                request.retry += 1

            if '$retry' not in options:
                pass
                # request.clear_proxies()

            else:
                options.pop('$retry', None)

            if IP_REGEX.match((str(request.proxy) or "")):
                request.proxy = ""

        else:
            indent = '\n            '
            header = f"\n{'=' * 25} \033[33m[WARNING]\033[0m {'=' * 25}"
            msg = [
                "",
                f"请求已经达到了最大重试次数,依旧请求失败",
                f"seeds: {context.seeds}",
                f"url: {request.real_url}",
                ""
            ]
            logger.warning(f'{header}\n{indent.join(msg)}{header}')

            if request.retry > options.get('$requestRetry', math.inf):
                raise signals.Success

            else:
                raise signals.Failure

    def _when_on_retry(self, raw_method):  # noqa
        @functools.wraps(raw_method)
        def wrapper(context: Context, *args, **kwargs):
            context.form = const.BEFORE_RETRY
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
            context.flow({"next": self.on_request})
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
            context.form = const.BEFORE_REQUEST
            events.Event.invoke(context)

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

            context.form = const.AFTER_REQUEST
            events.Event.invoke(context)
            context.flow({"response": response})

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
            context.form = const.ON_PARSING
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

    def on_pipline(self, context: Context):
        """
        管道中

        :param context:
        :return:
        """
        items: Items = context.items
        callback: Callable = items.callback or self.item_pipline
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

    def _when_on_pipline(self, raw_method):  # noqa
        @functools.wraps(raw_method)
        def wrapper(context: Context, *args, **kwargs):
            context.form = const.BEFORE_PIPLINE
            events.Event.invoke(context)

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
            response: Response = prepared.func(*prepared.args, **prepared.kwargs)
            context.response = response
            context.form = const.AFTER_PIPLINE
            events.Event.invoke(context)
            context.flow()

        return wrapper

    def make_seeds(self, context: Context, **kwargs):
        raise NotImplementedError

    def make_request(self, context: Context) -> Request:
        raise NotImplementedError

    def parse(self, context: Context):
        raise NotImplementedError

    def item_pipline(self, context: Context):
        context.items and logger.debug(context.items)
