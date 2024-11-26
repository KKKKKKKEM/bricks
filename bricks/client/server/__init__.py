# -*- coding: utf-8 -*-
# @Time    : 2023-12-12 14:47
# @Author  : Kem
# @Desc    : 客户端, 让 bricks 支持 api 调用
import asyncio
import collections
import inspect
import json
import threading
import time
from concurrent.futures import Future
from concurrent.futures import ThreadPoolExecutor
from typing import Union, Literal, Callable, Dict, List

from loguru import logger

from bricks import Response
from bricks.core import signals
from bricks.spider.addon import Rpc, Listener
from bricks.spider.air import Context
from bricks.utils import pandora


class Callback:
    @staticmethod
    def build(fns, request=None, **kwargs):
        def main(fu: asyncio.Future):
            if not fns or fu.cancelled():
                return

            namespace = {"fu": fu, "request": request}

            annotations = {
                type(fu): fu,
                type(request): request,
            }
            raw_req = request.get_options("$request")

            retval = fu.result()

            namespace.update({"context": retval})
            namespace.update({"retval": retval})
            annotations.update({type(raw_req): raw_req, type(retval): retval})

            for fn in pandora.iterable(fns):
                if callable(fn) and not inspect.iscoroutinefunction(fn):
                    prepared = pandora.prepare(
                        fn, kwargs=kwargs, namespace=namespace, annotations=annotations
                    )
                    try:
                        prepared.func(*prepared.args, **prepared.kwargs)
                    except signals.Break:
                        return

        return main

    @staticmethod
    async def call(fns, fu: asyncio.Future, request=None, **kwargs):
        if not fns:
            return

        namespace = {"fu": fu, "request": request}

        annotations = {
            type(fu): fu,
            type(request): request,
        }
        raw_req = request.get_options("$request")

        if not fu.cancelled():
            ctx = fu.result()

        else:
            ctx = None

        namespace.update({"context": ctx})
        annotations.update({type(raw_req): raw_req, type(ctx): ctx})

        for fn in pandora.iterable(fns):
            if callable(fn):
                prepared = pandora.prepare(
                    fn, kwargs=kwargs, namespace=namespace, annotations=annotations
                )
                try:
                    if inspect.iscoroutinefunction(fn):
                        await prepared.func(*prepared.args, **prepared.kwargs)
                    else:
                        await Gateway.awaitable_call(
                            prepared.func, *prepared.args, **prepared.kwargs
                        )
                except signals.Break:
                    return


class Gateway:
    def __init__(self):
        self.connections = {}
        self.router = None
        self._futures = collections.defaultdict(asyncio.Future)

    @staticmethod
    async def awaitable_call(func, *args, **kwargs):
        def sync2future():
            def callback():
                try:
                    ret = func(*args, **kwargs)
                    future.set_result(ret)
                except (SystemExit, KeyboardInterrupt):
                    raise
                except BaseException as exc:
                    if future.set_running_or_notify_cancel():
                        future.set_exception(exc)
                    raise

            future = Future()
            threading.Thread(target=callback, daemon=True).start()
            return future

        if inspect.iscoroutinefunction(func):
            return await func(*args, **kwargs)
        else:
            fu = sync2future()
            return await asyncio.wrap_future(fu)

    def create_addon(self, uri: str, adapter: Callable, **options):
        """
        创建绑定一个 addon 视图

        :param uri: 路径
        :param adapter: 主函数
        :param options: 其他选项
        :return:
        """
        ...

    def route(
        self,
        uri: str,
        timeout: int = None,
        concurrency: int = None,
        callback: List[Callable] = None,
        errback: List[Callable] = None,
        methods: List[str] = ["GET"],
        **options,
    ):
        """
        绑定一个路由
        :param uri: 路径
        :param timeout: 超时时间
        :param concurrency: 并发数
        :param callback: 成功回调
        :param errback: 失败回调
        :param methods: 请求方法
        :param options: 其他选项
        :return:

        """

        def inner(func):
            return self.bind_view(
                path=uri,
                handler=func,
                timeout=timeout,
                concurrency=concurrency,
                callback=callback,
                errback=errback,
                methods=methods,
                **options,
            )

        return inner

    def bind_view(
        self,
        path: str,
        handler: Callable,
        timeout: int = None,
        concurrency: int = None,
        callback: List[Callable] = None,
        errback: List[Callable] = None,
        **options,
    ) -> Callable:
        """
        绑定一个路由
        """

        async def normal_view(request=None, is_alive: Callable = None):
            if semaphore and semaphore.locked():
                raise signals.Wait(1)

            semaphore and await semaphore.acquire()

            async def monitor():
                if not is_alive:
                    return

                if not request:
                    return
                raw_req = request.get_options("$request")
                while not fu.done():
                    alive = await is_alive(raw_req)
                    if not alive:
                        fu.cancel()
                    else:
                        await asyncio.sleep(0.01)

            asyncio.ensure_future(coro_or_future=monitor())
            prepared = pandora.prepare(
                handler,
                kwargs=options,
                namespace={"request": request},
                annotations={type(request): request},
            )
            fu = asyncio.ensure_future(
                self.awaitable_call(prepared.func, *prepared.args, **prepared.kwargs)
            )
            fu.add_done_callback(Callback.build(cb1, request=request))

            try:
                ctx = await asyncio.wait_for(fu, timeout=timeout)

            except asyncio.exceptions.CancelledError:
                await Callback.call(errback, fu, request=request)

            except Exception as e:
                if isinstance(e, asyncio.TimeoutError):
                    raise asyncio.TimeoutError(f"任务超过最大超时时间 {timeout} s")
                else:
                    raise e
            else:
                await Callback.call(cb2, fu, request=request)
                return ctx

            finally:
                semaphore and semaphore.release()

        cb1 = []
        cb2 = []
        for cb in pandora.iterable(callback):
            if inspect.iscoroutinefunction(cb):
                cb2.append(cb)
            else:
                cb1.append(cb)

        if concurrency and concurrency > 0:
            semaphore = asyncio.Semaphore(concurrency)
        else:
            semaphore = None

        self.create_view(uri=path, adapter=normal_view, **options)

    def bind_addon(
        self,
        obj: Union[Rpc, Listener],
        path: str,
        form: Literal["$response", "$items", "$request"] = "$response",
        max_retry: int = 10,
        timeout: int = None,
        concurrency: int = None,
        callback: List[Callable] = None,
        errback: List[Callable] = None,
        **options,
    ):
        """
        绑定 listener / rpc

        :param errback: 错误回调, 如被取消, 或者出现异常之类的
        :param callback: 成功回调, 支持同步回调和异步回调
        :param timeout: 接口超时时间， 到了这个时间 还没有跑完，直接抛出 timeout
        :param concurrency: 接口并发数量，超出该数量时会返回 429
        :param form: 接口返回类型, 要在响应完了就返回就填 $response，要在解析完就返回就填 $items，要在请求前就返回就填 $request
        :param max_retry: 种子最大重试次数
        :param obj: 需要绑定的 Rpc
        :param path: 访问路径
        :return:
        """

        def is_failure(context: Context):
            return context and context.seeds.get("$status", 0) != 0

        def fix(context: Context):
            context.seeds.update({"$interfaceFinish": time.time()})
            if is_failure(context):
                context.response = Response(
                    status_code=403,
                    content=json.dumps(
                        {
                            "code": 403,
                            "msg": context.seeds.get("$msg"),
                            "data": {
                                k.strip("$"): v
                                for k, v in sorted(context.seeds.items())
                                if k not in ["$status", "$msg"]
                            },
                        }
                    ),
                    headers={"Content-type": "application/json"},
                )

                raise signals.Break

            if form == "$response" and getattr(context, "response", None) is None:
                context.response = Response(
                    status_code=204,
                    content="",
                    headers={"Content-type": "application/json"},
                )

        async def submit(seeds: dict, request=None, is_alive: Callable = None):
            if semaphore and semaphore.locked():
                raise signals.Wait(1)

            semaphore and await semaphore.acquire()

            async def monitor():
                if not is_alive:
                    return

                if not request:
                    return
                raw_req = request.get_options("$request")
                while not fu.done():
                    alive = await is_alive(raw_req)
                    if not alive:
                        logger.warning(f"{data} 被取消")
                        fu.cancel()
                    else:
                        await asyncio.sleep(0.01)

            loop = asyncio.get_event_loop()
            seeds = seeds or {}
            data = {
                **seeds,
                "$futureType": form,
                "$futureMaxRetry": max_retry,
                "$interfaceStart": time.time(),
            }
            asyncio.ensure_future(monitor())
            if "timeout" in inspect.signature(obj.execute).parameters:
                args = [data, timeout]
            else:
                args = [data]

            fu = loop.run_in_executor(pool, obj.execute, *args)
            fu.add_done_callback(Callback.build(cb1, request=request, seeds=seeds))
            ctx = None

            try:
                ctx = await asyncio.wait_for(fu, timeout=timeout)
                assert not is_failure(ctx), "超出最大重试次数"
            except AssertionError:
                await Callback.call(errback, fu, request=request, seeds=seeds)
                return ctx

            except asyncio.exceptions.CancelledError:
                await Callback.call(errback, fu, request=request, seeds=seeds)

            except Exception as e:
                if isinstance(e, asyncio.TimeoutError):
                    raise asyncio.TimeoutError(f"任务超过最大超时时间 {timeout} s")
                else:
                    raise e
            else:
                await Callback.call(cb2, fu, request=request, seeds=seeds)
                return ctx

            finally:
                semaphore and semaphore.release()

        cb1 = [fix]
        cb2 = []
        for cb in pandora.iterable(callback):
            if inspect.iscoroutinefunction(cb):
                cb2.append(cb)
            else:
                cb1.append(cb)

        pool = ThreadPoolExecutor(max_workers=concurrency)
        if concurrency and concurrency > 0:
            obj.spider.dispatcher.max_workers = concurrency
            semaphore = asyncio.Semaphore(concurrency)
        else:
            semaphore = None

        not obj.running and obj.run()
        self.create_addon(uri=path, adapter=submit, **options)

    async def _invoke(self, orders: Dict[str, List[dict]], timeout: int = None):
        futures = []
        for ws, cid in self.connections.items():
            if cid in orders:
                future = asyncio.Future()
                future_id = f"future-{id(future)}"
                self._futures[future_id] = future
                ctx = {"MID": future_id, "CID": cid, "CTX": orders[cid]}
                await ws.send(json.dumps(ctx, default=str))
                futures.append(future)

        else:
            ret = []
            for fu in asyncio.as_completed(futures, timeout=timeout):
                ctx = await fu
                ret.append(ctx)

            return ret

    async def invoke(
        self,
        orders: Dict[str, List[dict]],
        timeout: int = None,
    ):
        future = asyncio.ensure_future(self._invoke(orders, timeout=timeout))
        if timeout == 0:
            ret = {"code": 0, "msg": "成功提交任务"}

        else:
            try:
                await future
                ret = {"code": 0, "msg": "任务执行成功", "result": future.result()}
            except asyncio.TimeoutError:
                ret = {"code": 1, "msg": "任务执行超时"}

        return ret

    def run(self, *args, **kwargs): ...

    async def websocket_endpoint(self, *args, **kwargs): ...

    def add_route(self, *args, **kwargs):
        return self.router.add_route(*args, **kwargs)

    def add_websocket_route(self, *args, **kwargs):
        return self.router.add_websocket_route(*args, **kwargs)

    def add_middleware(self, *args, **kwargs): ...
