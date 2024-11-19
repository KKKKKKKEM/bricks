# -*- coding: utf-8 -*-
# @Time    : 2023-12-12 14:47
# @Author  : Kem
# @Desc    : 客户端, 让 bricks 支持 api 调用
import asyncio
import collections
import inspect
import json
import threading
from concurrent.futures import Future
from concurrent.futures import ThreadPoolExecutor
from typing import Union, Literal, Callable, Dict, List

from loguru import logger

from bricks.spider.addon import Rpc, Listener
from bricks.utils import pandora


class TooManyRequestsError(Exception):
    ...


class Callback:

    @staticmethod
    def build(fns, request=None, **kwargs):
        def main(fu: asyncio.Future, ):
            if not fns or fu.cancelled():
                return

            namespace = {
                "fu": fu,
                "request": request
            }

            annotations = {
                type(fu): fu,
                type(request): request,
            }
            raw_req = request.get_options("$request")

            ctx = fu.result()

            namespace.update({"context": ctx})
            annotations.update({type(raw_req): raw_req, type(ctx): ctx})

            for fn in pandora.iterable(fns):
                if callable(fn) and not inspect.iscoroutinefunction(fn):
                    prepared = pandora.prepare(
                        fn,
                        kwargs=kwargs,
                        namespace=namespace,
                        annotations=annotations
                    )
                    prepared.func(*prepared.args, **prepared.kwargs)

        return main

    @staticmethod
    async def call(fns, fu: asyncio.Future, request=None, **kwargs):
        if not fns:
            return

        namespace = {
            "fu": fu,
            "request": request
        }

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
                    fn,
                    kwargs=kwargs,
                    namespace=namespace,
                    annotations=annotations
                )
                if inspect.iscoroutinefunction(fn):
                    await prepared.func(*prepared.args, **prepared.kwargs)
                else:
                    await Gateway.awaitable_call(prepared.func, *prepared.args, **prepared.kwargs)


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

    def create_view(self, uri: str, adapter: Callable, options: dict = None):
        """
        创建绑定一个视图

        :param uri: 路径
        :param adapter: 主函数
        :param options: 其他选项
        :return:
        """
        ...

    def bind_addon(
            self,
            obj: Union[Rpc, Listener],
            path: str,
            form: Literal['$response', '$items', '$request'] = '$response',
            max_retry: int = 10,
            timeout: int = None,
            concurrency: int = None,
            callback: List[Callable] = None,
            errback: List[Callable] = None,
            **options
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

        async def submit(seeds: dict, request=None, is_alive: Callable = None):
            if semaphore and semaphore.locked():
                raise TooManyRequestsError

            semaphore and await semaphore.acquire()

            async def monitor():
                if not is_alive:
                    return

                if not request:
                    return

                while not fu.done():
                    alive = await is_alive(request.get_options("$request"))
                    if not alive:
                        logger.warning(f'{data} 被取消')
                        fu.cancel()

            loop = asyncio.get_event_loop()
            data = {
                **seeds,
                "$futureType": form,
                "$futureMaxRetry": max_retry
            }
            asyncio.ensure_future(monitor())
            fu = loop.run_in_executor(pool, obj.execute, data, timeout)
            fu.add_done_callback(Callback.build(cb1, request=request, data=data))
            try:
                ctx = await asyncio.wait_for(fu, timeout=timeout)
                await Callback.call(cb2, fu, request=request, data=data)
                return ctx

            except asyncio.exceptions.CancelledError:
                await Callback.call(errback, fu, request=request, data=data)

            except Exception as e:
                await Callback.call(errback, fu, request=request, data=data)
                raise e

            finally:
                semaphore and semaphore.release()

        cb1 = []
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
        self.create_view(uri=path, adapter=submit, options=options)

    async def _invoke(self, orders: Dict[str, List[dict]], timeout: int = None):
        futures = []
        for ws, cid in self.connections.items():
            if cid in orders:
                future = asyncio.Future()
                future_id = f'future-{id(future)}'
                self._futures[future_id] = future
                ctx = {
                    "MID": future_id,
                    "CID": cid,
                    "CTX": orders[cid]
                }
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
            ret = {
                "code": 0,
                "msg": "成功提交任务"
            }

        else:

            try:
                await future
                ret = {
                    "code": 0,
                    "msg": "任务执行成功",
                    "result": future.result()
                }
            except asyncio.TimeoutError:
                ret = {
                    "code": 1,
                    "msg": "任务执行超时"
                }

        return ret

    def run(self, *args, **kwargs):
        ...

    async def websocket_endpoint(self, *args, **kwargs):
        ...

    def add_route(self, *args, **kwargs):
        return self.router.add_route(*args, **kwargs)

    def add_websocket_route(self, *args, **kwargs):
        return self.router.add_websocket_route(*args, **kwargs)
