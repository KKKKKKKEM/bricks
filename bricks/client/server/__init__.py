# -*- coding: utf-8 -*-
# @Time    : 2023-12-12 14:47
# @Author  : Kem
# @Desc    : 客户端, 让 bricks 支持 api 调用
import asyncio
import collections
import inspect
import json
import threading
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any, Callable, Dict, List, Literal, Optional

from bricks import Request
from bricks.core import signals
from bricks.spider.addon import Rpc
from bricks.utils import pandora


class Gateway:
    def __init__(self):
        self.connections = {}
        self.router = ...
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
            timeout: int = None,  # type: ignore
            concurrency: int = None,  # type: ignore
            callback: List[Callable] = None,  # type: ignore
            errback: List[Callable] = None,  # type: ignore
            methods: List[str] = ("GET",),
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
            timeout: int = None,  # type: ignore
            concurrency: int = None,  # type: ignore
            callback: List[Callable] = None,  # type: ignore
            errback: List[Callable] = None,  # type: ignore
            **options,
    ):
        """
        绑定一个路由
        """

        async def normal_view(request: Request = ..., is_alive: Callable = ...):
            if semaphore and semaphore.locked():
                raise signals.Wait(1)

            if semaphore:
                await semaphore.acquire()

            async def monitor():
                if is_alive is ...:
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

            try:
                result = await asyncio.wait_for(fu, timeout=timeout)

            except asyncio.exceptions.CancelledError:
                await self.run_callbacks(errback, fu, request=request)

            except Exception as e:
                if isinstance(e, asyncio.TimeoutError):
                    raise asyncio.TimeoutError(f"任务超过最大超时时间 {timeout} s")
                else:
                    raise e
            else:
                await self.run_callbacks(callback, fu, request=request)
                return result

            finally:
                semaphore and semaphore.release()  # type: ignore

        if concurrency and concurrency > 0:
            semaphore = asyncio.Semaphore(concurrency)
        else:
            semaphore = None

        self.create_view(uri=path, adapter=normal_view, **options)

    def bind_addon(
            self,
            obj: Rpc,
            path: str,
            form: Literal["$response", "$items", "$request"] = "$response",
            max_retry: int = 10,
            timeout: int = None,  # type: ignore
            concurrency: int = None,  # type: ignore
            callback: List[Callable] = None,  # type: ignore
            errback: List[Callable] = None,  # type: ignore
            **options,
    ):
        """
        绑定  rpc

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

        async def submit(seeds: dict, request: Request = ...):
            if semaphore and semaphore.locked():
                raise signals.Wait(1)

            if semaphore:
                await semaphore.acquire()

            loop = asyncio.get_event_loop()
            seeds = seeds or {}
            data = {
                **seeds,
                "$futureType": form,
                "$futureMaxRetry": max_retry,
            }

            fu = loop.run_in_executor(pool, obj.execute, data, timeout)

            try:
                ctx = await asyncio.wait_for(fu, timeout=timeout)

            except Exception as e:
                await self.run_callbacks(errback, fu, request=request, seeds=seeds, error=e)
                raise

            else:
                await self.run_callbacks(callback, fu, request=request, seeds=seeds, retval=ctx)
                return ctx

            finally:
                semaphore and semaphore.release()  # type: ignore

        pool = ThreadPoolExecutor(max_workers=concurrency)
        if concurrency and concurrency > 0:
            obj.spider.dispatcher.max_workers = concurrency
            semaphore = asyncio.Semaphore(concurrency)
        else:
            semaphore = None

        not obj.running and obj.run()  # type: ignore
        self.create_addon(uri=path, adapter=submit, **options)

    async def _invoke(
            self, orders: Dict[str, List[dict]], timeout: Optional[int] = None
    ):
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
            timeout: Optional[int] = None,
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

    def run(self, *args, **kwargs):
        ...

    async def websocket_endpoint(self, *args, **kwargs):
        ...

    def add_route(self, *args, **kwargs):
        return self.router.add_route(*args, **kwargs)

    def add_websocket_route(self, *args, **kwargs):
        return self.router.add_websocket_route(*args, **kwargs)

    def add_middleware(self, *args, **kwargs):
        ...

    def create_view(self, uri: str, adapter: Callable = ..., **options):
        raise NotImplementedError

    @classmethod
    async def run_callbacks(cls, fns, fu: asyncio.Future, request: Request = ..., **kwargs):
        if not fns:
            return

        namespace: dict[str, Any] = {"fu": fu, "request": request}

        annotations: dict[Any, Any] = {
            type(fu): fu,
            type(request): request,
        }
        raw_req = request.get_options("$request")
        annotations.update({type(raw_req): raw_req})

        for fn in pandora.iterable(fns):
            if callable(fn):
                prepared = pandora.prepare(fn, kwargs=kwargs, namespace=namespace, annotations=annotations)
                try:
                    if inspect.iscoroutinefunction(fn):
                        await prepared.func(*prepared.args, **prepared.kwargs)
                    else:
                        await cls.awaitable_call(prepared.func, *prepared.args, **prepared.kwargs)
                except signals.Break:
                    return
