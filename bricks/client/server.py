# -*- coding: utf-8 -*-
# @Time    : 2023-12-12 14:47
# @Author  : Kem
# @Desc    : 客户端, 让 bricks 支持 api 调用
import asyncio
import ctypes
import inspect
import json
import re
import threading
import uuid
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Dict, List, Callable, Literal, Union

from loguru import logger

from bricks.spider.addon import Listener, Rpc
from bricks.spider.air import Context
from bricks.utils import pandora

pandora.require("sanic==24.6.0")
import sanic

_sanic = sanic.Sanic(name="bricks-api")


def parse_ctx(future_type: str, context: Context):
    if future_type == '$items':
        if context.items:
            return sanic.response.json(context.items.data, ensure_ascii=False)
        else:
            return sanic.response.empty()

    elif future_type == '$response':
        if context.response.status_code != -1:
            return sanic.response.text(
                context.response.text,
                content_type=context.response.headers.get("Content-Type", "text/plain"),
                status=context.response.status_code
            )
        else:
            return sanic.response.json(
                {
                    "code": -1,
                    "msg": f"error: {context.response.error}, reason: {context.response.reason}"
                },
                status=500
            )

    else:
        if context.request:
            return sanic.response.text(context.request.curl)
        else:
            return sanic.response.empty()


class APP:
    def __init__(self):
        self.connections: Dict[sanic.Websocket, str] = {}
        self.router = _sanic
        _sanic.add_route(self.invoke, uri="/invoke", methods=["POST"], name='发布指令/调用')
        _sanic.add_websocket_route(self.websocket_endpoint, uri="/ws/<client_id>")

    @staticmethod
    def _add_handler(method, path, tags, adapter, options, submit, form, max_retry, concurrency: int = 1):
        async def post(request: sanic.Request, timeout: int = None):
            if concurrency and semaphore.locked():
                return sanic.response.json(
                    body={
                        "code": -1,
                        "msg": "the concurrency limit is exceeded"
                    },
                    status=429
                )

            try:
                semaphore and await semaphore.acquire()
                seeds = request.json
                return await submit(
                    {
                        **seeds,
                        "$futureType": form,
                        "$futureMaxRetry": max_retry
                    },
                    timeout
                )
            except (SystemExit, KeyboardInterrupt):
                raise
            except Exception as e:
                return sanic.response.json(
                    body={
                        "code": -1,
                        "msg": str(e)
                    },
                    status=500
                )
            finally:
                semaphore and semaphore.release()

        async def get(request: sanic.Request, timeout: int = None):
            try:
                seeds = request.get_args()
                return await submit(
                    {
                        **seeds,
                        "$futureType": form,
                        "$futureMaxRetry": max_retry
                    },
                    timeout
                )
            except (SystemExit, KeyboardInterrupt):
                raise
            except Exception as e:
                return sanic.response.json(
                    body={
                        "code": -1,
                        "msg": str(e)
                    },
                    status=500
                )

        if concurrency:
            semaphore = asyncio.Semaphore(concurrency)
        else:
            semaphore = None

        _sanic.add_route(
            adapter or locals()[method.lower()],
            uri=path,
            methods=[method.upper()],
            name=path,
            **options
        )

    async def websocket_endpoint(self, _: sanic.Request, ws: sanic.Websocket, client_id: str):
        """
        websocket endpoint

        :param client_id:
        :param ws:
        :param _:
        :return:
        """

        try:
            # client_id = request.args.get("client_id")
            self.connections[ws] = client_id
            logger.debug(f'[连接成功] {client_id} | {ws}')
            async for msg in ws:
                future_id = msg["MID"]
                ptr = ctypes.cast(future_id, ctypes.py_object)
                future: [asyncio.Future] = ptr.value
                future.set_result(msg)

        except sanic.exceptions.WebsocketClosed:
            await ws.close()
        except (SystemExit, KeyboardInterrupt):
            raise
        finally:
            self.connections.pop(ws, None)
            logger.debug(f'[断开连接] {client_id} | {ws} ')

    async def invoke(
            self,
            orders: Dict[str, List[dict]],
            timeout: int = None,
            request: sanic.Request = None
    ):

        if request:
            headers = request.headers
        else:
            headers = {}

        request_id: str = headers.get("request_id") or str(uuid.uuid4())

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
            except (SystemExit, KeyboardInterrupt):
                raise
            except asyncio.TimeoutError:
                ret = {
                    "code": 1,
                    "msg": "任务执行超时"
                }

        return sanic.response.json(
            body=ret,
            headers={
                "request_id": request_id
            }

        )

    async def _invoke(self, orders: Dict[str, List[dict]], timeout: int = None):
        futures = []
        for ws, cid in self.connections.items():
            if cid in orders:
                future = asyncio.Future()
                ctx = {
                    "MID": id(future),
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

    @staticmethod
    def run(host: str = "0.0.0.0", port: int = 8888, **options):
        options.setdefault("single_process", True)
        options.setdefault("access_log", False)
        _sanic.run(host=host, port=port, **options)

    def bind_addon(
            self,
            obj: Union[Rpc, Listener],
            path: str,
            tags: list = None,
            method: str = "POST",
            adapter: Callable = None,
            form: Literal['$response', '$items', '$request'] = '$response',
            max_retry: int = 10,
            concurrency: int = None,
            **options
    ):
        """
        绑定 listener / rpc

        :param concurrency: 接口并发数量，超出该数量时会返回429
        :param form: 接口返回类型, 要在响应完了就返回就填 $response，要在解析完就返回就填 $items，要在请求前就返回就填 $request
        :param max_retry: 种子最大重试次数
        :param tags: 接口标签
        :param obj: 需要绑定的 Rpc
        :param path: 访问路径
        :param method: 访问方法
        :param adapter: 自定义视图函数
        :return:
        """

        async def submit(seeds: dict, timeout: int = None):
            loop = asyncio.get_event_loop()
            fu = loop.run_in_executor(pool, obj.execute, seeds, timeout)
            ctx = await asyncio.wait_for(fu, timeout=timeout)
            return parse_ctx(form, ctx)

        pool = ThreadPoolExecutor(max_workers=concurrency)
        if concurrency and concurrency > 0:
            obj.spider.dispatcher.max_workers = concurrency

        not obj.running and obj.run()
        self._add_handler(
            method=method,
            path=path,
            tags=tags,
            adapter=adapter,
            form=form,
            max_retry=max_retry,
            options=options,
            submit=submit,
            concurrency=concurrency
        )

    def on(self, form: Literal['request', 'response'], pattern: str = ""):
        """
        设置拦截器

        :param form: request - 拦截请求; response - 拦截响应
        :param pattern: 用于限制 url 的正则表达式
        :return:
        """
        if pattern:
            re_pattern = re.compile(pattern)
        else:
            re_pattern = None

        def inner(func):
            async def hook_request(request: sanic.Request):
                if re_pattern and not re_pattern.search(str(request.url)):
                    return

                prepared = pandora.prepare(
                    func=func,
                    namespace={
                        "request": request,
                    },
                    annotations={
                        sanic.Request: request,
                    }
                )
                return await self._call(prepared.func, *prepared.args, **prepared.kwargs)

            async def hook_response(request: sanic.Request, response: sanic.response.HTTPResponse):
                if re_pattern and not re_pattern.search(str(request.url)):
                    return

                prepared = pandora.prepare(
                    func=func,
                    namespace={
                        "request": request,
                        "response": response,
                    },
                    annotations={
                        sanic.Request: request,
                        sanic.response.HTTPResponse: response,
                    }
                )
                resp = await self._call(prepared.func, *prepared.args, **prepared.kwargs)
                if resp:
                    return resp
                else:
                    return response

            _sanic.middleware(form)(locals()[f"hook_{form}"])

            return func

        return inner

    @staticmethod
    async def _call(func, *args, **kwargs):
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


app = APP()
