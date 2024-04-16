# -*- coding: utf-8 -*-
# @Time    : 2023-12-12 14:47
# @Author  : Kem
# @Desc    : 客户端, 让 bricks 支持 api 调用
import asyncio
import collections
import inspect
import re
import threading
import time
import uuid
from concurrent.futures import Future
from typing import Dict, List, Callable, Any, Literal

from loguru import logger

from bricks.spider.air import Listener, Context
from bricks.utils import pandora

pandora.require("fastapi==0.105.0")
pandora.require("websockets==12.0")
pandora.require("uvicorn")
import fastapi
import uvicorn
from starlette import requests, responses, websockets


class APP:
    def __init__(
            self,
            port: int = 8888,
            host: str = "0.0.0.0",
            router: fastapi.APIRouter = None,
            **options
    ):
        self.app = fastapi.FastAPI(**options)
        self.host = host
        self.port = port
        self.router = router or fastapi.APIRouter()
        self.connections: Dict[websockets.WebSocket, str] = {}
        self.futures: Dict[str, asyncio.Future] = collections.defaultdict(asyncio.Future)

    async def websocket_endpoint(self, websocket: websockets.WebSocket, client_id: str):
        """
        websocket endpoint

        :param websocket:
        :param client_id:
        :return:
        """

        try:
            await websocket.accept()
            self.connections[websocket] = client_id
            logger.debug(f'[连接成功] {client_id} | {websocket}')
            async for msg in websocket.iter_json():
                self.futures[f'{msg["CID"]}|{msg["MID"]}'].set_result(msg)

        except fastapi.WebSocketDisconnect:
            await websocket.close()

        finally:
            self.connections.pop(websocket, None)
            logger.debug(f'[断开连接] {client_id} | {websocket} ')

    async def invoke(
            self,
            orders: Dict[str, List[dict]],
            timeout: int = None,
            request: requests.Request = None
    ):

        if request:
            headers = request.headers
        else:
            headers = {}

        request_id: str = headers.get("request_id") or str(uuid.uuid4())

        future = asyncio.ensure_future(self._invoke(request_id, orders))
        if timeout == 0:
            ret = {
                "code": 0,
                "msg": "成功提交任务"
            }

        else:

            try:
                await asyncio.wait_for(future, timeout=timeout)
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

        return responses.JSONResponse(
            content=ret,
            headers={
                "request_id": request_id
            }

        )

    async def _invoke(self, request_id: str, orders: Dict[str, List[dict]]):
        futures = []
        for ws, cid in self.connections.items():
            if cid in orders:
                ctx = {
                    "MID": request_id,
                    "CID": cid,
                    "CTX": orders[cid]
                }
                fid = f'{cid}|{request_id}'
                future = self.futures[fid]
                await ws.send_json(ctx)
                futures.append(future)

        else:
            futures and await asyncio.wait(futures)
            ret = []
            for index, fu in enumerate(futures):
                ctx = fu.result()
                ret.append(ctx)
                del self.futures[f"{ctx['CID']}|{ctx['MID']}"]

            return ret

    def run(self):
        self.router.websocket('/ws/{client_id}')(self.websocket_endpoint)
        self.router.post("/invoke", name='发布指令/调用')(self.invoke)
        self.app.include_router(self.router)
        uvicorn.run(self.app, host=self.host, port=self.port)

    def bind_listener(
            self,
            listener: Listener,
            path: str,
            tags: list = None,
            method: str = "POST",
            adapter: Callable = None,
            form: str = '$response',
            max_retry: int = 10,
            **options
    ):
        """
        绑定 listener

        :param form: 接口返回类型, $response-> 响应; $items -> items
        :param max_retry: 种子最大重试次数
        :param tags:
        :param listener: 需要绑定的 listener
        :param path: 访问路径
        :param method: 访问方法
        :param adapter: 自定义视图函数
        :return:
        """

        def fmt_ret(future_type: str, context: Context):
            if future_type == '$items':
                return responses.JSONResponse(content=context.items.data if context.items else None)
            elif future_type == '$response':
                return responses.PlainTextResponse(content=context.response.content if context.response else None)
            else:
                return responses.PlainTextResponse(content=context.request.curl if context.request else None)

        async def submit(seeds: dict, timeout: int = None):
            async for ctx in listener.wait(seeds, timeout=timeout):
                return fmt_ret(form, ctx)

        async def post(request: fastapi.Request, timeout: int = None):
            try:
                seeds = await request.json()
                return await submit(
                    {
                        **seeds,
                        "$futureType": form,
                        "$futureMaxRetry": max_retry
                    },
                    timeout
                )
            except Exception as e:
                return responses.JSONResponse(
                    content={
                        "code": -1,
                        "msg": str(e)
                    },
                    status_code=500
                )

        async def get(request: fastapi.Request, timeout: int = None):
            try:
                seeds = dict(request.query_params)
                return await submit(
                    {
                        **seeds,
                        "$futureType": form,
                        "$futureMaxRetry": max_retry
                    },
                    timeout
                )
            except Exception as e:
                return responses.JSONResponse(
                    content={
                        "code": -1,
                        "msg": str(e)
                    },
                    status_code=500
                )

        func = getattr(self.router, method.lower())
        func(path, tags=tags, **options)(adapter or locals()[method.lower()])

    def add_middleware(self, middleware: [Callable, type], form: str = "http", **options: Any):
        """
        添加中间件

        :param middleware:
        :param form:
        :param options:
        :return:
        """
        if inspect.isclass(middleware):
            self.app.add_middleware(middleware, **options)
        else:
            self.app.middleware(form)(middleware)

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

        async def get_body(resp):
            original_body = b''
            async for chunk in resp.body_iterator:
                original_body += chunk
            original_data = original_body.decode()
            return original_data

        def inner(func):
            async def hook_req(request: fastapi.Request, call_next):
                if re_pattern and not re_pattern.search(str(request.url)):
                    return await call_next(request)

                prepared = pandora.prepare(
                    func=func,
                    namespace={
                        "request": request,
                    },
                    annotations={
                        fastapi.Request: request,
                    }
                )
                resp = await self._call(prepared.func, *prepared.args, **prepared.kwargs)
                if resp:
                    return resp
                else:
                    return await call_next(request)

            async def hook_resp(request: fastapi.Request, call_next):
                if re_pattern and not re_pattern.search(str(request.url)):
                    return await call_next(request)

                response = await call_next(request)
                raw_resp = fastapi.Response(
                    content=await get_body(response),
                    headers=response.headers,
                    status_code=response.status_code
                )
                prepared = pandora.prepare(
                    func=func,
                    namespace={
                        "request": request,
                        "response": raw_resp,
                    },
                    annotations={
                        fastapi.Request: request,
                        fastapi.Response: raw_resp,
                    }
                )
                resp = await self._call(prepared.func, *prepared.args, **prepared.kwargs)
                if resp:
                    return resp
                else:
                    headers = dict(raw_resp.headers)
                    headers.pop("content-length", None)
                    raw_resp.init_headers(headers)
                    return raw_resp

            if form == 'request':
                self.add_middleware(hook_req)
            else:
                self.add_middleware(hook_resp)

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


if __name__ == '__main__':
    server = APP()


    @server.on("response")
    def after_request(response: fastapi.Response):
        time.sleep(5)
        print(response.body)


    @server.on("request")
    async def before_request(request: fastapi.Request):
        print(request)


    server.run()
