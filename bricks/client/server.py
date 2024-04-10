# -*- coding: utf-8 -*-
# @Time    : 2023-12-12 14:47
# @Author  : Kem
# @Desc    : 客户端, 让 bricks 支持 api 调用
import asyncio
import collections
import inspect
import uuid
from typing import Dict, List, Callable, Any

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
            **options
    ):
        """
        绑定 listener

        :param tags:
        :param listener: 需要绑定的 listener
        :param path: 访问路径
        :param method: 访问方法
        :param adapter: 自定义视图函数
        :return:
        """

        def fmt_ret(future_type: str, context: Context):
            if future_type == '$items':
                return responses.JSONResponse(content=context.items.data)
            elif future_type == '$response':
                return responses.PlainTextResponse(content=context.response.content)
            else:
                return responses.PlainTextResponse(content=context.request.curl)

        async def post(request: fastapi.Request, form: str = '$response', timeout: int = None):
            try:
                seeds = await request.json()
                async for ctx in listener.wait({**seeds, "$futureType": form}, timeout=timeout):
                    return fmt_ret(form, ctx)
            except Exception as e:
                return responses.JSONResponse(
                    content={
                        "code": -1,
                        "msg": str(e)
                    },
                    status_code=500
                )

        async def get(request: fastapi.Request, form: str = '$response', timeout: int = None):
            try:
                seeds = request.query_params
                async for ctx in listener.wait({**seeds, "$futureType": form}, timeout=timeout):
                    return fmt_ret(form, ctx)
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


if __name__ == '__main__':
    server = APP()
    server.run()
