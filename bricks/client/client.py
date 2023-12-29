# -*- coding: utf-8 -*-
# @Time    : 2023-12-12 14:47
# @Author  : Kem
# @Desc    : 客户端, 让 bricks 支持 api 调用
import asyncio
import collections
import uuid
from typing import Dict, List

from loguru import logger

from bricks.utils import pandora

pandora.require("fastapi==0.105.0")
pandora.require("websockets==12.0")
pandora.require("uvicorn")
import fastapi
import uvicorn
from starlette import requests, responses, websockets

fast_app = fastapi.FastAPI()


class Server:
    def __init__(
            self,
            port: int = 8888,
            reload: bool = False,
            host: str = "0.0.0.0",
            router: fastapi.APIRouter = None,
    ):
        self.host = host
        self.port = port
        self.reload = reload
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
        fast_app.include_router(self.router)
        uvicorn.run(f'{__name__}:fast_app', host=self.host, port=self.port, reload=self.reload)


if __name__ == '__main__':
    server = Server()
    server.run()
