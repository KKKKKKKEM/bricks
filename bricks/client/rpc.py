# -*- coding: utf-8 -*-
# @Time    : 2023-12-28 15:37
# @Author  : Kem
# @Desc    : rpc 组件
import asyncio
import json
import threading
import uuid
from concurrent.futures import Future
from typing import Callable, Any
from urllib import parse

from loguru import logger

from bricks.utils import pandora

pandora.require("websockets==12.0")
import websockets


class APP:

    def __init__(
            self,

            identifier: str = None,
            host: str = "0.0.0.0",
            path: str = "ws",
            port: int = 8888,
            scheme: str = "ws",
            target: Any = None,
    ):
        self.identifier = identifier or str(uuid.uuid4())
        self.adapters = {}
        self.host = host
        self.path = path
        self.port = port
        self.scheme = scheme
        self.target = target

    async def bind_sockets(self):
        identifier = self.identifier or str(uuid.uuid4())
        uri = parse.urljoin(f"{self.scheme}://{self.host}:{self.port}", f'{self.path}/{identifier}')
        while True:

            try:
                async with websockets.connect(uri) as websocket:
                    logger.debug(f'[成功连接] {uri}')
                    while True:
                        greeting = await websocket.recv()
                        if not greeting:
                            fs = []
                        else:
                            if isinstance(greeting, bytes):
                                greeting: str = greeting.decode()

                            greeting: dict = json.loads(greeting)

                            mid: str = greeting['MID']
                            cid: str = greeting['CID']
                            ctx: list = greeting['CTX']
                            logger.debug(f'收到消息: {greeting}')
                            fs = []

                            for mctx in ctx:
                                action: str = mctx.get('action') or ""
                                args: list = mctx.get("args") or []
                                kwargs: dict = mctx.get("kwargs") or {}

                                try:
                                    if action.startswith('$') and hasattr(self.target, action[1:]):
                                        action: Callable = getattr(self.target, action[1:])

                                    elif action in self.adapters:
                                        action: Callable = self.adapters[action]

                                    else:
                                        action: Callable = pandora.load_objects(action)

                                except Exception as e:
                                    fu = asyncio.Future()
                                    fu.set_exception(e)
                                    fs.append(fu)
                                    continue

                                if asyncio.iscoroutinefunction(action):
                                    fu = asyncio.ensure_future(action(*args, **kwargs))
                                elif callable(action):
                                    fu = self._sync2async(action, *args, **kwargs)
                                else:
                                    fu = asyncio.Future()
                                    fu.set_result(action)

                                fs.append(fu)

                        fs and await asyncio.wait(fs, timeout=None)

                        rctx = []

                        for f in fs:
                            try:
                                rctx.append({
                                    "code": 0,
                                    "message": "success",
                                    "result": f.result()
                                })
                            except BaseException as e:
                                rctx.append({
                                    "code": -1,
                                    "message": e
                                })

                        await websocket.send(
                            json.dumps(
                                {
                                    "MID": mid,
                                    "CID": cid,
                                    "CTX": rctx
                                },
                                default=str
                            )
                        )
            except (KeyboardInterrupt, SystemError, SystemExit):
                raise
            except BaseException as e:
                logger.error(e)
                await asyncio.sleep(1)

    def bind_target(self, target: Any = None):
        self.target = target or self.target

    @staticmethod
    def _sync2async(func: Callable, *args, **kwargs):
        fu = Future()

        def main():

            try:
                ret = func(*args, **kwargs)
                fu.set_result(ret)
            except (SystemExit, KeyboardInterrupt):
                raise
            except BaseException as exc:
                if fu.set_running_or_notify_cancel():
                    fu.set_exception(exc)
                raise

        threading.Thread(target=main, daemon=True).start()

        return asyncio.wrap_future(fu)

    def run(self):
        asyncio.run(self.bind_sockets())

    def start(self, daemon=True):
        t = threading.Thread(target=self.run, daemon=daemon)
        t.start()

    def register_adapter(self, form: str, action: Callable):
        self.adapters[form] = action
