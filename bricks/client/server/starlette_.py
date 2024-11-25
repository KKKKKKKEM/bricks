from __future__ import annotations

import asyncio
import dataclasses
import functools
import re
from re import Pattern
from typing import Dict, Callable, Literal, Optional, Union, List

from loguru import logger

from bricks.client.server import Gateway
from bricks.core import signals
from bricks.lib.request import Request
from bricks.spider.air import Context
from bricks.utils import pandora

pandora.require("starlette==0.41.3")
pandora.require("uvicorn==0.32.0")
import uvicorn  # noqa E402
from starlette import requests, responses, websockets  # noqa E402
from starlette.applications import Starlette  # noqa E402
from starlette.endpoints import HTTPEndpoint  # noqa E402
from starlette.routing import Route, WebSocketRoute  # noqa E402
from starlette.websockets import WebSocketDisconnect  # noqa E402
from starlette.middleware.base import ( BaseHTTPMiddleware,RequestResponseEndpoint,DispatchFunction)  # noqa E402
from starlette.types import ASGIApp  # noqa E402


@dataclasses.dataclass
class Middleware:
    adapter: Callable
    pattern: Optional[Pattern] = None
    args: Union[tuple, list] = ()
    kwargs: dict = None


class GlobalMiddleware(BaseHTTPMiddleware):
    def __init__(
        self,
        app: ASGIApp,
        dispatch: DispatchFunction | None = None,
        gateway: "APP" = None,
    ) -> None:
        super().__init__(app, dispatch)
        self.gateway = gateway

    async def dispatch(
        self, request: requests.Request, call_next: RequestResponseEndpoint
    ):
        try:
            for middleware in self.gateway.middlewares["request"]:
                req = AddonView.make_req(request)

                if middleware.pattern and not middleware.pattern.search(
                    str(request.url)
                ):
                    continue

                prepared = pandora.prepare(
                    middleware.adapter,
                    args=middleware.args,
                    kwargs=middleware.kwargs,
                    namespace={
                        "request": req,
                        "gateway": self.gateway,
                    },
                    annotations={
                        type(req): req,
                        type(self.gateway): self.gateway,
                    },
                )

                await self.gateway.awaitable_call(
                    prepared.func, *prepared.args, **prepared.kwargs
                )

            response = await call_next(request)

            for middleware in self.gateway.middlewares["response"]:
                if middleware.pattern and not middleware.pattern.search(
                    str(request.url)
                ):
                    continue

                prepared = pandora.prepare(
                    middleware.adapter,
                    args=middleware.args,
                    kwargs=middleware.kwargs,
                    namespace={
                        "request": request,
                        "response": response,
                        "gateway": self.gateway,
                    },
                    annotations={
                        type(request): request,
                        type(response): response,
                        type(self.gateway): self.gateway,
                    },
                )
                await self.gateway.awaitable_call(
                    prepared.func, *prepared.args, **prepared.kwargs
                )

            return response
        except Exception as e:
            return responses.JSONResponse(
                {"status": 403, "msg": str(e)}, status_code=403
            )

    @staticmethod
    async def get_body(resp):
        original_body = b""
        async for chunk in resp.body_iterator:
            original_body += chunk
        original_data = original_body.decode()
        return original_data


async def make_req(request: requests.Request):
    body = await request.body()

    return Request(
        url=str(request.url),
        method=request.method,
        body=body.decode(),
        headers=dict(request.headers),
        cookies=request.cookies,
        options={"$request": request},
    )


class AddonView(HTTPEndpoint):
    def __init__(
        self, *args, main: Callable = None, future_type: str = "$response", **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.main = main
        self.future_type = future_type

    async def get(self, request: requests.Request):
        try:
            seeds = dict(request.query_params)
            req = await self.make_req(request)
            ctx = await self.main(seeds, req, is_alive=self.is_alive)

            return self.fmt(ctx)
        except (SystemExit, KeyboardInterrupt):
            raise

        except signals.Wait:
            return responses.JSONResponse(
                content={"code": 429, "msg": "Too Many Requests"}, status_code=429
            )

        except Exception as e:
            return responses.JSONResponse(
                content={"code": 500, "msg": str(e)}, status_code=500
            )

    async def post(self, request: requests.Request):
        try:
            seeds = await request.json()
            req = await self.make_req(request)
            ctx = await self.main(seeds, req, is_alive=self.is_alive)
            return self.fmt(ctx)
        except (SystemExit, KeyboardInterrupt):
            raise

        except signals.Wait:
            return responses.JSONResponse(
                content={"code": 429, "msg": "Too Many Requests"}, status_code=429
            )

        except Exception as e:
            return responses.JSONResponse(
                content={"code": 500, "msg": str(e)}, status_code=500
            )

    def fmt(self, context: Context):
        if context is None:
            return responses.Response()

        future_type = context.seeds.get("$futureType", self.future_type)
        if future_type == "$items":
            if context.items:
                return responses.JSONResponse(context.items.data)
            else:
                return responses.Response()

        elif future_type == "$response":
            if context.response.status_code != -1:
                return responses.Response(
                    context.response.text,
                    media_type=context.response.headers.get(
                        "Content-Type", "text/plain"
                    ),
                    status_code=context.response.status_code,
                )
            else:
                return responses.JSONResponse(
                    {
                        "code": -1,
                        "msg": f"error: {context.response.error}, reason: {context.response.reason}",
                    },
                    status_code=500,
                )

        else:
            if context.request:
                return responses.PlainTextResponse(context.request.curl)
            else:
                return responses.Response()

    @staticmethod
    async def is_alive(request: requests.Request):
        close = await request.is_disconnected()
        return not close


class APP(Gateway):
    def __init__(self):
        super().__init__()
        self.connections: Dict[websockets.WebSocket, str] = {}
        self.router = Starlette(
            routes=[
                Route(
                    path="/invoke",
                    methods=["POST"],
                    name="发布指令/调用",
                    endpoint=self.invoke,
                ),
                WebSocketRoute(
                    path="/ws/<client_id>",
                    name="websocket",
                    endpoint=self.websocket_endpoint,
                ),
            ]
        )
        self.router.add_middleware(GlobalMiddleware, gateway=self)  # noqa
        self.middlewares: Dict[str, List[Middleware]] = {
            "request": [],
            "response": [],
        }

    def create_addon(self, uri: str, adapter: Callable = None, **options):
        options.setdefault("methods", ["GET", "POST"])
        self.router.add_route(
            route=functools.partial(AddonView, main=adapter), path=uri, **options
        )

    def create_view(self, uri: str, adapter: Callable = None, **options):
        async def handler(request: requests.Request):
            try:
                req = await make_req(request)
                prepared = pandora.prepare(
                    func=adapter,
                    namespace={
                        "request": req,
                    },
                )
                ret = await Gateway.awaitable_call(
                    prepared.func, *prepared.args, **prepared.kwargs
                )

                if not ret:
                    return responses.Response()

                elif isinstance(ret, responses.Response):
                    return ret

                elif isinstance(ret, dict):
                    return responses.JSONResponse(ret)

                else:
                    return responses.PlainTextResponse(str(ret))

            except (SystemExit, KeyboardInterrupt):
                raise

            except signals.Wait:
                return responses.JSONResponse(
                    content={"code": 429, "msg": "Too Many Requests"}, status_code=429
                )

            except Exception as e:
                return responses.JSONResponse(
                    content={"code": 500, "msg": str(e)}, status_code=500
                )

        self.router.add_route(route=handler, path=uri, **options)

    async def websocket_endpoint(self, ws: websockets.WebSocket, client_id: str):
        """
        websocket endpoint

        :param client_id:
        :param ws:
        :return:
        """

        try:
            await ws.accept()
            self.connections[ws] = client_id
            logger.debug(f"[连接成功] {client_id} | {ws}")
            async for msg in ws.iter_json():
                future_id = msg["MID"]
                # ptr = ctypes.cast(future_id, ctypes.py_object)
                future: [asyncio.Future] = self._futures.pop(future_id, None)
                future and future.set_result(msg)

        except WebSocketDisconnect:
            await ws.close()
        except (SystemExit, KeyboardInterrupt):
            raise
        finally:
            self.connections.pop(ws, None)
            logger.debug(f"[断开连接] {client_id} | {ws} ")

    def run(self, host: str = "0.0.0.0", port: int = 8888, **options):
        uvicorn.run(self.router, host=host, port=port, **options)

    def add_middleware(
        self,
        form: Literal["request", "response"],
        adapter: Callable,
        *args,
        pattern: str = "",
        **kwargs,
    ):
        """
        设置拦截器

        :param adapter:
        :param form: request - 拦截请求; response - 拦截响应
        :param pattern: 用于限制 url 的正则表达式
        :return:
        """
        if pattern:
            re_pattern = re.compile(pattern)
        else:
            re_pattern = None

        self.middlewares[form].append(
            Middleware(pattern=re_pattern, adapter=adapter, args=args, kwargs=kwargs)
        )


app = APP()
