import asyncio
import json
import re
import uuid
from typing import Dict, Callable, Literal

from loguru import logger

from bricks.client.server import Gateway
from bricks.core import signals
from bricks.lib.request import Request
from bricks.spider.air import Context
from bricks.utils import pandora

pandora.require(package_spec="sanic==24.6.0")
import sanic  # noqa E402
from sanic.views import HTTPMethodView  # noqa E402
from sanic.response import BaseHTTPResponse  # noqa E402


async def make_req(request: sanic.Request):
    await request.receive_body()
    return Request(
        url=request.url,
        method=request.method,
        body=request.body.decode("utf-8"),
        headers=request.headers,
        cookies=request.cookies,
        options={"$request": request},
    )


async def is_alive(request: sanic.Request):
    return not request.transport.is_closing()


class AddonView(HTTPMethodView):
    def __init__(self, main: Callable, future_type: str = "$response"):
        super().__init__()
        self.main = main
        self.future_type = future_type

    async def get(self, request: sanic.Request):
        try:
            body = request.args.get("body", "")
            if body:
                body = json.loads(body)
            else:
                body = {}

            await request.receive_body()
            req = await make_req(request)
            ctx = await self.main(body, req, is_alive=is_alive)
            return self.fmt(ctx)
        except (SystemExit, KeyboardInterrupt):
            raise

        except signals.Wait:
            return sanic.response.json(
                body={"code": 429, "msg": "Too Many Requests"}, status=429
            )

        except Exception as e:
            return sanic.response.json(body={"code": 500, "msg": str(e)}, status=500)

    async def post(self, request: sanic.Request):
        try:
            req = await make_req(request)
            seeds = request.json
            ctx = await self.main(seeds, req, is_alive=is_alive)
            return self.fmt(ctx)
        except (SystemExit, KeyboardInterrupt):
            raise

        except signals.Wait:
            return sanic.response.json(
                body={"code": 429, "msg": "Too Many Requests"}, status=429
            )

        except Exception as e:
            return sanic.response.json(body={"code": 500, "msg": str(e)}, status=500)

    def fmt(self, context: Context):
        if context is None:
            return sanic.response.empty()

        future_type = context.seeds.get("$futureType", self.future_type)
        if future_type == "$items":
            if context.items:
                return sanic.response.json(context.items.data, ensure_ascii=False)
            else:
                return sanic.response.empty()

        elif future_type == "$response":
            if context.response.status_code != -1:
                return sanic.response.text(
                    context.response.text,
                    content_type=context.response.headers.get(
                        "Content-Type", "text/plain"
                    ),
                    status=context.response.status_code,
                )
            else:
                return sanic.response.json(
                    {
                        "code": -1,
                        "msg": f"error: {context.response.error}, reason: {context.response.reason}",
                    },
                    status=500,
                )

        else:
            if context.request:
                return sanic.response.text(context.request.curl)
            else:
                return sanic.response.empty()


class APP(Gateway):
    def __init__(self):
        super().__init__()
        self.connections: Dict[sanic.Websocket, str] = {}
        self.router = sanic.Sanic(name="bricks-api")
        self.router.add_route(
            self.invoke, uri="/invoke", methods=["POST"], name="发布指令/调用"
        )
        self.router.add_websocket_route(self.websocket_endpoint, uri="/ws/<client_id>")

    def create_addon(self, uri: str, adapter: Callable = None, **options):
        options.setdefault("name", str(uuid.uuid4()))
        self.router.add_route(AddonView.as_view(main=adapter), uri=uri, **options)

    def create_view(self, uri: str, adapter: Callable = None, **options):
        async def handler(request: sanic.Request):
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
                    return sanic.response.empty()

                elif isinstance(ret, BaseHTTPResponse):
                    return ret

                elif isinstance(ret, dict):
                    return sanic.response.json(ret)

                else:
                    return sanic.response.text(str(ret))

            except (SystemExit, KeyboardInterrupt):
                raise

            except signals.Wait:
                return sanic.response.json(
                    body={"code": 429, "msg": "Too Many Requests"}, status=429
                )

            except Exception as e:
                return sanic.response.json(
                    body={"code": 500, "msg": str(e)}, status=500
                )

        options.setdefault("name", str(uuid.uuid4()))
        self.router.add_route(handler, uri=uri, **options)

    async def websocket_endpoint(
        self, _: sanic.Request, ws: sanic.Websocket, client_id: str
    ):
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
            logger.debug(f"[连接成功] {client_id} | {ws}")
            async for msg in ws:
                future_id = msg["MID"]
                # ptr = ctypes.cast(future_id, ctypes.py_object)
                future: [asyncio.Future] = self._futures.pop(future_id, None)
                future and future.set_result(msg)

        except sanic.exceptions.WebsocketClosed:
            await ws.close()
        except (SystemExit, KeyboardInterrupt):
            raise
        finally:
            self.connections.pop(ws, None)
            logger.debug(f"[断开连接] {client_id} | {ws} ")

    def run(self, host: str = "0.0.0.0", port: int = 8888, **options):
        options.setdefault("single_process", True)
        options.setdefault("access_log", False)
        self.router.run(host=host, port=port, **options)

    def add_middleware(
        self,
        form: Literal["request", "response"],
        adapter: Callable,
        pattern: str = "",
        *args,
        **kwargs,
    ):
        def wrapper(func):
            async def inner(request, *a, **kw):
                if re_pattern and not re_pattern.search(request.url):
                    return

                req = await make_req(request)
                prepared = pandora.prepare(
                    func,
                    args=[*args, *a],
                    kwargs={**kwargs, **kw},
                    namespace={
                        "request": req,
                        "gateway": app,
                    },
                    annotations={
                        type(req): req,
                        type(app): app,
                    },
                )

                await app.awaitable_call(
                    prepared.func, *prepared.args, **prepared.kwargs
                )

            return inner

        if pattern:
            re_pattern = re.compile(pattern)
        else:
            re_pattern = None

        # self.router.on_request()
        self.router.middleware(wrapper(adapter), form)


app = APP()
