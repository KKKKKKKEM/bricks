import asyncio
import uuid
from typing import Dict, Callable

from loguru import logger

from bricks.client.server import Gateway, TooManyRequestsError
from bricks.spider.air import Context
from bricks.lib.request import Request
from bricks.utils import pandora

pandora.require("sanic==24.6.0")
import sanic  # noqa E402
from sanic.views import HTTPMethodView  # noqa E402


class View(HTTPMethodView):

    def __init__(self, main: Callable, future_type: str = "$response"):
        super().__init__()
        self.main = main
        self.future_type = future_type

    async def get(self, request: sanic.Request):
        try:
            seeds = request.get_args()
            await request.receive_body()
            req = await self.make_req(request)
            ctx = await self.main(seeds, req, is_alive=self.is_alive)
            return self.fmt(ctx)
        except (SystemExit, KeyboardInterrupt):
            raise

        except TooManyRequestsError:
            return sanic.response.json(
                body={
                    "code": 429,
                    "msg": "Too Many Requests"
                },
                status=429
            )

        except Exception as e:
            return sanic.response.json(
                body={
                    "code": 500,
                    "msg": str(e)
                },
                status=500
            )

    async def post(self, request: sanic.Request):
        try:
            req = await self.make_req(request)
            seeds = request.json
            ctx = await self.main(seeds, req, is_alive=self.is_alive)
            return self.fmt(ctx)
        except (SystemExit, KeyboardInterrupt):
            raise

        except TooManyRequestsError:
            return sanic.response.json(
                body={
                    "code": 429,
                    "msg": "Too Many Requests"
                },
                status=429
            )

        except Exception as e:
            return sanic.response.json(
                body={
                    "code": 500,
                    "msg": str(e)
                },
                status=500
            )

    def fmt(self, context: Context):
        if context is None:
            return sanic.response.empty()

        future_type = context.seeds.get("$futureType", self.future_type)
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

    @staticmethod
    async def make_req(request: sanic.Request):
        await request.receive_body()
        return Request(
            url=request.url,
            method=request.method,
            body=request.body,
            headers=request.headers,
            cookies=request.cookies,
            options={"$request": request}
        )

    @staticmethod
    async def is_alive(request: sanic.Request):
        return not request.transport.is_closing()


class APP(Gateway):
    def __init__(self):
        super().__init__()
        self.connections: Dict[sanic.Websocket, str] = {}
        self.router = sanic.Sanic(name="bricks-api")
        self.router.add_route(self.invoke, uri="/invoke", methods=["POST"], name='发布指令/调用')
        self.router.add_websocket_route(self.websocket_endpoint, uri="/ws/<client_id>")

    def create_view(self, uri: str, adapter: Callable = None, options: dict = None):
        options = options or {}
        options.setdefault("name", str(uuid.uuid4()))
        self.router.add_route(
            View.as_view(main=adapter),
            uri=uri,
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
                # ptr = ctypes.cast(future_id, ctypes.py_object)
                future: [asyncio.Future] = self._futures.pop(future_id, None)
                future and future.set_result(msg)

        except sanic.exceptions.WebsocketClosed:
            await ws.close()
        except (SystemExit, KeyboardInterrupt):
            raise
        finally:
            self.connections.pop(ws, None)
            logger.debug(f'[断开连接] {client_id} | {ws} ')

    def run(self, host: str = "0.0.0.0", port: int = 8888, **options):
        options.setdefault("single_process", True)
        options.setdefault("access_log", False)
        self.router.run(host=host, port=port, **options)


app = APP()
