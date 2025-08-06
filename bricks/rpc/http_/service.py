import asyncio
import json
from concurrent import futures
from typing import Callable

import requests
from loguru import logger

from bricks.utils import pandora

pandora.require("aiohttp==3.10.11")

from aiohttp import web

from bricks.rpc.common import BaseRpcService, BaseRpcClient, RpcRequest, RpcResponse


class Service(BaseRpcService):
    """
    基于 HTTP 的 RPC 服务。
    任何业务逻辑服务只需继承此类，并实现其业务方法。
    """

    async def handle_rpc(self, request: web.Request) -> web.Response:
        """
        处理 HTTP RPC 请求
        """
        try:
            # 解析请求数据
            request_data = await request.json()
            rpc_request = RpcRequest.from_dict(request_data)

            # 处理请求
            rpc_response = await self.process_rpc_request(rpc_request)

            # 返回响应
            return web.json_response(rpc_response.to_dict())

        except json.JSONDecodeError:
            return web.json_response({
                "message": "Invalid JSON in request body",
                "code": 400,
                "data": "",
                "request_id": ""
            }, status=400)
        except Exception as e:
            logger.error(f"Error handling HTTP RPC request: {e}")
            return web.json_response({
                "message": f"Internal server error: {e}",
                "code": 500,
                "data": "",
                "request_id": ""
            }, status=500)

    async def serve(self, concurrency: int = 10, ident: int = 0, on_server_started: Callable[[int], None] = None, **kwargs):
        """
        启动 HTTP RPC 服务器

        :param concurrency: 并发数
        :param ident: 监听端口，默认随机
        :param on_server_started: 当服务启动完后的回调
        """
        # 设置线程池
        self._executor = futures.ThreadPoolExecutor(max_workers=concurrency)

        # 创建 aiohttp 应用
        app = web.Application()
        app.router.add_post('/rpc', self.handle_rpc)

        # 创建服务器
        runner = web.AppRunner(app)
        await runner.setup()

        # 启动服务器
        site = web.TCPSite(runner, '0.0.0.0', ident)
        await site.start()

        # 获取实际端口
        actual_port = site._server.sockets[0].getsockname()[1]
        logger.info(f"HTTP RPC Server started on port {actual_port}...")

        try:
            if on_server_started:
                on_server_started(actual_port)

            # 保持服务器运行
            while True:
                await asyncio.sleep(1)

        except KeyboardInterrupt:
            logger.info("HTTP RPC Server shutdown initiated.")
        finally:
            await runner.cleanup()


class Client(BaseRpcClient):
    """
    HTTP RPC 客户端（同步实现）
    """

    def __init__(self, endpoint: str):
        """
        :param endpoint: HTTP 服务器地址，如 "http://localhost:8080"
        """
        self.endpoint = endpoint.rstrip('/')
        if not self.endpoint.startswith("http://") and not self.endpoint.startswith("https://"):
            self.endpoint = "http" + "://" + self.endpoint


    def rpc(self, method: str, *args, **kwargs) -> RpcResponse:
        """
        同步 RPC 调用接口
        """
        # 准备请求
        rpc_request = self._prepare_request(method, *args, **kwargs)

        try:
            # 发送 HTTP 请求
            response = requests.post(
                f"{self.endpoint}/rpc",
                json=rpc_request.to_dict(),
                headers={"Content-Type": "application/json"}
            )
            # 检查 HTTP 状态码
            if not response.ok:
                raise RuntimeError(f"HTTP error {response.status_code}: {response.text}")

            # 解析响应
            response_data = response.json()
            return RpcResponse.from_dict({**response_data, "request_id": rpc_request.request_id})

        except Exception as e:
            raise RuntimeError(f"HTTP RPC call failed for method '{method}': {e}")
