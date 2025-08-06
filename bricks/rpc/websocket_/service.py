import asyncio
import json
from concurrent import futures
from typing import Callable, Set

from loguru import logger

from bricks.utils import pandora

pandora.require("websockets==13.1")

import websockets
from websockets.server import WebSocketServerProtocol

from bricks.rpc.common import BaseRpcService, BaseRpcClient, RpcRequest, RpcResponse


class Service(BaseRpcService):
    """
    基于 WebSocket 的 RPC 服务。
    任何业务逻辑服务只需继承此类，并实现其业务方法。
    """

    def __init__(self):
        super().__init__()
        self.connections: Set[WebSocketServerProtocol] = set()

    async def handle_client(self, websocket: WebSocketServerProtocol, _: str):
        """
        处理 WebSocket 客户端连接
        """
        self.connections.add(websocket)
        logger.info(f"WebSocket client connected: {websocket.remote_address}")

        try:
            async for message in websocket:
                try:
                    # 解析请求数据
                    request_data = json.loads(message)
                    rpc_request = RpcRequest.from_dict(request_data)

                    # 处理请求
                    rpc_response = await self.process_rpc_request(rpc_request)

                    # 发送响应
                    await websocket.send(json.dumps(rpc_response.to_dict()))

                except json.JSONDecodeError:
                    error_response = RpcResponse(
                        message="Invalid JSON in message",
                        code=400
                    )
                    await websocket.send(json.dumps(error_response.to_dict()))
                except Exception as e:
                    logger.error(f"Error handling WebSocket message: {e}")
                    error_response = RpcResponse(
                        message=f"Internal server error: {e}",
                        code=500
                    )
                    await websocket.send(json.dumps(error_response.to_dict()))

        except websockets.exceptions.ConnectionClosed:
            logger.info(f"WebSocket client disconnected: {websocket.remote_address}")
        finally:
            self.connections.discard(websocket)

    async def serve(self, concurrency: int = 10, ident: int = 0, on_server_started: Callable[[int], None] = None, **kwargs):
        """
        启动 WebSocket RPC 服务器

        :param concurrency: 并发数
        :param ident: 监听端口，默认随机
        :param on_server_started: 当服务启动完后的回调
        """
        # 设置线程池
        self._executor = futures.ThreadPoolExecutor(max_workers=concurrency)

        # 启动 WebSocket 服务器
        server = await websockets.serve(
            self.handle_client,
            "0.0.0.0",
            ident,
            max_size=None,  # 无消息大小限制
            max_queue=None  # 无队列大小限制
        )

        # 获取实际端口
        actual_port = server.sockets[0].getsockname()[1]
        logger.info(f"WebSocket RPC Server started on port {actual_port}...")

        try:
            if on_server_started:
                on_server_started(actual_port)

            # 保持服务器运行
            await server.wait_closed()

        except KeyboardInterrupt:
            logger.info("WebSocket RPC Server shutdown initiated.")
            server.close()
            await server.wait_closed()


class Client(BaseRpcClient):
    """
    WebSocket RPC 客户端（同步实现）
    """

    def __init__(self, endpoint: str):
        """
        :param endpoint: WebSocket 服务器地址，如 "ws://localhost:8080"
        """
        if not endpoint.startswith("ws://") or not endpoint.startswith("wss://"):
            endpoint = "ws://" + endpoint

        self.endpoint = endpoint
        self.websocket = None
        self._connected = False
        self._loop = None
        self._thread = None

    def _run_event_loop(self):
        """在单独线程中运行事件循环"""
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()

    def _ensure_connection(self):
        """确保连接已建立"""
        if self._connected:
            return

        # 启动事件循环线程
        if self._thread is None or not self._thread.is_alive():
            import threading
            self._thread = threading.Thread(target=self._run_event_loop, daemon=True)
            self._thread.start()

            # 等待事件循环启动
            import time
            while self._loop is None:
                time.sleep(0.01)

        # 建立 WebSocket 连接
        future = asyncio.run_coroutine_threadsafe(self._async_connect(), self._loop)
        try:
            future.result(timeout=10.0)  # 10秒超时
        except Exception as e:
            raise RuntimeError(f"Failed to connect to WebSocket server: {e}")

    async def _async_connect(self):
        """异步建立连接"""
        try:
            self.websocket = await websockets.connect(self.endpoint)
            self._connected = True
            logger.info(f"Connected to WebSocket server: {self.endpoint}")
        except Exception as e:
            raise RuntimeError(f"Failed to connect to WebSocket server: {e}")

    def rpc(self, method: str, *args, **kwargs):
        """
        同步 RPC 调用接口
        """
        # 确保连接已建立
        self._ensure_connection()

        # 准备请求
        rpc_request = self._prepare_request(method, *args, **kwargs)

        # 在事件循环中执行异步调用
        future = asyncio.run_coroutine_threadsafe(
            self._async_rpc(rpc_request), self._loop
        )

        try:
            response = future.result(timeout=30.0)  # 30秒超时
            return response
        except Exception as e:
            raise RuntimeError(f"WebSocket RPC call failed for method '{method}': {e}")

    async def _async_rpc(self, rpc_request):
        """异步 RPC 调用"""
        try:
            # 发送请求
            await self.websocket.send(json.dumps(rpc_request.to_dict()))

            # 接收响应
            response_message = await self.websocket.recv()
            response_data = json.loads(response_message)
            return RpcResponse.from_dict(response_data)

        except Exception as e:
            self._connected = False
            raise e

    def close(self):
        """关闭 WebSocket 连接"""
        if self._connected and self._loop:
            # 在事件循环中关闭连接
            future = asyncio.run_coroutine_threadsafe(self._async_close(), self._loop)
            try:
                future.result(timeout=5.0)
            except:
                pass

        # 停止事件循环
        if self._loop:
            self._loop.call_soon_threadsafe(self._loop.stop)

        # 等待线程结束
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2.0)

        self._connected = False

    async def _async_close(self):
        """异步关闭连接"""
        if self.websocket:
            await self.websocket.close()

    def __del__(self):
        """析构函数，确保连接被关闭"""
        self.close()
