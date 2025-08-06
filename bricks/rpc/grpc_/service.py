from concurrent import futures
from typing import Callable

from loguru import logger

from bricks.utils import pandora

pandora.require("grpcio==1.74.0")

import grpc

# 导入通用 gRPC 生成的代码
from bricks.rpc.grpc_ import generic_pb2 as pb2
from bricks.rpc.grpc_ import generic_pb2_grpc as pb2_grpc
from bricks.rpc.common import BaseRpcService, RpcRequest, RpcResponse, BaseRpcClient


# ----------------- gRPC 服务基类 -----------------

class Service(pb2_grpc.GenericServicer, BaseRpcService):
    """
    通用 gRPC 服务基类。
    任何业务逻辑服务只需继承此类，并使用 @grpc_method 装饰器标记其业务方法。
    """

    async def rpc(self, request: pb2.Request, context) -> pb2.Response:
        """
        这是 gRPC 定义的唯一处理方法。
        它负责根据 GenericJsonRequest 中的 method_name 分发请求到具体的业务方法，
        并处理 JSON 的序列化/反序列化。
        """
        method_name = request.method
        request_id = request.request_id

        rpc_request = RpcRequest.from_dict({
            "method": method_name,
            "data": request.data,
            "request_id": request_id
        })

        rpc_response = await self.process_rpc_request(rpc_request)
        if rpc_response.code != 0:
            context.set_code(rpc_response.code)
            context.set_details(rpc_response.message)

        return pb2.Response(
            data=rpc_response.data,
            message=rpc_response.message,
            code=rpc_response.code,
            request_id=rpc_response.request_id
        )

    async def serve(self, concurrency: int = 10, ident: int = 0, on_server_started: Callable[[int], None] = None, **kwargs):
        """
        启动 gRPC 服务器

        :param concurrency: 并发
        :param ident: 监听端口, 默认随机
        :param on_server_started: 当服务启动完后的会回调, 如果是随机端口, 可以使用这个回调来获取端口, 或者链接其他服务
        :return:
        """
        # 使用 grpc.aio.server 启动异步服务器
        _executor = futures.ThreadPoolExecutor(max_workers=concurrency)
        setattr(self, "_executor", _executor)
        server = grpc.aio.server(_executor)

        # 将您的业务服务实例添加到 gRPC 服务器。

        pb2_grpc.add_generic_servicer_to_server(self, server)
        ident = server.add_insecure_port(f'[::]:{ident}')
        logger.info(f"gRPC Server started on port {ident} (insecure)...")
        await server.start()
        try:
            on_server_started and on_server_started(ident)
            await server.wait_for_termination()
        except KeyboardInterrupt:
            # 当接收到 Ctrl+C 时，优雅地关闭服务器
            print("Server shutdown initiated.")
            await server.stop(grace=5)  # 允许5秒钟的优雅关闭时间


# ----------------- gRPC 客户端 -----------------

class Client(BaseRpcClient):
    """
    一个辅助类，用于简化对通用 gRPC JSON 服务的客户端调用。
    """

    def __init__(self, endpoint: str):
        self.channel = grpc.insecure_channel(endpoint)
        self.stub = pb2_grpc.GenericStub(self.channel)

    def rpc(self, method: str, *args, **kwargs) -> RpcResponse:
        """


        :param method:
        :param args:
        :param kwargs:
        :return:
        """

        rpc_request = self._prepare_request(method, *args, **kwargs)

        generic_request = pb2.Request(
            method=rpc_request.method,
            data=rpc_request.data,
            request_id=rpc_request.request_id
        )

        response: pb2.Response = self.stub.rpc(generic_request)
        return RpcResponse(
            data=response.data,
            message=response.message,
            code=response.code,
            request_id=response.request_id
        )
