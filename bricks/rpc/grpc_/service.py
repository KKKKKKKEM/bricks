import asyncio
import uuid
from concurrent import futures
from typing import Callable

from loguru import logger

from bricks.utils import pandora

pandora.require("grpcio==1.74.0")

import inspect
import json

import grpc

# 导入通用 gRPC 生成的代码
from bricks.rpc.grpc_ import generic_pb2 as pb2
from bricks.rpc.grpc_ import generic_pb2_grpc as pb2_grpc


# ----------------- 通用 gRPC 服务基类 -----------------

class GrpcService(pb2_grpc.GenericServicer):
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

        try:
            # 1. 检查方法是否存在且已注册
            if not hasattr(self, method_name):
                logger.error(f"Error: Method '{method_name}' not found or not registered.")
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details(f"Method '{method_name}' not found on server.")
                return pb2.Response(
                    message=f"Method '{method_name}' not found.",
                    code=grpc.StatusCode.NOT_FOUND.value,
                    request_id=request_id
                )

            # 2. 获取并调用实际的业务方法
            handler_method = getattr(self, method_name)  # 获取类实例上的方法

            # 3. 解析请求 JSON 负载
            try:
                payload = json.loads(request.data)
            except json.JSONDecodeError as e:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details(f"Invalid JSON payload: {e}")
                raise ValueError(f"Invalid JSON payload: {e}")

            if not isinstance(payload, dict):
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details(
                    f"Expected request data to decode to a dictionary, but got {type(payload).__name__}")
                raise TypeError(
                    f"Expected request data to decode to a dictionary, but got {type(payload).__name__}")

            args = payload.get("args", [])
            kwargs = payload.get("kwargs", {})

            prepared = pandora.prepare(handler_method, args=args, kwargs=kwargs, namespace={"grpc_context": context})

            # 判断业务方法是否是异步的
            if inspect.iscoroutinefunction(handler_method):
                result_data = await prepared.func(*prepared.args, **prepared.kwargs)
            else:
                loop = asyncio.get_running_loop()
                result_data = await loop.run_in_executor(
                    getattr(self, "_executor", None),
                    lambda: prepared.func(*prepared.args, **prepared.kwargs)
                )

            def to_json(obj):
                if hasattr(obj, "to_json"):
                    return obj.to_json()
                return str(obj)

            # 4. 将业务方法的返回值打包成 JSON 字符串
            packed_response_payload = json.dumps(result_data, default=to_json)

            return pb2.Response(
                data=packed_response_payload,
                request_id=request_id
            )

        except Exception as e:

            error_msg = f"Internal server error processing '{method_name}' with payload '{request.data}': {e}"
            logger.error(error_msg)
            # 根据异常类型设置不同的状态码
            if isinstance(e, ValueError) or isinstance(e, TypeError) or isinstance(e, json.JSONDecodeError):
                status_code = grpc.StatusCode.INVALID_ARGUMENT
            elif isinstance(e, NotImplementedError):
                status_code = grpc.StatusCode.UNIMPLEMENTED
            else:
                status_code = grpc.StatusCode.INTERNAL

            context.set_code(status_code)
            context.set_details(error_msg)
            return pb2.Response(message=str(e), code=500, request_id=request_id)

    async def serve(self, concurrency: int = 10, port: int = 0, on_server_started: Callable[[int], None] = None):
        """
        启动 gRPC 服务器

        :param concurrency: 并发
        :param port: 监听端口, 默认随机
        :param on_server_started: 当服务启动完后的会回调, 如果是随机端口, 可以使用这个回调来获取端口, 或者链接其他服务
        :return:
        """
        # 使用 grpc.aio.server 启动异步服务器
        _executor = futures.ThreadPoolExecutor(max_workers=concurrency)
        setattr(self, "_executor", _executor)
        server = grpc.aio.server(_executor)

        # 将您的业务服务实例添加到 gRPC 服务器。
        rpc_method_handlers = {
            'rpc': grpc.unary_unary_rpc_method_handler(
                self.rpc,
                request_deserializer=pb2.Request.FromString,
                response_serializer=pb2.Response.SerializeToString,
            ),
        }
        pb2_grpc.add_generic_servicer_to_server(rpc_method_handlers, server)
        port = server.add_insecure_port(f'[::]:{port}')
        logger.info(f"gRPC Server started on port {port} (insecure)...")
        await server.start()
        try:
            on_server_started and on_server_started(port)
            await server.wait_for_termination()
        except KeyboardInterrupt:
            # 当接收到 Ctrl+C 时，优雅地关闭服务器
            print("Server shutdown initiated.")
            await server.stop(grace=5)  # 允许5秒钟的优雅关闭时间


# ----------------- 通用 gRPC 客户端辅助类 -----------------

class GrpcClient:
    """
    一个辅助类，用于简化对通用 gRPC JSON 服务的客户端调用。
    """

    def __init__(self, endpoint: str):
        self.channel = grpc.insecure_channel(endpoint)
        self.stub = pb2_grpc.GenericStub(self.channel)

    def rpc(self, method: str, *args, **kwargs) -> dict:
        """


        :param method:
        :param args:
        :param kwargs:
        :return:
        """
        # 将请求数据打包成 JSON 字符串
        request_id = kwargs.pop("request_id", None) or str(uuid.uuid4())
        try:
            payload = json.dumps({"args": args, "kwargs": kwargs})
        except TypeError as e:
            raise ValueError(f"Request data is not JSON serializable: {e}")

        generic_request = pb2.Request(
            method=method,
            data=payload,
            request_id=request_id
        )

        response: pb2.Response = self.stub.rpc(generic_request)

        # 检查服务器是否返回了错误
        if response.message:
            raise RuntimeError(
                f"Server error for method '{method}': "
                f"{response.message} (Code: {response.code}, Request ID: {response.request_id or 'N/A'})"
            )

        # 解包响应负载
        try:
            return pandora.json_or_eval(response.data)
        except json.JSONDecodeError as e:
            raise ValueError(f"Failed to decode response JSON payload for method '{method}': {e}")
