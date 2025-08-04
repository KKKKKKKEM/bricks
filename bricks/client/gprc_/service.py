from bricks.utils import pandora

pandora.require("grpcio==1.74.0")

import json
from concurrent import futures
from typing import Type, Dict, Any

import grpc
from loguru import logger

from bricks.spider import air
from bricks.spider.addon import Rpc
from bricks.client.gprc_.generated import service_pb2, service_pb2_grpc


class GrpcService:
    """将 Spider 包装成 gRPC 服务"""

    def __init__(self, spider_class: Type[air.Spider], attrs: Dict[str, Any] = None):
        self.spider_class = spider_class
        self.attrs = attrs or {}
        self.rpc_wrapper = Rpc.wrap(self.spider_class, attrs=self.attrs)

    def execute(self, request, context):
        """处理种子并返回结果"""
        try:
            seeds = json.loads(request.data)
            # 提交种子到 RPC 包装器
            result = self.rpc_wrapper.execute(seeds)
            future_type = seeds.get("$futureType", "$response")
            if future_type == "$request":
                data = {"success": True, "data": result.request.url}
            elif future_type == "$response":
                data = {"success": True, "data": result.response.text}
            elif future_type == "$items":
                data = {"success": True, "data": result.items.data}
            else:
                data = {"success": False, "data": "unkown futureType"}
        except Exception as e:
            logger.error(f"execute error: {e}")
            data = {"success": False, "error": str(e)}

        return service_pb2.Response(
            success=data["success"],
            data=json.dumps(data.get("data", {})),
            error=data.get("error", "")
        )

    def submit(self, request, context):
        """提交种子到队列"""
        try:
            seeds = json.loads(request.data)
            self.rpc_wrapper.submit(seeds)
            data = {"success": True}
        except Exception as e:
            logger.error(f"submit error: {e}")
            data = {"success": False, "error": str(e)}

        return service_pb2.Response(
            success=data["success"],
            data="",
            error=data.get("error", "")
        )

    @staticmethod
    def health(request, context):
        """健康检查"""
        try:

            return service_pb2.Response(
                success=True,
                data="",
                error=""
            )

        except Exception as e:
            logger.error(f"health error: {e}")

            return service_pb2.Response(
                success=False,
                error=str(e)
            )

    @staticmethod
    def serve(
            spider_class: Type[air.Spider],
            attrs: dict = None,
            port: int = 50051,
            concurrency: int = 10,
    ):
        """启动 gRPC Spider 服务"""

        attrs = attrs or {}
        concurrency = attrs.get("concurrency", concurrency)

        server = grpc.server(futures.ThreadPoolExecutor(max_workers=concurrency))
        # 添加服务
        service_pb2_grpc.add_servicer_to_server(
            GrpcService(spider_class, attrs or None),
            server
        )

        listen_addr = f'[::]:{port}'
        server.add_insecure_port(listen_addr)

        logger.info(f"gRPC Spider server listening on {listen_addr}")
        server.start()
        server.wait_for_termination()
