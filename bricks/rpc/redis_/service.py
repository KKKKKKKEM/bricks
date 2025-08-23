import asyncio
import json
import uuid
from concurrent import futures
from typing import Callable, Optional
from urllib.parse import urlparse, parse_qs

import redis
import redis.asyncio as async_redis
from loguru import logger

from bricks.rpc.common import BaseRpcService, BaseRpcClient, RpcRequest, RpcResponse


def _parse_redis_endpoint(endpoint: str):
    """
    解析 Redis 连接字符串
    格式: redis://[username:password@]host:port[/database][?server_id=xxx&other_params=yyy]
    """
    if not endpoint.startswith("redis://"):
        endpoint = "redis://" + endpoint

    parsed = urlparse(endpoint)

    # 解析查询参数
    query_params = parse_qs(parsed.query)

    config = {
        "host": parsed.hostname or "127.0.0.1",
        "port": parsed.port or 6379,
        "username": parsed.username,
        "password": parsed.password,
        "db": int(parsed.path.lstrip('/')) if parsed.path and parsed.path != '/' else 0,
    }


    # 移除 None 值
    config = {k: v for k, v in config.items() if v is not None}

    query_params = {k: v[0] for k, v in query_params.items()}
    return config, query_params


class Service(BaseRpcService):
    """
    基于 Redis 的 RPC 服务。
    使用 Redis 列表作为请求队列，键值对作为响应存储。
    """

    def __init__(self, ):
        """
        初始化 Redis RPC 服务

        """
        super().__init__()
        self._running = False
        self.input_key = "rpc:request"
        self.output_key = "rpc:response"
        self.key_type = "list"

    async def _connect_redis(self, endpoint: str, **kwargs):
        """
        连接到 Redis

        :param endpoint: Redis 连接字符串，格式: redis://[username:password@]host:port[/database][?server_id=xxx]
        :param kwargs: 其他 Redis 连接参数

        """
        # 解析连接字符串


        self.redis_config, options = _parse_redis_endpoint(endpoint)
        self.input_key = options.get("input_key") or kwargs.pop("input_key", "rpc:request")
        self.output_key = options.get("output_key") or kwargs.pop("output_key", "rpc:response")
        self.key_type = options.get("key_type") or kwargs.pop("key_type", "list")
        self.server_id = options.get("server_id") or kwargs.pop("server_id", str(uuid.uuid4())) or str(uuid.uuid4())

        self.redis_config.update(kwargs)
        self.server_id = options.get("server_id") or str(uuid.uuid4())
        self.redis = async_redis.Redis(**self.redis_config)
        self.input_key = f"{self.input_key}:{self.server_id}"
        self.output_key = f"{self.output_key}:{self.server_id}"

        # 测试连接
        await self.redis.ping()
        logger.info(f"Connected to Redis: {self.redis_config['host']}:{self.redis_config['port']}")

    async def _listen_for_requests(self):
        """
        监听 Redis 队列中的请求
        """
        logger.info(f"Redis RPC Server listening on queue: {self.input_key}")

        while self._running:
            try:
                # 使用 BLPOP 阻塞式监听请求，超时时间为 1 秒
                result = await self.redis.blpop([self.input_key], timeout=5)

                if result:
                    _, request_data = result
                    # 异步处理请求
                    await self._handle_request(request_data)

            except asyncio.TimeoutError:
                # 超时是正常的，继续循环
                continue
            except Exception as e:
                if self._running:  # 只有在服务运行时才记录错误
                    logger.error(f"Error listening for Redis RPC requests: {e}")
                    await asyncio.sleep(1)  # 避免快速重试

    async def _handle_request(self, request_data: str):
        """
        处理单个请求
        """
        try:
            # 解析请求
            request_dict = json.loads(request_data)
            rpc_request = RpcRequest.from_dict(request_dict)

            # 处理请求
            rpc_response = await self.process_rpc_request(rpc_request)

            # 将响应存储到 Redis，使用 request_id 作为键
            response_key = f"{self.output_key}:{rpc_request.request_id}"
            response_data = json.dumps(rpc_response.to_dict())
            await self.redis.lpush(response_key, response_data)
            # 设置响应数据，并设置 30 秒过期时间
            await self.redis.expire(response_key, 30)

        except Exception as e:
            logger.error(f"Error handling Redis RPC request: {e}")

    async def serve(
            self,
            concurrency: int = 10,
            ident: str = "redis://127.0.0.1:6379/0",
            on_server_started: Callable[[str], None] = None,
            **kwargs
    ):
        """
        启动 Redis RPC 服务器

        :param concurrency: 并发数（用于线程池）
        :param ident: 端口参数（Redis RPC 不使用，保持接口一致性）
        :param on_server_started: 服务启动回调
        """
        # 设置线程池
        self._executor = futures.ThreadPoolExecutor(max_workers=concurrency)

        # 连接到 Redis
        await self._connect_redis(ident, **kwargs)

        # # 清理可能存在的旧请求队列
        # await self.redis.delete(self.request_queue)

        self._running = True

        logger.info(f"Redis RPC Server started with server_id: {self.server_id}")

        if on_server_started:
            on_server_started(self.server_id)

        try:
            # 开始监听请求
            await self._listen_for_requests()
        except KeyboardInterrupt:
            logger.info("Redis RPC Server shutdown initiated.")
        finally:
            self._running = False
            # 清理请求队列
            if self.redis:
                await self.redis.delete(self.input_key)
                await self.redis.close()


class Client(BaseRpcClient):
    """
    Redis RPC 客户端
    """

    def __init__(self, endpoint: str, **kwargs):
        """
        初始化 Redis RPC 客户端

        :param endpoint: Redis 连接字符串，格式: redis://[username:password@]host:port[/database][?server_id=xxx]
        :param kwargs: 其他 Redis 连接参数
        """
        super().__init__()

        # 解析连接字符串
        self.redis_config, options = _parse_redis_endpoint(endpoint)
        self.input_key = options.get("input_key") or kwargs.pop("input_key", "rpc:request")
        self.output_key = options.get("output_key") or kwargs.pop("output_key", "rpc:response")

        server_id = options.get("server_id") or kwargs.pop("server_id")
        if not server_id:
            raise ValueError("server_id is required in endpoint or as parameter")

        self.redis_config.update(kwargs)
        self.redis_config.update(decode_responses=True)
        self.key_prefix = self.redis_config.pop("key_prefix", "rpc")


        self.server_id = server_id
        self.output_key = f"{self.output_key}:{self.server_id}"
        self.redis: Optional[redis.Redis] = None
        self._connected = False

    def _ensure_connected(self):
        """
        确保 Redis 连接已建立
        """
        if not self._connected or not self.redis:
            self.redis = redis.Redis(**self.redis_config)
            self.redis.ping()
            self._connected = True

    def rpc(self, method: str, *args, timeout: int = 30, **kwargs) -> RpcResponse:
        """
        同步 RPC 调用接口

        :param method: 方法名
        :param args: 位置参数
        :param timeout: 超时时间（秒）
        :param kwargs: 关键字参数
        :return: RPC 响应
        """
        # 确保连接
        self._ensure_connected()

        # 准备请求
        rpc_request = self._prepare_request(method, *args, **kwargs)

        try:
            # 发送请求到服务器队列
            request_data = json.dumps(rpc_request.to_dict())
            self.redis.rpush(self.output_key, request_data)

            # 等待响应
            response_key = f"{self.key_prefix}:response:{rpc_request.request_id}"
            result = self.redis.blpop([response_key], timeout)
            if result:
                _, request_data = result
                return RpcResponse.from_dict(json.loads(str(request_data)))

        except Exception as e:
            raise RuntimeError(f"Redis RPC call failed for method '{method}': {e}")

    def close(self):
        """
        关闭 Redis 连接
        """
        if self.redis:
            self.redis.close()
            self._connected = False
