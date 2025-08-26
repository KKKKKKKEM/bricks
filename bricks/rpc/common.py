import asyncio
import inspect
import json
import uuid
from concurrent import futures
from typing import Any, Dict, Optional, Callable, Literal, Tuple

from loguru import logger

from bricks.utils import pandora


class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        """
        重写 default 方法来处理不能被标准 JSON 编码器序列化的对象。
        """
        # 1. 检查对象是否有 'on_json_dump' 方法
        if hasattr(obj, 'on_json_dump') and callable(obj.on_json_dump):
            try:
                # 尝试调用 on_json_dump 方法并返回其结果
                return obj.on_json_dump()
            except TypeError:
                # 如果 on_json_dump 方法本身返回了不可序列化的内容，
                # 或者调用 on_json_dump 失败，就退化到 str()
                logger.warning(
                    f"WARNING: on_json_dump() for {type(obj).__name__} failed or returned un_serializable data, falling back to str().")
                return str(obj)

        # 2. 如果没有 'on_json_dump' 方法，或者 on_json_dump 处理失败，则将其转换为字符串
        # 这一步会捕获所有其他未能被 json 默认处理的对象，例如 SimpleObject
        try:
            # 尝试直接使用父类的 default 方法处理，如果可以，就让它处理
            # 这对于那些可能被其他自定义编码器处理，或者将来 json 模块更新后能处理的类型是有益的。
            # 但是，根据题意，我们应该在不能序列化时直接降级为 str
            return super().default(obj)
        except TypeError:
            # 如果父类 default 也处理不了（即对象类型不是 JSONEncoder 知道如何处理的），
            # 那么就退化到 str()
            logger.warning(
                f"DEBUG: Object of type {type(obj).__name__} cannot be serialized by default JSON encoder, falling back to str().")
            return str(obj)


class MultiObjectProxy:
    """多对象代理类，支持按顺序查找和类名.方法名调用"""

    def __init__(self, objects: Tuple[Any]):
        self.objects = []
        self._method_cache = {}
        self._class_map = {}  # 类名到对象的映射

        for obj in objects:
            if inspect.isfunction(obj):
                # 函数包装成类
                wrapper_name = f"{obj.__name__.title()}Wrapper"
                wrapper = type(wrapper_name, (), {obj.__name__: staticmethod(obj)})
                instance = wrapper()
                self.objects.append(instance)
                self._class_map[wrapper_name] = instance
                self._class_map[obj.__name__] = instance  # 也支持函数名作为类名

            elif inspect.isclass(obj):
                # 类实例化
                instance = obj()
                self.objects.append(instance)
                self._class_map[obj.__name__] = instance

            else:
                # 已经是实例
                self.objects.append(obj)
                self._class_map[obj.__class__.__name__] = obj

    def __getattr__(self, name):
        # 先检查缓存
        if name in self._method_cache:
            return self._method_cache[name]

        # 检查是否为 className.method 格式
        if '.' in name:
            class_name, method_name = name.split('.', 1)
            if class_name in self._class_map:
                obj = self._class_map[class_name]
                if hasattr(obj, method_name):
                    method = getattr(obj, method_name)
                    if callable(method):
                        self._method_cache[name] = method
                        return method
            raise AttributeError(f"Method '{method_name}' not found in class '{class_name}'")

        # 按顺序查找方法（原有逻辑）
        for obj in self.objects:
            if hasattr(obj, name):
                method = getattr(obj, name)
                if callable(method):
                    self._method_cache[name] = method
                    return method

        raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{name}'")

    def get_available_methods(self):
        """获取所有可用的方法列表"""
        methods = {}
        for obj in self.objects:
            class_name = obj.__class__.__name__
            obj_methods = [attr for attr in dir(obj)
                           if callable(getattr(obj, attr)) and not attr.startswith('_')]
            methods[class_name] = obj_methods
        return methods


class RpcRequest:
    """统一的 RPC 请求数据结构"""

    def __init__(self, method: str, data: str, request_id: str = None):
        self.method = method
        self.data = data
        self.request_id = request_id or str(uuid.uuid4())

    def to_dict(self) -> Dict[str, str]:
        return {
            "method": self.method,
            "data": self.data,
            "request_id": self.request_id
        }

    @classmethod
    def from_dict(cls, data: Dict[str, str]) -> "RpcRequest":
        return cls(
            method=data["method"],
            data=data.get("data"),
            request_id=data.get("request_id")
        )

    def to_json(self):
        return json.dumps(self.to_dict(), cls=CustomJSONEncoder, ensure_ascii=False)


class RpcResponse:
    """统一的 RPC 响应数据结构"""

    def __init__(self, data: str = "", message: str = "", code: int = 0, request_id: str = "", decode_response= False):
        self.data = data if not decode_response else pandora.json_or_eval(data)
        self.message = message
        self.code = code
        self.request_id = request_id

    def to_dict(self) -> Dict[str, Any]:
        return {
            "data": self.data,
            "message": self.message,
            "code": self.code,
            "request_id": self.request_id
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any], decode_response= False) -> "RpcResponse":
        return cls(
            data=data.get("data", ""),
            message=data.get("message", ""),
            code=data.get("code", 0),
            request_id=data.get("request_id", ""),
            decode_response=decode_response
        )

    @staticmethod
    def ensure_str(data):
        if isinstance(data, str):
            return data
        else:
            return json.dumps(data, cls=CustomJSONEncoder, ensure_ascii=False)

    def __str__(self):
        return f"RpcResponse(data={self.data}, message={self.message}, code={self.code}, request_id={self.request_id})"


class BaseRpcService:
    """RPC 服务基类，提供统一的请求处理逻辑"""

    def __init__(self):
        self._executor: Optional[futures.ThreadPoolExecutor] = None
        self._targe: Any = None

    async def process_rpc_request(self, request: RpcRequest) -> RpcResponse:
        """
        处理 RPC 请求的核心逻辑，从 gRPC 实现中提取
        """
        method_name = request.method
        request_id = request.request_id
        target = self._targe or self
        try:
            if method_name == "PING":
                return RpcResponse(
                    message="success",
                    data="PONG",
                    request_id=request_id
                )

            # 1. 检查方法是否存在且已注册
            if not hasattr(target, method_name):
                logger.error(f"Error: Method '{method_name}' not found or not registered.")
                return RpcResponse(
                    message=f"Method '{method_name}' not found.",
                    code=404,
                    request_id=request_id
                )

            # 2. 获取并调用实际的业务方法
            handler_method = getattr(target, method_name)

            # 3. 解析请求 JSON 负载
            try:
                payload = json.loads(request.data) if isinstance(request.data, str) else (request.data or {})

            except json.JSONDecodeError as e:
                error_msg = f"Invalid JSON payload: {e}"
                return RpcResponse(
                    message=error_msg,
                    code=400,
                    request_id=request_id
                )

            if not isinstance(payload, dict):
                error_msg = f"Expected request data to decode to a dictionary, but got {type(payload).__name__}"
                return RpcResponse(
                    message=error_msg,
                    code=400,
                    request_id=request_id
                )

            args = payload.get("args", [])
            kwargs = payload.get("kwargs", {})

            prepared = pandora.prepare(handler_method, args=args, kwargs=kwargs, namespace={})

            # 判断业务方法是否是异步的
            if inspect.iscoroutinefunction(handler_method):
                result = await prepared.func(*prepared.args, **prepared.kwargs)
            else:
                loop = asyncio.get_running_loop()
                result = await loop.run_in_executor(
                    self._executor,
                    lambda: prepared.func(*prepared.args, **prepared.kwargs)
                )

            return RpcResponse(
                data=json.dumps(result, cls=CustomJSONEncoder, ensure_ascii=False),
                request_id=request_id
            )

        except Exception as e:
            error_msg = f"Internal server error processing '{method_name}' with payload '{request.data}': {e}"
            logger.error(error_msg)

            # 根据异常类型设置不同的状态码
            if isinstance(e, (ValueError, TypeError, json.JSONDecodeError)):
                status_code = 400
            elif isinstance(e, NotImplementedError):
                status_code = 501
            else:
                status_code = 500

            return RpcResponse(
                message=str(e),
                code=status_code,
                request_id=request_id
            )

    async def serve(
            self,
            concurrency: int = 10,
            ident: Any = 0,
            on_server_started: Callable[[any], None] = None,
            **kwargs
    ):
        """
        启动服务器的抽象方法，子类需要实现
        """
        raise NotImplementedError("Subclasses must implement serve method")

    def bind_target(self, target: Any):
        self._targe = target


class BaseRpcClient:
    """RPC 客户端基类，提供统一的调用接口"""

    def rpc(self, method: str, *args, **kwargs) -> Any:
        """
        统一的 RPC 调用接口
        """
        raise NotImplementedError("Subclasses must implement rpc method")

    @staticmethod
    def _prepare_request(method: str, *args, **kwargs) -> RpcRequest:
        """准备 RPC 请求"""
        request_id = kwargs.pop("request_id", None) or str(uuid.uuid4())
        try:
            payload = json.dumps({"args": args, "kwargs": kwargs})
        except TypeError as e:
            raise ValueError(f"Request data is not JSON serializable: {e}")

        return RpcRequest(
            method=method,
            data=payload,
            request_id=request_id
        )


MODE = Literal["http", "websocket", "socket", "grpc", "redis"]


def serve(
        *obj: Any,
        mode: MODE = "http",
        concurrency: int = 10,
        ident: Any = 0,
        on_server_started: Callable[[Any], None] = None,
        **kwargs
):
    asyncio.run(
        start_rpc_server(
            *obj,
            mode=mode,
            concurrency=concurrency,
            ident=ident,
            on_server_started=on_server_started,
            **kwargs
        )
    )


async def start_rpc_server(
        *obj: Any,
        mode: MODE = "http",
        concurrency: int = 10,
        ident: Any = 0,
        on_server_started: Callable[[Any], None] = None,
        **kwargs
):
    if mode == "http":
        from bricks.rpc.http_ import service
    elif mode == "websocket":
        from bricks.rpc.websocket_ import service
    elif mode == "socket":
        from bricks.rpc.socket_ import service
    elif mode == "grpc":
        from bricks.rpc.grpc_ import service
    elif mode == "redis":
        from bricks.rpc.redis_ import service
    else:
        raise ValueError(f"不支持的模式: {mode}")

    proxy = MultiObjectProxy(obj)


    server: BaseRpcService = service.Service()
    server.bind_target(target=proxy)
    try:
        await server.serve(concurrency=concurrency, ident=ident, on_server_started=on_server_started, **kwargs)
    except (KeyboardInterrupt, SystemExit):
        return
