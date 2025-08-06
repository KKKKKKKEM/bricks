import asyncio
import json
import socket
import struct
from concurrent import futures
from typing import Callable

from loguru import logger

from bricks.rpc.common import BaseRpcService, BaseRpcClient, RpcRequest, RpcResponse


class SocketProtocol:
    """Socket 协议处理，使用长度前缀解决粘包问题"""
    
    @staticmethod
    def pack_message(message: str) -> bytes:
        """打包消息：4字节长度 + 消息内容"""
        message_bytes = message.encode('utf-8')
        length = len(message_bytes)
        return struct.pack('!I', length) + message_bytes
    
    @staticmethod
    async def unpack_message(reader: asyncio.StreamReader) -> str:
        """解包消息：先读取4字节长度，再读取消息内容"""
        # 读取消息长度
        length_data = await reader.readexactly(4)
        length = struct.unpack('!I', length_data)[0]
        
        # 读取消息内容
        message_data = await reader.readexactly(length)
        return message_data.decode('utf-8')


class Service(BaseRpcService):
    """
    基于 Socket 的 RPC 服务。
    任何业务逻辑服务只需继承此类，并实现其业务方法。
    """

    def __init__(self):
        super().__init__()
        self.server = None

    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """
        处理 Socket 客户端连接
        """
        client_address = writer.get_extra_info('peername')
        logger.info(f"Socket client connected: {client_address}")
        
        try:
            while True:
                try:
                    # 接收消息
                    message = await SocketProtocol.unpack_message(reader)
                    
                    # 解析请求数据
                    request_data = json.loads(message)
                    rpc_request = RpcRequest.from_dict(request_data)
                    
                    # 处理请求
                    rpc_response = await self.process_rpc_request(rpc_request)
                    
                    # 发送响应
                    response_message = json.dumps(rpc_response.to_dict())
                    response_data = SocketProtocol.pack_message(response_message)
                    
                    writer.write(response_data)
                    await writer.drain()
                    
                except asyncio.IncompleteReadError:
                    # 客户端断开连接
                    break
                except json.JSONDecodeError:
                    error_response = RpcResponse(
                        message="Invalid JSON in message",
                        code=400
                    )
                    response_message = json.dumps(error_response.to_dict())
                    response_data = SocketProtocol.pack_message(response_message)
                    writer.write(response_data)
                    await writer.drain()
                except Exception as e:
                    logger.error(f"Error handling Socket message: {e}")
                    error_response = RpcResponse(
                        message=f"Internal server error: {e}",
                        code=500
                    )
                    response_message = json.dumps(error_response.to_dict())
                    response_data = SocketProtocol.pack_message(response_message)
                    writer.write(response_data)
                    await writer.drain()
                    
        except Exception as e:
            logger.error(f"Error in client handler: {e}")
        finally:
            logger.info(f"Socket client disconnected: {client_address}")
            writer.close()
            await writer.wait_closed()

    async def serve(self, concurrency: int = 10, ident: int = 0, on_server_started: Callable[[int], None] = None, **kwargs):
        """
        启动 Socket RPC 服务器

        :param concurrency: 并发数
        :param ident: 监听端口，默认随机
        :param on_server_started: 当服务启动完后的回调
        """
        # 设置线程池
        self._executor = futures.ThreadPoolExecutor(max_workers=concurrency)
        
        # 启动 Socket 服务器
        self.server = await asyncio.start_server(
            self.handle_client,
            '0.0.0.0',
            ident
        )
        
        # 获取实际端口
        actual_port = self.server.sockets[0].getsockname()[1]
        logger.info(f"Socket RPC Server started on port {actual_port}...")
        
        try:
            if on_server_started:
                on_server_started(actual_port)
            
            # 保持服务器运行
            await self.server.serve_forever()
            
        except KeyboardInterrupt:
            logger.info("Socket RPC Server shutdown initiated.")
        finally:
            self.server.close()
            await self.server.wait_closed()


class Client(BaseRpcClient):
    """
    Socket RPC 客户端（同步实现）
    """

    def __init__(self, endpoint: str):
        """
        """
        host, port = endpoint.split(":")
        self.host = host
        self.port = int(port)
        self.socket = None
        self._connected = False

    def connect(self):
        """建立 Socket 连接"""
        if self._connected:
            return

        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.connect((self.host, self.port))
            self._connected = True
            logger.info(f"Connected to Socket server: {self.host}:{self.port}")

        except Exception as e:
            if self.socket:
                self.socket.close()
                self.socket = None
            raise RuntimeError(f"Failed to connect to Socket server: {e}")

    def _send_data(self, data: bytes):
        """发送数据"""
        total_sent = 0
        while total_sent < len(data):
            sent = self.socket.send(data[total_sent:])
            if sent == 0:
                raise RuntimeError("Socket connection broken")
            total_sent += sent

    def _recv_data(self, length: int) -> bytes:
        """接收指定长度的数据"""
        data = b''
        while len(data) < length:
            chunk = self.socket.recv(length - len(data))
            if not chunk:
                raise RuntimeError("Socket connection broken")
            data += chunk
        return data

    @staticmethod
    def _pack_message(message: str) -> bytes:
        """打包消息：4字节长度 + 消息内容"""
        message_bytes = message.encode('utf-8')
        length = len(message_bytes)
        return struct.pack('!I', length) + message_bytes

    def _unpack_message(self) -> str:
        """解包消息：先读取4字节长度，再读取消息内容"""
        # 读取消息长度
        length_data = self._recv_data(4)
        length = struct.unpack('!I', length_data)[0]

        # 读取消息内容
        message_data = self._recv_data(length)
        return message_data.decode('utf-8')

    def rpc(self, method: str, *args, **kwargs):
        """
        同步 RPC 调用接口
        """
        # 确保连接已建立
        if not self._connected:
            self.connect()

        # 准备请求
        rpc_request = self._prepare_request(method, *args, **kwargs)

        try:
            # 发送请求
            request_message = json.dumps(rpc_request.to_dict())
            request_data = self._pack_message(request_message)
            self._send_data(request_data)

            # 接收响应
            response_message = self._unpack_message()
            response_data = json.loads(response_message)

            # 处理响应
            return RpcResponse.from_dict(response_data)

        except Exception as e:
            # 连接可能已断开，重置状态
            self._connected = False
            if self.socket:
                self.socket.close()
                self.socket = None
            raise RuntimeError(f"Socket RPC call failed for method '{method}': {e}")

    def close(self):
        """关闭 Socket 连接"""
        if self.socket:
            try:
                self.socket.close()
            except:
                pass
            self.socket = None
            self._connected = False

    def __del__(self):
        """析构函数，确保连接被关闭"""
        self.close()
