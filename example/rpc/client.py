import json


def test_http(port: int):
    """测试 HTTP RPC 客户端"""
    from bricks.rpc.http_.service import Client
    client = Client(f"localhost:{port}")
    print(client.rpc("execute", {"page": 1}))


def test_websocket(port: int):
    """测试 WebSocket RPC 客户端"""
    from bricks.rpc.websocket_.service import Client
    client = Client(f"localhost:{port}")
    print(client.rpc("execute", {"page": 1}))

def test_socket(port: int):
    """测试 Socket RPC 客户端"""
    from bricks.rpc.socket_.service import Client
    client = Client(f"localhost:{port}")
    print(client.rpc("execute", {"page": 1}))

def test_grpc(port: int):
    """测试 gRPC 客户端"""
    from bricks.rpc.grpc_.service import Client
    client = Client(f"localhost:{port}")
    print(client.rpc("execute", {"page": 1}))

def test_redis(ident: str):
    """测试 Redis 客户端"""
    from bricks.rpc.redis_.service import Client
    client = Client(f"redis://:0boNLgeuiPIxv7@127.0.0.1:6379/0?server_id={ident}")
    print(client.rpc("execute", {"page": 1}))

if __name__ == '__main__':
    test_grpc(53548)