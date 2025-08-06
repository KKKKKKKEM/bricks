


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

if __name__ == '__main__':
    test_grpc(64880)