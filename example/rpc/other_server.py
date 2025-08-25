import threading

from bricks.rpc.grpc_.service import Client
from bricks import rpc


class Class1:
    def execute(self, seeds: dict):
        return {"seeds": seeds, "from": "Class1 class"}


class Class2:
    def execute(self, seeds: dict):
        return {"seeds": seeds, "from": "Class2 class"}


def execute(seeds: dict):
    return {"seeds": seeds, "data": "demo func"}


def execute2(seeds: dict):
    return {"seeds": seeds, "data": "demo func2"}


def test(port):
    client = Client(f"localhost:{port}")
    print(client.rpc("Class1.execute", {"page": 1}))
    print(client.rpc("Class2.execute", {"page": 1}))
    print(client.rpc("execute", {"page": 1}))
    print(client.rpc("execute2", {"page": 1}))

def on_server_started(port: int):
    print(f"Server started on port {port}")
    threading.Thread(target=test, args=(port,)).start()



if __name__ == '__main__':
    rpc.serve(execute2, execute, Class1(), Class2(), mode="grpc", on_server_started=on_server_started, ident=8888)
