import time

from loguru import logger

from bricks.client.runner import RpcProxy

"""
使用这段代码在根目录创建一个 client

from bricks.client.manage import Manager

if __name__ == '__main__':
    m = Manager()
    m.run()


"""

# python manager.py run_task -m example.cmd.rpc_task.rpc_func -rpc mode=http
def rpc_func(rpc: RpcProxy = None):
    print(f"rpc: {rpc}")

    while True:
        logger.debug(time.time())
        time.sleep(5)

# python manager.py run_task -m example.cmd.rpc_task.rpc_class -rpc mode=http
class RpcClass:
    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def run(self):
        rpc_func()

    def info(self):
        return {
            "name": "RpcClass",
            "version": "1.0.0",
            "kwargs": self.kwargs,
        }


def rpc_class(rpc: RpcProxy = None, **kwargs):
    ins = RpcClass()
    rpc.bind(ins)
    ins.run()
