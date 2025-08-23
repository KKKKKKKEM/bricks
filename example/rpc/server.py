from loguru import logger

from bricks.spider.addon import Rpc
from example.rpc.spider import MySpider


def on_finish(ctx: MySpider.Context, err: Exception):
    # 当种子被消耗完成, 就会回调这个方法
    # 用法:
    # 1. 你可以将你的结果写入到数据库
    # 2. 通过 url 回调回传结果, 是用于 golang 搭建的 rpc 服务, 因为 python 的性能实在是太差了, 无法支撑高并发, 所以才有了这个方法
    if err:
        logger.debug(f"发生错误 -> {err}")
    else:
        logger.debug(f"跑完了 -> {ctx.response.text}")


if __name__ == '__main__':
    rpc = Rpc.wrap(MySpider)
    # [可选] 添加回调 -> 用于使用回传 url 回传结果 / 写入数据库
    rpc.with_callback(on_finish)

    # 开启 rpc 服务, 支持的模式如下:
    # http
    # websocket
    # socket
    # grpc
    rpc.serve(concurrency=10, mode="redis", ident="redis://:0boNLgeuiPIxv7@127.0.0.1:6379/0")
