# -*- coding: utf-8 -*-
# @Desc    : CLI 入口
import argparse
import inspect
import os
import sys

from loguru import logger


def _find_spider_class(module):
    """从模块中查找 Spider 子类"""
    from bricks.spider import air

    candidates = []
    for name in dir(module):
        obj = getattr(module, name)
        if (
            isinstance(obj, type)
            and issubclass(obj, air.Spider)
            and obj is not air.Spider
            and obj.__module__ == module.__name__
        ):
            candidates.append(obj)

    if not candidates:
        return None

    if len(candidates) == 1:
        return candidates[0]

    # 多个候选时按源代码行号排序取第一个
    candidates.sort(key=lambda c: inspect.getsourcelines(c)[1])
    return candidates[0]


def _load_spider(filepath, concurrency=None):
    """加载爬虫文件并返回实例化的 Spider"""
    from bricks.utils import pandora

    module = pandora.load_objects(filepath)
    cls = _find_spider_class(module)
    if cls is None:
        logger.error(f"未在 {filepath} 中找到 Spider 子类")
        sys.exit(1)

    kwargs = {}
    if concurrency is not None:
        kwargs["concurrency"] = concurrency

    return cls(**kwargs)


def cmd_new(args):
    """生成爬虫文件"""
    name = args.name
    form = args.type
    output = args.output or f"./{name}.py"
    curl = args.curl

    if curl:
        from bricks.utils.convert import curl2spider
        curl2spider(curl=curl, path=output, name=name, form=form)
    else:
        tpl_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)), "tpls", "spider", form + ".tpl"
        )
        if not os.path.exists(tpl_path):
            logger.error(f"模板文件不存在: {tpl_path}")
            sys.exit(1)

        with open(tpl_path) as f:
            tpl = f.read()

        content = tpl.format(
            **{
                "SPIDER": name,
                "URL": "https://example.com",
                "METHOD": "GET",
                "PARAMS": None,
                "BODY": None,
                "HEADERS": None,
                "COOKIES": None,
                "OPTIONS": None,
                "ALLOW_REDIRECTS": True,
                "PROXIES": None,
                "PROXY": None,
                "MAX_RETRY": 5,
                "USE_SESSION": False,
            }
        )

        target_dir = os.path.dirname(output)
        if target_dir and not os.path.exists(target_dir):
            os.makedirs(target_dir, exist_ok=True)

        with open(output, "w") as f:
            f.write(content)

        logger.debug(f"生成成功, 路径为: {output}")


def cmd_run(args):
    """运行爬虫"""
    spider = _load_spider(args.file, concurrency=args.concurrency)
    spider.run(task_name=args.task)


def cmd_serve(args):
    """启动 RPC 服务"""
    from bricks.client.runner import RpcProxy

    spider = _load_spider(args.file, concurrency=args.concurrency)
    proxy = RpcProxy(spider.run)
    proxy.bind(spider)
    proxy.register_adapter("get_stats", lambda: {
        "number_of_total_requests": spider.number_of_total_requests.value,
        "number_of_failure_requests": spider.number_of_failure_requests.value,
        "number_of_new_seeds": spider.number_of_new_seeds.value,
        "number_of_seeds_obtained": spider.number_of_seeds_obtained.value,
        "number_of_seeds_pending": spider.number_of_seeds_pending,
    })
    proxy.start(mode=args.mode, ident=args.port)


def cmd_status(args):
    """查询 RPC 服务状态"""
    from bricks.rpc.http_.service import Client

    endpoint = f"{args.host}:{args.port}"
    client = Client(endpoint)

    try:
        resp = client.rpc("PING")
        print(f"连接成功: {endpoint}")
        print(f"PING -> {resp.data}")
    except Exception as e:
        logger.error(f"无法连接到 {endpoint}: {e}")
        sys.exit(1)

    try:
        resp = client.rpc("get_stats")
        stats = resp.data
        if isinstance(stats, dict):
            print("\n--- 运行统计 ---")
            for key, value in stats.items():
                print(f"  {key}: {value}")
        else:
            print(f"统计数据: {stats}")
    except Exception as e:
        logger.warning(f"获取统计数据失败: {e}")


def main():
    parser = argparse.ArgumentParser(
        prog="bricks",
        description="Bricks 爬虫框架 CLI",
    )
    subparsers = parser.add_subparsers(dest="command")

    # bricks new
    p_new = subparsers.add_parser("new", help="生成爬虫模板文件")
    p_new.add_argument("name", help="爬虫类名")
    p_new.add_argument("-t", "--type", default="air", choices=["air", "form", "template"], help="模板类型 (默认: air)")
    p_new.add_argument("--curl", help="curl 命令字符串，自动填充请求配置")
    p_new.add_argument("-o", "--output", help="输出路径 (默认: ./<name>.py)")

    # bricks run
    p_run = subparsers.add_parser("run", help="运行爬虫")
    p_run.add_argument("file", help="爬虫 Python 文件路径")
    p_run.add_argument("-c", "--concurrency", type=int, help="并发数")
    p_run.add_argument("--task", default="all", help="任务名 (默认: all)")

    # bricks serve
    p_serve = subparsers.add_parser("serve", help="启动 RPC 服务")
    p_serve.add_argument("file", help="爬虫文件路径")
    p_serve.add_argument("-m", "--mode", default="http", choices=["http", "websocket", "socket", "grpc", "redis"], help="RPC 模式 (默认: http)")
    p_serve.add_argument("-p", "--port", type=int, default=8080, help="端口 (默认: 8080)")
    p_serve.add_argument("-c", "--concurrency", type=int, help="并发数")

    # bricks status
    p_status = subparsers.add_parser("status", help="查询 RPC 服务状态")
    p_status.add_argument("--host", default="localhost", help="主机 (默认: localhost)")
    p_status.add_argument("-p", "--port", type=int, default=8080, help="端口 (默认: 8080)")

    args = parser.parse_args()

    handlers = {
        "new": cmd_new,
        "run": cmd_run,
        "serve": cmd_serve,
        "status": cmd_status,
    }

    handler = handlers.get(args.command)
    if handler:
        handler(args)
    else:
        parser.print_help()
