# -*- coding: utf-8 -*-
# @Time    : 2023-12-29 11:11
# @Author  : Kem
# @Desc    : 运行器
import os
import signal
import sys
import threading
import time
from concurrent.futures import Future
from typing import Callable, Any

from loguru import logger

from bricks.client import Argv
from bricks.rpc.common import MODE, serve
from bricks.utils import pandora


class RpcProxy:

    def __init__(self, main: Callable, *args, **kwargs):
        self.object = None
        self.main = main
        self.args = args
        self.kwargs = kwargs
        self.adapters = {}

    @classmethod
    def stop(cls, delay=1):
        fu = cls.add_background_task(
            lambda: os.kill(os.getpid(), signal.SIGTERM), delay=delay
        )
        return f"即将在 {delay} 秒后停止程序, 后台任务: {fu}"

    @classmethod
    def reload(cls, python: str = None, delay=1):
        python = python or sys.executable

        def main():
            try:
                os.execv(python, [python] + sys.argv)
            except OSError:
                os.spawnv(os.P_NOWAIT, python, [python] + sys.argv)
                sys.exit(0)

        fu = cls.add_background_task(main, delay=delay)
        return f"即将在 {delay} 秒后停止程序, 后台任务: {fu}"

    @staticmethod
    def add_background_task(func, args: list = None, kwargs: dict = None, delay=1, daemon=False):
        future = Future()
        args = args or []
        kwargs = kwargs or {}

        def main():
            try:
                delay and time.sleep(delay)
                ret = func(*args, **kwargs)
            except BaseException as exc:
                if future.set_running_or_notify_cancel():
                    future.set_exception(exc)
                raise
            else:
                future.set_result(ret)

        if delay:
            t = threading.Thread(target=main, daemon=daemon)
            t.start()
        else:
            main()

        return future

    def bind(self, obj):
        self.object = obj
        return self

    def __getattr__(self, name):
        if name in self.adapters:
            return self.adapters[name]
        else:
            return getattr(self.object, name)

    def start(self, mode: MODE = "http", concurrency: int = 10, ident: Any = 0, **kwargs):
        serve(self, mode=mode, concurrency=concurrency, ident=ident, on_server_started=self.on_server_started, **kwargs)

    def on_server_started(self, ident: Any):
        logger.debug(f'[RPC] 服务启动成功, 监听端口: {ident}')
        threading.Thread(target=lambda: self.main(*self.args, **self.kwargs), daemon=True).start()

    def register_adapter(self, form: str, action: Callable):
        self.adapters[form] = action


class BaseRunner:
    def __init__(self):
        self.st_utime = time.time()
        self.et_utime = time.time()

    @staticmethod
    def run_local(argv: Argv):
        main: str = argv.main
        if os.path.sep in main or os.path.exists(main):
            with open(argv.main) as f:
                exec(f.read(), {"__name__": "__main__", **argv.args, **argv.extra})
                return None
        else:
            func: Callable = pandora.load_objects(main)
            return pandora.invoke(func=func, kwargs=argv.args, namespace=argv.extra)

    def run_task(self, argv: Argv):
        """
        运行任务

        :param argv:
        :return:
        """

        assert argv.main, "main 参数不能为空"

        # bind rpc
        rpc_options: dict = argv.rpc or {}

        if rpc_options:
            proxy = RpcProxy(self.run_local, argv)
            argv.extra.update({"rpc": proxy})
            proxy.start(**rpc_options)
            return None

        else:
            return self.run_local(argv)
