# -*- coding: utf-8 -*-
# @Time    : 2023-12-29 11:11
# @Author  : Kem
# @Desc    : 运行器
import os
import shutil
import signal
import subprocess
import sys
import threading
import time
import venv
from concurrent.futures import Future
from typing import Callable
from urllib import parse

from loguru import logger

from bricks.client import Argv
from bricks.utils import pandora


class BaseRunner:

    def __init__(self):
        self.st_utime = time.time()
        self.et_utime = time.time()

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

    @classmethod
    def stop(cls, delay=1):
        fu = cls.add_background_task(lambda: os.kill(os.getpid(), signal.SIGTERM), delay=delay)
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
    def run_local(argv: Argv):
        main: str = argv.main
        if os.path.sep in main or os.path.exists(main):
            with open(argv.main) as f:
                exec(f.read(), {"__name__": "__main__", **argv.args, **argv.extra})
                return
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
        env: dict = argv.env or {}

        # build project
        self.build_project(env)
        # build env
        self.build_env(env)

        # bind rpc
        rpc: dict = argv.rpc or {}
        client = self.build_rpc(rpc)
        client and argv.extra.update({"rpc": client})

        return self.run_local(argv)

    @classmethod
    def build_rpc(cls, settings: dict):
        if not settings:
            return

        from bricks.client.rpc import APP
        app = APP(**settings)
        app.register_adapter('stop', cls.stop)
        app.register_adapter('reload', cls.reload)
        app.start()
        return app

    def build_env(self, settings: dict):
        if not settings:
            return

        workdir = settings.get('workdir') or os.getcwd()
        requirements: str = os.path.join(workdir, settings.get('requirements', "requirements.txt"))
        python = sys.executable
        pypi = settings.get('pypi')
        venv_folder = os.path.join(workdir, settings.get('venv_dir', "venv"))
        sysm = {
            "nt": {
                "python": os.sep.join(["Scripts", "python.exe"]),
                "venv_lib": "Lib"
            },
            "unix": {
                "python": os.sep.join(["bin", "python"]),
                "venv_lib": "lib"
            }
        }
        if os.path.exists(requirements):
            # 获取文件的修改时间
            st_mtime = os.stat(requirements).st_mtime

            # 判断虚拟环境目录是否存在
            if not os.path.exists(venv_folder):
                logger.debug(f'准备创建虚拟环境: {venv_folder}')
                venv.main([venv_folder, "--without-pip", "--system-site-packages"])
                logger.debug(f'成功创建虚拟环境: {venv_folder}')
                st_mtime = self.st_utime

            # 获取虚拟环境下面的 python 对象
            python = os.path.join(venv_folder, (sysm.get(os.name) or sysm.get('unix'))["python"])

            # 依赖文件有修改
            if self.st_utime <= st_mtime <= self.et_utime or not os.path.exists(venv_folder):
                venv_lib = os.sep.join([venv_folder, (sysm.get(os.name) or sysm.get('unix'))["venv_lib"]])

                # 获取虚拟环境安装包目录
                if os.name == 'nt':
                    packages_path: str = os.sep.join([venv_lib, "site-packages"])
                else:
                    packages_path: str = os.sep.join([venv_lib, os.listdir(venv_lib)[0], "site-packages"])

                logger.debug(f'依赖文件疑似存在修改, 准备安装/更新依赖: {packages_path}')

                cmds = [
                    f'{sys.executable} -m pip install -r {requirements} --upgrade --target={packages_path}'
                ]

                with open(requirements) as f:
                    text = f.read()
                    if "bricks-py" not in text or "bricks_py" not in text:
                        ver = pandora.require("bricks-py")
                        cmds.insert(0, f"{sys.executable} -m pip install -U bricks-py=={ver} --target={packages_path}")

                for cmd in cmds:
                    if pypi: cmd += f" -i {pypi} --upgrade --trusted-host {parse.urlparse(pypi).hostname}"
                    rret = subprocess.call(cmd.strip().split(" "))
                    if rret != 0:
                        shutil.rmtree(venv_folder, ignore_errors=True)
                        raise RuntimeError("安装依赖错误")

                logger.debug(f'成功安装/更新依赖: {packages_path}')

        if python != sys.executable:
            os.putenv("bricks-switch-python", "yes")
            self.reload(python, delay=0)

    def build_project(self, settings: dict):
        if not settings:
            return

        def build_for_git():
            pandora.require('gitpython==3.1.40')
            from git import Repo, InvalidGitRepositoryError
            workdir = settings.get('workdir') or os.getcwd()
            url: str = settings['url']

            try:
                if os.path.exists(workdir):
                    repo = Repo(workdir)
                    u1, u2 = url.lower(), repo.remote().url.lower()

                    if not u1.endswith('.git'):
                        u1 += '.git'
                    if not u2.endswith('.git'):
                        u2 += '.git'
                    assert u1 == u2

                    logger.debug(f"开始更新代码: {workdir}")
                    repo.git.pull()
                    logger.debug(f"成功更新代码: {workdir}")

                else:
                    logger.debug(f"开始拉取代码: {workdir}")
                    Repo.clone_from(url=url, to_path=workdir)
                    logger.debug(f"成功拉取代码: {workdir}")
                    self.et_utime = time.time()
            except (InvalidGitRepositoryError, AssertionError):
                # 文件夹不存在 / 文件夹存在但是不是正确的 Repo / Repo 的 URL 更改了
                shutil.rmtree(workdir, ignore_errors=True)

        form = settings.get('form')
        builders = {
            "git": build_for_git,
        }
        if os.environ.pop('bricks-switch-python', None) == 'yes':
            return

        builder = builders.get(form)
        builder and builder()
