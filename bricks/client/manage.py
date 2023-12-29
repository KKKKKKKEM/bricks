# -*- coding: utf-8 -*-
# @Time    : 2023-12-29 10:03
# @Author  : Kem
# @Desc    : 管理工具
import base64
import sys
from typing import Union, List, Callable
from urllib import parse

from bricks.client import Argv
from bricks.client.runner import BaseRunner
from bricks.utils import pandora


@pandora.with_metaclass(singleton=True, autonomous=("install",))
class Manager:
    def __init__(self):

        self.adapters = {
            "run_task": self.run_task
        }

    @staticmethod
    def _parse(argvs: list) -> Argv:
        """
        解析参数

        python manage.py [action] -r/--ref reference -a/--args options -e/--extra extra -w/--workdir dir -s/--server server


        :return:
        """

        def _2dict(obj):
            kw = {}
            for (key, value) in parse.parse_qsl("&".join(pandora.iterable(obj))):
                kw[key] = pandora.guess(value)
            return kw

        if len(argvs) == 2 and argvs[1].startswith("base64://"):
            en_argv = argvs[1][9:]
            de_argvs = base64.b64decode(en_argv.encode()).decode().split(" ")
            argvs = [argvs[0], *de_argvs]

        parser = Argv.get_parser()
        argv = parser.parse_args(argvs)

        return Argv(
            filename=argv.filename,
            form=argv.form,
            main=argv.main,
            args=_2dict(argv.args),
            extra=_2dict(argv.extra),
            env={**_2dict(argv.env), "workdir": argv.workdir},
            rpc=_2dict(argv.rpc),
            workdir=argv.workdir,
        )

    def run(self, argv: Union[str, List[str]] = None):
        """
        运行

        :param argv: 命令行参数, 不传的时候取 sys.argv
        :return:
        """
        argvs = [*sys.argv]
        if isinstance(argv, str):
            argvs.extend(argv.split(' '))
        else:
            argvs.extend(pandora.iterable(argv))

        argv = self._parse(argvs)
        self.run_adapter(argv)

    def register_adapter(self, form: str, action: Callable):
        self.adapters[form] = action

    def run_adapter(self, argv: Argv):
        form: str = argv.form
        assert form in self.adapters, f"form 未注册: {form}"
        adapter = self.adapters[form]
        return adapter(argv)

    @staticmethod
    def run_task(argv: Argv):
        runner = BaseRunner()
        return runner.run_task(argv)
