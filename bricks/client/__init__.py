import argparse
import dataclasses
import os
import sys


@dataclasses.dataclass
class Argv:
    filename: str
    form: str
    main: str
    workdir: str = ""
    args: dict = dataclasses.field(default_factory=dict)
    extra: dict = dataclasses.field(default_factory=dict)
    env: dict = dataclasses.field(default_factory=dict)
    rpc: dict = dataclasses.field(default_factory=dict)

    def to_cmd(self) -> str:
        cmd = f"{sys.executable} {self.filename} {self.form}"
        if self.workdir:
            cmd += f" -w {self.workdir}"

        for key, payload in [
            ("args", "-a"),
            ("extra", "-extra"),
            ("env", "-env"),
            ("rpc", "-rpc"),
        ]:
            obj: dict = getattr(self, key)
            for k, v in obj.items():
                cmd += f" {payload} {k}={v!r}"
        else:
            return cmd

    @classmethod
    def get_parser(cls) -> argparse.ArgumentParser:
        def set_work_dir(x):
            x and sys.path.insert(0, x)
            x and os.chdir(x)
            return x

        parser = argparse.ArgumentParser()
        parser.add_argument("filename", help="启动文件")
        parser.add_argument("form", help="adapter 名称")
        parser.add_argument("-m", "--main", help="运行配置")
        parser.add_argument("-a", "--args", help="操作参数: key=vaule", action="append")
        parser.add_argument(
            "-extra", "--extra", help="其他信息: key=value", action="append"
        )
        parser.add_argument("-workdir", "--workdir", help="工作目录", type=set_work_dir)
        parser.add_argument("-env", "--env", help="环境变量信息", action="append", type=cls.set_env_var)
        parser.add_argument(
            "-rpc", "--rpc", help="rpc 参数: key=value", action="append"
        )
        return parser

    @staticmethod
    def set_env_var(env_string: str) -> str:
        """
        解析并设置环境变量

        :param env_string: 格式为 "key=value" 的字符串
        :return: 原始字符串
        """
        if not env_string or '=' not in env_string:
            return env_string

        key, value = env_string.split('=', 1)
        key = key.strip()
        value = value.strip()

        if key:
            os.environ[key] = value

        return env_string