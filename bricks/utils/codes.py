# -*- coding: utf-8 -*-
# @Time    : 2023-12-10 14:02
# @Author  : Kem
# @Desc    : 动态 code 生成
import dataclasses
import enum
from typing import List, Tuple, Any, Union

from bricks.utils.pandora import iterable


class Type(str, enum.Enum):
    define: str = "define"  # 变量定义
    choice: str = "choice"  # 只选择一个
    condition: str = "condition"  # 只要满足条件都会走
    code: str = "code"  # 直接拼接的代码


@dataclasses.dataclass
class Generator:
    flows: List[Tuple[Type, Any]]
    code: str = ""

    def build(self):
        tpls = []
        for ctype, value in self.flows:
            if ctype == Type.code:
                value: Union[str, list]
                value and tpls.append(";".join(iterable(value)))

            elif ctype == Type.define:
                value: Union[tuple, list]
                value and tpls.append(f"{value[0]} = " + (" and ".join(iterable(value[1])) or "1"))

            elif ctype == Type.choice:
                value: dict = value or {}
                for i, (cond, action) in enumerate(value.items()):
                    if i == 0:
                        tpls.append(f"if {cond}:\n    {';'.join(iterable(action)) or 'pass'}")
                    else:
                        tpls.append(f"elif {cond}:\n    {';'.join(iterable(action)) or 'pass'}")

            elif ctype == Type.condition:
                value: dict = value or {}
                for cond, action in value.items():
                    tpls.append(f"if {cond}:\n    {';'.join(iterable(action)) or 'pass'}")

        else:
            self.code = "\n".join(tpls)

        return self.code

    def run(self, namespace: dict):
        exec(self.build(), namespace)
        return namespace

    def __str__(self):
        return self.build()
