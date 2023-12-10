# -*- coding: utf-8 -*-
# @Time    : 2023-12-10 14:02
# @Author  : Kem
# @Desc    : 动态 code 生成
import dataclasses
from typing import List, Tuple, Any, Union

from bricks.utils.pandora import iterable


class Type:
    define: str = "define"
    condition: str = "condition"
    code: str = "code"


@dataclasses.dataclass
class Genertor:
    flows: List[Tuple[str, Any]]
    code: str = ""

    def make(self):
        tpls = []
        for ctype, value in self.flows:
            if ctype == Type.code:
                value: Union[str, list]
                value and tpls.append(";".join(iterable(value)))
            elif ctype == Type.define:
                value: Union[tuple, list]
                value and tpls.append(f"{value[0]} = " + (" and ".join(iterable(value[1])) or "1"))
            elif ctype == Type.condition:
                value: dict = value or {}
                for i, (cond, action) in enumerate(value.items()):
                    if i == 0:
                        tpls.append(f"if {cond}:\n    {';'.join(iterable(action)) or 'pass'}")
                    else:
                        tpls.append(f"elif {cond}:\n    {';'.join(iterable(action)) or 'pass'}")

            self.code = "\n".join(tpls)

    def run(self, namespace: dict):
        self.make()
        exec(self.code, namespace)
        return namespace
