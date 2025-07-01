# -*- coding: utf-8 -*-
# @Time    : 2023-11-20 21:26
# @Author  : Kem
# @Desc    :
import copy
import dataclasses
import json
import re
from collections import UserDict
from typing import Any, Callable, List, Literal, Mapping, Optional, Tuple, Union

from bricks.utils import pandora

FORMAT_REGEX = re.compile(r"[{](\w+)(?::(\w+))?[}]")
# TRIPARTITE_EXPRESSIONS_REGEX = re.compile(r'[{]([^{}?]*)\?\s*([^{}:]*)\s*:\s*([^{}]*)[}]')


class UnRendered:
    def __init__(self, value):
        self.value = value


_adapters: dict = {
    "int": int,
    "str": str,
    "float": float,
    "list": pandora.json_or_eval,
    "dict": pandora.json_or_eval,
    "json": lambda value: json.dumps(pandora.json_or_eval(value), ensure_ascii=False),
}


@dataclasses.dataclass
class RenderNode:
    # 字段缺失时的处理手段
    miss: Literal["fix", "raise", "ignore"] = "fix"

    # 不需要渲染的字段
    un_rendered: Union[List[str], Tuple[str]] = dataclasses.field(
        default_factory=lambda: []
    )

    # 适配器, 可以改造渲染语法
    adapters: dict = dataclasses.field(default_factory=lambda: {})

    def format(self, value, base: dict, ident: str = ""):
        if ident in self.un_rendered:
            return value

        if isinstance(value, str):
            while True:
                try:
                    return value.format(**base)
                except (ValueError, TypeError):
                    placeholders = FORMAT_REGEX.findall(value)
                    for placeholder, type_str in placeholders:
                        if placeholder not in base or not placeholder:
                            if self.miss == "raise":
                                raise ValueError(f"Missing key in base: {placeholder}")

                            elif self.miss == "ignore":
                                return value

                            else:
                                base.setdefault(placeholder, "")

                        placeholder_value = base[placeholder]

                        if type_str:
                            tpl = f"{{{placeholder}:{type_str}}}"
                        else:
                            tpl = f"{{{placeholder}}}"

                        value = self.run_adapter(
                            value.replace(tpl, str(placeholder_value)),
                            type_str,
                            base=base,
                        )

                    return value

                except (KeyError, IndexError) as e:
                    if self.miss == "raise":
                        raise ValueError(f"Missing key in base: {e}")

                    elif self.miss == "ignore":
                        return value

                    else:
                        base.setdefault(e.args[0], "")

        elif isinstance(value, (list, tuple, set)):
            return type(value)(
                self.format(item, base, ident=".".join([ident, str(idx)]).strip("."))
                for idx, item in enumerate(value)
            )

        elif isinstance(value, dict):
            return {
                k: self.format(v, base, ident=".".join([ident, str(k)]).strip("."))
                for k, v in value.items()
            }

        elif isinstance(value, UnRendered):
            return value.value

        elif dataclasses.is_dataclass(value) and isinstance(value, RenderNode):
            return value.render(base)

        return value

    def render(self, data: (dict, Mapping, UserDict) = None):
        # 创建一个新的实例，避免修改原始实例
        data = data or {}
        node = copy.deepcopy(self)
        for field in dataclasses.fields(node):
            if field.name in [*["adapters", "miss", "un_rendered"], *node.un_rendered]:
                continue
            value = getattr(node, field.name)
            new_value = node.format(value, data, ident=field.name)
            setattr(node, field.name, new_value)
        return node

    def register_adapter(self, form: Any, action: Callable):
        self.adapters[form] = action

    def run_adapter(self, value, form: Any, base: dict = None):
        adapters = {**_adapters, **self.adapters}
        if form in adapters:
            adapter = adapters[form]
            try:
                assert callable(adapter), f"Invalid adapter action: {adapter}"
                return pandora.invoke(adapter, args=[value], namespace=base)
            except (ValueError, AssertionError):
                return value
        else:
            return value


@dataclasses.dataclass
class LinkNode:
    root: Callable = None
    prev: Optional["LinkNode"] = None
    callback: Optional[Callable] = None

    def __setattr__(self, key, value):
        if key == "prev" and value is not None and not isinstance(value, LinkNode):
            value = LinkNode(value)

        return super().__setattr__(key, value)

    def __bool__(self):
        return bool(self.root)

    def __str__(self):
        return f"LinkNode({self.root})"

    def __eq__(self, other):
        if not isinstance(other, LinkNode):
            return self.root == other
        else:
            return super().__eq__(other)
