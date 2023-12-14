# -*- coding: utf-8 -*-
# @Time    : 2023-11-20 21:26
# @Author  : Kem
# @Desc    :
import copy
import dataclasses
import re
from collections import UserDict
from typing import Any, Callable, Optional, Literal, Mapping

from bricks.utils import pandora

FORMAT_REGEX = re.compile(r'{(\w+)(?::(\w+))?}')


@dataclasses.dataclass
class RenderNode:
    errors: Literal["fix", "raise", "ignore"] = "fix"
    adapters: dict = dataclasses.field(default_factory=lambda: {
        "int": int,
        int: int,
        "str": str,
        str: str,
        "float": float,
        float: float,
        "json": pandora.json_or_eval,
        list: pandora.json_or_eval,
        dict: pandora.json_or_eval,
    })

    def format(self, value, base: dict):
        if isinstance(value, str):
            while True:
                try:
                    return value.format(**base)
                except ValueError:

                    placeholders = FORMAT_REGEX.findall(value)
                    # 有多个, 那最终肯定还是字符串
                    convert_value = len(placeholders) == 1
                    for placeholder, type_str in placeholders:

                        if placeholder not in base:
                            if self.errors == 'raise':
                                raise ValueError(f"Missing key in base: {placeholder}")
                            elif self.errors == 'ignore':
                                return value
                            else:
                                base.setdefault(placeholder, "")

                        placeholder_value = base[placeholder]
                        if type_str:
                            placeholder_value = self.run_adapter(placeholder_value, type_str, base=base)
                            value = value.replace(f"{{{placeholder}:{type_str}}}", str(placeholder_value))
                        else:
                            value = value.replace(f"{{{placeholder}}}", str(placeholder_value))

                        if convert_value:
                            value = self.run_adapter(value, type(placeholder_value), base=base)

                    return value

                except KeyError as e:
                    if self.errors == "raise":
                        raise ValueError(f"Missing key in base: {e}")

                    elif self.errors == 'ignore':
                        return value

                    else:
                        base.setdefault(e.args[0], "")

        elif isinstance(value, list):
            return [self.format(item, base) for item in value]
        elif isinstance(value, dict):
            return {k: self.format(v, base) for k, v in value.items()}
        elif dataclasses.is_dataclass(value):
            return value.render(base)
        return value

    def render(self, base: (dict, Mapping, UserDict) = None):
        # 创建一个新的实例，避免修改原始实例
        base = base or {}
        node = copy.deepcopy(self)
        for field in dataclasses.fields(self):
            value = getattr(node, field.name)
            new_value = self.format(value, base)
            setattr(node, field.name, new_value)
        return node

    def register_adapter(self, form: Any, action: Callable):
        self.adapters[form] = action

    def run_adapter(self, value, form: Any, base: dict = None):
        print(value)
        if form in self.adapters:
            adapter = self.adapters[form]
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
    prev: Optional['LinkNode'] = None
    callback: Optional[Callable] = None

    def __setattr__(self, key, value):
        if key == "prev" and value is not None and not isinstance(value, LinkNode):
            value = LinkNode(value)

        return super().__setattr__(key, value)

    def __bool__(self):
        return bool(self.root)

    def __str__(self):
        return f'LinkNode({self.root})'
