# -*- coding: utf-8 -*-
# @Time    : 2023-11-20 21:26
# @Author  : Kem
# @Desc    :
import copy
import dataclasses
import re
from typing import Any, Union, Callable, Optional

from bricks.utils import pandora

FORMAT_REGEX = re.compile(r'{(\w+)(?::(\w+))?}')


@dataclasses.dataclass
class RenderNode:

    @classmethod
    def format(cls, value, base: dict, errors: str = "raise"):
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
                            if errors == 'raise':
                                raise ValueError(f"Missing key in base: {placeholder}")
                            elif errors == 'ignore':
                                return value
                            else:
                                base.setdefault(placeholder, "")

                        placeholder_value = base[placeholder]
                        if type_str:
                            placeholder_value = cls.convert(placeholder_value, type_str)
                            value = value.replace(f"{{{placeholder}:{type_str}}}", str(placeholder_value))
                        else:
                            value = value.replace(f"{{{placeholder}}}", str(placeholder_value))

                        if convert_value:
                            value = cls.convert(value, type(placeholder_value))

                    return value

                except KeyError as e:
                    if errors == "raise":
                        raise ValueError(f"Missing key in base: {e}")

                    elif errors == 'ignore':
                        return value

                    else:
                        base.setdefault(e.args[0], "")

        elif isinstance(value, list):
            return [cls.format(item, base, errors=errors) for item in value]
        elif isinstance(value, dict):
            return {k: cls.format(v, base, errors=errors) for k, v in value.items()}
        elif dataclasses.is_dataclass(value):
            return value.render(base)
        return value

    @staticmethod
    def convert(value, type_str):
        maps = {
            "int": {
                "action": int,
                "default": 0
            },
            int: {
                "action": int,
                "default": 0
            },
            "str": {
                "action": str,
                "default": ""
            },
            str: {
                "action": str,
                "default": ""
            },
            "float": {
                "action": str,
                "default": 0.0
            },
            float: {
                "action": str,
                "default": 0.0
            },
            "json": {
                "action": pandora.json_or_eval,
                "default": None
            },
            list: {
                "action": pandora.json_or_eval,
                "default": None
            },
            dict: {
                "action": pandora.json_or_eval,
                "default": None
            },
        }
        if type_str in maps:
            try:
                return maps[type_str]['action'](value)
            except ValueError:
                return maps[type_str]['default']
        else:
            return value

    def render(self, base: dict):
        # 创建一个新的实例，避免修改原始实例
        node = copy.deepcopy(self)
        for field in dataclasses.fields(self):
            value = getattr(node, field.name)
            new_value = self.format(value, base, errors=getattr(self, "strict", "fix"))
            setattr(node, field.name, new_value)
        return node


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


@dataclasses.dataclass
class Post:
    value: Any = None
    prev: "Post" = None


@dataclasses.dataclass
class SignPost:
    """
    流程游标

    """

    cursor: Union[Post, int] = Post(0)
    download: Union[Post, int] = Post(0)
    parse: Union[Post, int] = Post(0)
    pipeline: Union[Post, int] = Post(0)
    action: Union[Post, str] = Post("")

    def __setattr__(self, key, value):
        if not isinstance(value, Post):
            value = Post(value)
            value.prev = getattr(self, key, Post())

        return super().__setattr__(key, value)

    def rollback(self, names: list = None):

        names = names or [field.name for field in dataclasses.fields(self)]

        for name in names:
            v = getattr(self, name, None)
            if v is None:
                continue
            else:
                setattr(self, name, v.prev)
