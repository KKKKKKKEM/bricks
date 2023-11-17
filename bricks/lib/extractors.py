# -*- coding: utf-8 -*-
# @Time    : 2023-11-14 17:05
# @Author  : Kem
# @Desc    : Parser


import collections
import itertools
import math
import re
import time
from typing import Any, Optional, Callable, Union, Type

import jmespath
import jsonpath
from lxml import etree

from bricks.utils import pandora


class Rule:

    def __init__(
            self,
            exprs: Optional[Union[str, Callable]] = "",
            condition: Optional[Callable] = None,
            pre_script: Optional[Callable] = None,
            post_script: Optional[Callable] = None,
            is_array: Optional[bool] = None,
            acquire: bool = True,
            options: Optional[dict] = None,
            const: Optional[Any] = ...,
            default: Optional[Any] = None,
            engine: Optional[Union[Type["Extractor"], str]] = None,
    ):
        self.exprs = exprs
        self.condition = condition
        self.pre_script = pre_script
        self.post_script = post_script
        self.is_array = is_array
        self.allow_nulls = acquire
        self.options = options or {}
        self.const = const
        self.default = default
        self.engine: Optional["Extractor"] = self.to_engine(engine)
        self.backup: Optional['Rule'] = None

    @classmethod
    def to_rule(cls, value, engine=None):
        if isinstance(value, cls):
            value.engine = cls.to_engine(value.engine or engine)
            return value
        else:
            return cls(exprs=value, engine=engine)

    @classmethod
    def to_engine(cls, value):
        if not value:
            return

        if isinstance(value, Extractor) or issubclass(value, Extractor):
            return value
        elif isinstance(value, str):
            return globals()[f'{value.title()}Extractor']

        else:
            raise ValueError("engine must be a string or Extractor instance")

    def apply(self, obj):
        # 常量
        if self.const is not ...:
            return self.const

        # 前置处理脚本
        if callable(self.pre_script):
            obj = self.pre_script(obj)

        # 匹配结果
        res = self.engine.extract(obj, self.exprs, **self.options)

        # 设置默认值
        if res in self.engine.empty:
            if self.backup:
                return self.backup.apply(obj)
            else:
                res = self.default

        # 后置处理脚本
        if callable(self.post_script):
            res = self.post_script(res)

        # 引擎适配逻辑
        if self.is_array is False:
            res = pandora.single(res)

        elif self.is_array is True:
            res = pandora.iterable(res)

        elif self.engine.is_array and isinstance(res, list) and len(res) == 1:
            res = res[0]

        return res

    def is_work(self, obj):
        if self.condition is None:
            return True

        assert callable(self.condition), "condition must be a callable object"
        return self.condition(obj)

    def __or__(self, other):
        if not self.backup:
            self.backup = other
        else:
            self.backup = self.backup | other

        return self

    def __str__(self):
        return f"<Rule {self.exprs}>"


class Extractor:
    """
    提取器抽象类

    """
    is_array = False
    empty = (None,)

    @classmethod
    def extract(cls, obj: Any, exprs: str, **kwargs):
        raise NotImplementedError

    @staticmethod
    def fmt(obj, **kwargs):
        raise NotImplementedError

    @classmethod
    def extract_first(cls, obj: Any, exprs: str, default: Any = None, **kwargs):
        res = cls.extract(
            obj=obj,
            exprs=exprs,
            **kwargs
        )
        return pandora.single(res, default=default)

    @classmethod
    def match(cls, obj: Any, rules: Union[dict, list]):
        rows = []

        def tree():
            return collections.defaultdict(tree)

        for rule in pandora.iterable(rules):
            vessel = tree()

            for item in cls._match(
                    obj=[obj],
                    rules=rule,
            ):
                key, value, rkey1, rkey2, rkey3 = item
                if value is not None: vessel[rkey1][rkey2][rkey3][key] = value

            cleans = []
            for k1, v1 in vessel.items():
                skeys = sorted(v1.keys())
                for index, skey in enumerate(skeys[:-1]):
                    d2 = v1[skeys[index + 1]]
                    for k2, v2 in v1.pop(skey).items():
                        hit = False
                        for k3, v3 in d2.items():
                            if k3.startswith(k2):
                                hit = True
                                v3.update({**v2, **v3})

                        if not hit: d2.update({k2: v2})

                else:
                    # 不同层级执行笛卡尔积
                    cleans = [
                        {**i[1], **i[0]}
                        for i in itertools.product(v1[skeys[-1]].values(), cleans or [{}])
                    ]

            if isinstance(rules, list):
                rows.extend(cleans)
            else:
                rows = cleans

        return rows

    @classmethod
    def _match(cls, obj: Any, rules: dict, rkey="", pkey=""):
        flag = isinstance(obj, list)
        if obj in [None, []]: obj = [None]
        obj = pandora.iterable(obj, enforce=object, exclude=(set, list, tuple))

        def get_rkey(*keys, splice="."):
            return splice.join(filter(bool, keys))

        number_of_digits = math.ceil(math.log(len(obj), 10))

        for index, _obj in enumerate(obj):

            for k, v in rules.items():

                if not isinstance(v, dict):
                    rule = Rule.to_rule(v, engine=cls)
                    _rkey = (
                        rkey.split(".")[0], pkey.count(".") + 1 if flag else pkey.count("."),
                        get_rkey(pkey, str(index).zfill(number_of_digits)) if flag else pkey
                    )

                    if rule.exprs == '@index':
                        yield k, index, *_rkey

                    elif rule.exprs == '@ts':
                        yield k, time.time(), *_rkey

                    elif rule.exprs == '@date':
                        yield k, time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), *_rkey

                    elif not rule.is_work(_obj) is False:
                        _unpack_obj = rule.apply(_obj)

                        if k == '@unpack':

                            if isinstance(_unpack_obj, dict):
                                for _k, _v in _unpack_obj.items():
                                    yield _k, _v, *_rkey

                            elif isinstance(_unpack_obj, (list, tuple, set)):

                                for _k, _v in enumerate(_unpack_obj):
                                    yield _k, _v, *_rkey

                            else:
                                for _k, _v in [(k, _unpack_obj)]:
                                    yield _k, _v, *_rkey

                        else:
                            yield k, _unpack_obj, *_rkey

                # 代表进入下一级别 / 提取功能参数
                else:

                    rule = Rule.to_rule(k, engine=cls)
                    if not rule.is_work(_obj) is False:
                        _ = rule.apply(_obj)

                        for i in cls._match(
                                obj=_,
                                rules=v,
                                rkey=get_rkey(rkey, rule.exprs),
                                pkey=get_rkey(pkey, str(index).zfill(number_of_digits)),
                        ):
                            yield i


class XpathExtractor(Extractor):
    """
    xpath 提取器

    """
    is_array = True
    empty = (None, [])

    @classmethod
    def extract(cls, obj: Union[etree.HTML, str], exprs: str, parser=None, base_url=None):
        obj = cls.fmt(obj, parser=parser, base_url=base_url)
        ret = obj.xpath(exprs, parser=parser, base_url=base_url)
        return [i.strip() if isinstance(i, str) else i for i in ret]

    @classmethod
    def fmt(cls, obj, parser=None, base_url=None):

        if isinstance(obj, str):
            if obj.strip().startswith("<?xml "):
                obj = etree.fromstring(obj, parser=parser, base_url=base_url)
            else:
                obj = etree.HTML(obj, parser=parser, base_url=base_url)
        return obj


class JsonExtractor(Extractor):
    """
    json jmespath 提取器

    """
    is_array = False
    empty = (None,)

    @classmethod
    def extract(cls, obj: Union[dict, list, str], exprs: str, jsonp=False, errors='strict', options=None):
        obj = cls.fmt(obj, jsonp=jsonp, errors=errors)
        ret = jmespath.search(
            data=obj,
            expression=exprs,
            options=options,
        )
        return ret

    @staticmethod
    def fmt(obj, **kwargs):
        if isinstance(obj, str):
            obj = pandora.json_or_eval(
                text=obj,
                **kwargs
            )

        return obj


class JsonpathExtractor(Extractor):
    is_array = False
    empty = (None,)

    @classmethod
    def extract(cls, obj: Union[dict, list, str], exprs: str, jsonp=False, errors='strict', result_type='VALUE',
                debug=0, use_eval=True):
        obj = cls.fmt(obj, jsonp=jsonp, errors=errors)
        ret = jsonpath.jsonpath(
            obj=obj,
            expr=exprs,
            result_type=result_type,
            debug=debug,
            use_eval=use_eval
        )
        if ret is False: ret = None
        return ret

    @staticmethod
    def fmt(obj, **kwargs):
        if isinstance(obj, str):
            obj = pandora.json_or_eval(
                text=obj,
                **kwargs
            )

        return obj


class RegexExtractor(Extractor):
    """
    正则提取器

    """
    is_array = True
    empty = (None, [], "")

    @classmethod
    def extract(cls, obj: str, exprs: str, flags=0):
        obj = cls.fmt(obj)
        return re.findall(exprs, obj, flags=flags)

    @staticmethod
    def fmt(obj, **kwargs):  # noqa
        if not isinstance(obj, str):
            if isinstance(obj, etree._Element):  # noqa
                obj = etree.tostring(obj).decode()
            else:
                obj = str(obj) if obj is not None else ""

        return obj


if __name__ == '__main__':
    r1 = {
        "people": {
            "name": "name",
            "index": "@index",
            "ts": "@ts",
            "hobbies": {
                "hobby": "name"
            }
        }
    }
    data = {
        "people": [
            {
                "name": "John",
                "age": 30,
                "hobbies": [
                    {
                        "name": "hobby1"
                    },
                    {
                        "name": "hobby2"
                    },
                    {
                        "name": "hobby3"
                    },

                ]
            },
            {
                "name": "Jane",
                "age": 25,
                "hobbies": [
                    {
                        "name": "hobby4"
                    },
                    {
                        "name": "hobby5"
                    },
                    {
                        "name": "hobby6"
                    },

                ]
            },
            {"name": "Bob", "age": 20}
        ]
    }

    print(JsonExtractor.match(data, r1))
