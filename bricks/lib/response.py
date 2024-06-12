# -*- coding: utf-8 -*-
# @Time    : 2023-11-14 21:40
# @Author  : Kem
# @Desc    : Response Model
import re
import sys
from typing import Union, Callable, Any, List, Mapping

from lxml import etree
from w3lib.encoding import http_content_type_encoding, html_body_declared_encoding

from bricks.lib import extractors
from bricks.lib.cookies import Cookies
from bricks.lib.headers import Header
from bricks.lib.request import Request
from bricks.utils import pandora

_HEADER_ENCODING_RE = re.compile(r"charset=([\w-]+)", re.I)


class Response:
    def __init__(
            self,
            content: Any = None,
            status_code: int = 200,
            headers: Union[Header, dict, Mapping] = None,
            url: str = None,
            encoding: str = None,
            reason: str = 'ok',
            cookies: Cookies = None,
            history: List['Response'] = None,
            request: 'Request' = ...,
            error: Any = "",
            callback: Callable = None
    ):
        self.content: Union[str, bytes] = content
        self.status_code = status_code
        self._headers = Header(headers)
        self.url = url
        self.encoding = encoding or self.guess_encoding()
        self.reason = reason
        self.cookies = cookies
        self.history: List['Response'] = history or []
        self.request: 'Request' = request
        self.error = error
        self.callback = callback
        self._cache = {}

    headers: Header = property(
        fget=lambda self: self._headers,
        fset=lambda self, v: setattr(self, "_headers", Header(v)),
        fdel=lambda self: setattr(self, "_headers", Header({})),
        doc="请求头"
    )

    def guess_encoding(self):
        if not self.content:
            return

        # 1. 从header中获取编码
        content_type = self.headers.get("Content-Type")
        temp = http_content_type_encoding(content_type)
        if temp:
            return temp

        temp = html_body_declared_encoding(self.content)
        if temp:
            return temp

        return 'utf-8'

    @property
    def text(self):
        """
        :return: the str for response content.
        """
        if isinstance(self.content, bytes):

            if not self.content:
                return str('')

            try:
                return str(self.content, self.encoding, errors='replace')
            except (LookupError, TypeError):
                return str(self.content, errors='replace')

        else:
            return self.content

    @property
    def html(self):
        return etree.HTML(self.text)

    @property
    def length(self):
        """
        :return:the length of response content.
        """
        return len(self.content or '')

    @property
    def size(self):
        """
        :return:the size of response content in bytes.
        """
        return sys.getsizeof(self.content)

    def json(self, **kwargs):
        """
        Deserialize a JSON document to a Python object.
        """
        return pandora.json_or_eval(self.text, **kwargs) if isinstance(self.text, str) else self.text

    def extract_all(
            self,
            engine: Union[str, Callable],
            rules: Union[dict, list],
    ):
        """
        根据多个规则循环匹配

        :param engine:
        :param rules:
        :return:
        """

        for rule in pandora.iterable(rules):
            yield self.extract(
                engine=engine,
                rules=rule,
            )

    def extract(
            self,
            engine: Union[str, Callable],
            rules: dict,

    ):
        """
        提取引擎, 生成器模式, 支持 Rule, 批量匹配

        :param engine:
        :param rules:
        :return:
        """

        exs = {
            'JSON': extractors.JsonExtractor,
            'XPATH': extractors.XpathExtractor,
            'JASONPATH': extractors.JsonpathExtractor,
            'REGEX': extractors.RegexExtractor,
        }
        if not engine:
            return []

        try:
            if isinstance(engine, str):
                if engine.upper() in exs:
                    extractor: extractors.Extractor = exs[engine.upper()]
                    ret = extractor.match(
                        obj=self.text,
                        rules=rules
                    )
                    return ret
                else:
                    engine = pandora.load_objects(engine)

            if callable(engine):

                return engine({
                    'response': self,
                    'request': self.request,
                    'rules': rules,
                })

            else:
                raise ValueError(f"无法识别的引擎类型: {engine}")

        except Exception as e:
            raise RuntimeError(f"解析数据出现了意料之外的错误: {e}")

    def xpath(self, xpath, obj=None, **kwargs):
        """
        进行xpath匹配

        :param xpath: xpath规则
        :param obj: 要匹配的对象
        :param kwargs:
        :return:
        """

        if not xpath:
            return obj

        obj = obj or self.html

        return extractors.XpathExtractor.extract(
            obj=obj,
            exprs=xpath,
            **kwargs
        )

    def xpath_first(self, xpath, obj=None, default=None, **kwargs):
        """
        返回xpath的第一个对象

        :param default:
        :param xpath:
        :param obj:
        :param kwargs:
        :return:
        """

        return pandora.first(self.xpath(xpath, obj, **kwargs), default=default)

    def jsonpath(self, jpath, obj=None, strict=True, **kwargs):
        """
        使用 jsonpath 进行匹配

        :param jpath:
        :param obj:
        :param strict:
        :param kwargs:
        :return:
        """
        if not jpath: return obj
        obj = obj or self.text
        return extractors.JsonpathExtractor.extract(
            obj=obj,
            exprs=jpath,
            jsonp=not strict,
            **kwargs
        )

    def jsonpath_first(self, jpath, obj=None, default=None, strict=True, **kwargs):
        """
        返回jpath的第一个对象

        :param strict:
        :param default:
        :param jpath:
        :param obj:
        :param kwargs:
        :return:
        """
        return pandora.first(self.jsonpath(jpath, obj, strict, **kwargs), default=default)

    def re(self, regex, obj=None, **kwargs):
        """
        正则匹配

        :param regex: 正则表达式
        :param obj:
        :param kwargs:
        :return:
        """
        obj = obj or self.text

        if not regex:
            return obj

        return extractors.RegexExtractor.extract(
            obj=obj,
            exprs=regex,
            **kwargs
        )

    def re_first(self, regex, default=None, obj=None, **kwargs):
        """
        正则

        :param obj:
        :param regex:
        :param default:
        :param kwargs:
        :return:
        """
        return pandora.first(self.re(regex, obj, **kwargs), default=default)

    def get(self, rule: str, obj=None, strict=True, **kwargs):
        """
        json匹配
        json规则示例:
        字典内直接采用 a.b.c 的方式匹配
        如果要匹配列表内的每一项, 采用$代替每一项
        如果要从父级开始匹配, 采用//代替父级节点

        :param rule: json解析规则
        :param strict: json解析规则
        :param obj: 需要匹配的dict/list对象
        :return:
        """
        obj = obj or self.text

        if not rule:
            return obj

        return extractors.JsonExtractor.extract(
            obj=obj,
            exprs=rule,
            jsonp=not strict,
            **kwargs
        )

    def get_first(self, rule: str, default=None, obj=None, strict=True, **kwargs):
        """
        返回json匹配的第一个对象

        :param rule: json解析规则
        :param strict: json解析规则
        :param default: 获取不到的时候的默认值为
        :param obj: 需要匹配的dict/list对象
        :return:

        """
        return pandora.first(self.get(rule, obj, strict, **kwargs), default=default)

    @property
    def ok(self):
        """
        return true if response status_code conform to the rules

        :return:
        """
        return 200 <= self.status_code < 400

    @classmethod
    def make_response(cls, **kwargs):
        """
        make one empty response if not kwargs else normal response

        :param kwargs:
        :return:
        """
        kwargs.setdefault("status_code", 0)
        kwargs.setdefault("reason", "empty")
        kwargs.setdefault("error", "empty")
        return cls(**kwargs)

    def is_json(self, **kwargs):
        try:
            self.json(**kwargs)
            return True
        except:  # noqa
            return False

    def __str__(self):
        return f'<Response [{self.error if self.status_code == -1 else self.status_code}] {self.url}>'

    __repr__ = __str__

    def __bool__(self):
        return self.ok

    def __setattr__(self, key, value):

        if key in ["encoding", "content"]:
            # 修改这三个属性的时候, 需要把缓存清空
            object.__setattr__(self, "_cache", {})

        return object.__setattr__(self, key, value)

    def __getattribute__(self, item):

        def cache_method(func):
            def wrapper(*args, **kwargs):
                # 生成缓存的键
                cache_key = (args, tuple(kwargs.items()))
                # 检查缓存
                if cache_key not in self._cache:
                    self._cache[cache_key] = func(*args, **kwargs)
                return self._cache[cache_key]

            return wrapper

        if item in ["text", "html", "json"]:
            cache = self._cache
            if cache and item in cache:
                cached = cache[item]
                return cached
            else:
                ret = object.__getattribute__(self, item)
                self._cache[item] = cache_method(ret) if callable(ret) else ret
                return self._cache[item]

        return object.__getattribute__(self, item)


if __name__ == '__main__':
    res = Response('{"name":"kem"}')
    print(res.json())
    print(res.json())
    print(res.json())
    print(res.json())
    res.content = '{"name":"kem2"}'
    print(res.json())
    print(res.json())
    print(res.json())
