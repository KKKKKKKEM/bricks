# -*- coding: utf-8 -*-
# @Time    : 2023-11-14 21:40
# @Author  : Kem
# @Desc    : Response Model
import sys
from typing import Union, Callable, Any, List

from lxml import etree
from w3lib.encoding import http_content_type_encoding, html_body_declared_encoding

from bricks._libs import extractors
from bricks._libs.headers import Header
from bricks._libs.request import Request
from bricks.utils import universal


class Response:
    def __init__(
            self,
            content: Any = None,
            status_code: int = 200,
            headers: Union[Header, dict] = None,
            url: str = None,
            encoding: str = None,
            reason: str = 'ok',
            cookies=None,
            history: List['Response'] = None,
            request: 'Request' = ...,
            error: Any = ...,
    ):
        self._raw_content: Union[str, bytes] = content
        self.status_code = status_code
        self._headers = Header(headers, _dtype=str)
        self.url = url
        self._encoding = encoding
        self.reason = reason
        self.cookies = cookies
        self.history: List['Response'] = history or []
        self.request: 'Request' = request
        self.error = error
        self._cached_json = None
        self._cached_selector = None
        self._cached_text = ""

    headers: Header = property(
        fget=lambda self: self._headers,
        fset=lambda self, v: setattr(self, "_headers", Header(v, _dtype=str)),
        fdel=lambda self: setattr(self, "_headers", Header({}, _dtype=str)),
        doc="请求头"
    )

    def clear_cache(self):
        self._cached_json = None
        self._cached_selector = None
        self._cached_text = None

    @property
    def encoding(self):
        """
        编码优先级：自定义编码 > header中编码 > 页面编码 > 根据content猜测的编码
        """
        self._encoding = (
                self._encoding
                or self._headers_encoding()
                or self._body_declared_encoding()
                or 'utf-8'
        )
        return self._encoding

    @encoding.setter
    def encoding(self, value):
        self.clear_cache()
        self._encoding = value

    @property
    def content(self):
        return self._raw_content

    @content.setter
    def content(self, value):
        self.clear_cache()
        self._raw_content = value

    @property
    def text(self):
        """
        :return: the str for response content.
        """
        if not self._cached_text:
            if isinstance(self.content, bytes):

                if not self.content:
                    return str('')

                try:
                    self._cached_text = str(self.content, self.encoding, errors='replace')
                except (LookupError, TypeError):

                    self._cached_text = str(self.content, errors='replace')

            else:
                self._cached_text = self.content

        return self._cached_text

    @text.setter
    def text(self, value):
        self.clear_cache()
        self._cached_text = value

    @property
    def html(self):
        if not self._cached_selector:
            self._cached_selector = etree.HTML(self.text)
        return self._cached_selector

    def _headers_encoding(self):
        """
        从headers获取头部charset编码

        """
        content_type = self.headers.get("Content-Type") or self.headers.get(
            "content-type"
        )
        return http_content_type_encoding(content_type)

    def _body_declared_encoding(self):
        """
        从html xml等获取<meta charset="编码">

        """

        return html_body_declared_encoding(self.content)

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
        .. versionadded:: 2.2

        Deserialize a JSON document to a Python object.
        """
        if not self._cached_json:
            self._cached_json = universal.json_or_eval(self.text, **kwargs) if isinstance(self.text, str) else self.text
        return self._cached_json

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

        for rule in universal.iterable(rules):
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
            'json': extractors.JsonExtractor,
            'xpath': extractors.XpathExtractor,
            'jsonpath': extractors.JsonpathExtractor,
            'regex': extractors.RegexExtractor,
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
                    engine = universal.load_objects(engine)

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
        obj = obj or self.html

        if not xpath:
            return obj

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

        obj = obj or self.html

        if not xpath:
            return obj

        return extractors.XpathExtractor.extract_first(
            obj=obj,
            exprs=xpath,
            default=default,
            **kwargs
        )

    def jsonpath(self, jpath, obj=None, strict=True, **kwargs):
        """
        使用 jsonpath 进行匹配

        :param jpath:
        :param obj:
        :param strict:
        :param kwargs:
        :return:
        """

        obj = obj or self.text

        if not jpath:
            return obj

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
        obj = obj or self.text

        if not jpath:
            return obj

        return extractors.JsonpathExtractor.extract_first(
            obj=obj,
            exprs=jpath,
            default=default,
            jsonp=not strict,
            **kwargs
        )

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
        obj = obj or self.text

        if not regex:
            return obj

        return extractors.RegexExtractor.extract_first(
            obj=obj,
            exprs=regex,
            default=default,
            **kwargs
        )

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
        obj = obj or self.text

        if not rule:
            return obj

        return extractors.JsonExtractor.extract_first(
            obj=obj,
            exprs=rule,
            default=default,
            jsonp=not strict,
            **kwargs
        )

    def __bool__(self):
        return self.ok

    @property
    def ok(self):
        """
        return true if response status_code conform to the rules

        :return:
        """
        status_codes = self.request.status_codes

        # None -> 所有状态码, 除了 -1 , -1 代表请求过程中发生了错误
        if status_codes is None:
            return self.status_code != -1

        elif isinstance(status_codes, dict):
            mode = status_codes.get('mode', 'include')
            default = 1 if mode == 'include' else 0
            return bool(status_codes.get(self.status_code, default))

        # 默认 -> 200 到 400
        else:
            return 200 <= self.status_code < 400

    def __str__(self):
        return f'<Response [{self.error if self.status_code == -1 else self.status_code}] {self.url}>'

    __repr__ = __str__

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
        except:
            return False
