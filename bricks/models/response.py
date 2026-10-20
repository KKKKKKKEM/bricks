"""HTTP 响应数据、内部失败信息与常用内容解码。"""

from __future__ import annotations

import codecs
import json
from collections.abc import Iterable, Mapping
from email.message import Message
from http.cookiejar import Cookie, CookieJar
from typing import Any, Literal

from interlace.engine.errors import ExecutionControlError

from ..parsers import (
    BaseParser,
    CssParser,
    JmesPathParser,
    JsonPathParser,
    RegexParser,
    XPathParser,
)
from ..parsers.rules import RuleSet
from ._validation import duration, http_url
from .cookies import Cookies
from .headers import HeaderInput, Headers
from .request import Request

Engine = Literal["json", "jsonpath", "css", "xpath", "regex"]

# 便捷提取使用的无状态解析器；JSON 类引擎读取 json()，其余读取 text。
_ENGINES: dict[str, BaseParser] = {
    "json": JmesPathParser(),
    "jsonpath": JsonPathParser(),
    "css": CssParser(),
    "xpath": XPathParser(),
    "regex": RegexParser(),
}


def _invalid_constant(value: str) -> Any:
    """拒绝 JSON 中的 NaN 和 Infinity 等非标准常量。

    Args:
        value: JSON 解码器遇到的非标准常量文本。

    Raises:
        ValueError: 参数值或字段组合不合法。
    """

    raise ValueError(f"invalid JSON constant: {value}")


def _text_encoding(value: str) -> str:
    """校验编码名称对应文本解码器并返回规范名称。

    Args:
        value: 需要校验的文本编码名称。

    Returns:
        文本解码器的规范编码名称。

    Raises:
        LookupError: 指定编码不存在或不是文本编码。
    """

    codec = codecs.lookup(value)
    try:
        decoded, _ = codec.decode(b"")
    except (TypeError, ValueError) as exc:
        raise LookupError(f"{value!r} is not a text encoding") from exc
    if not isinstance(decoded, str):
        raise LookupError(f"{value!r} is not a text encoding")
    return codec.name


class Response:
    """HTTP 响应类，用于查看服务器返回的数据或请求内部失败的原因。

    提供状态码、请求头、Cookie、文本和 JSON 读取能力，保留关联请求与重定向历史。
    支持修正编码和在响应 Hook 中替换内容；实际下载和页面解析由其他组件负责。

    Attributes:
        __slots__: 实例允许保存的字段名称，限制动态增加属性。
        content: 当前保存的内容字节。
        status_code: HTTP 状态码，-1 表示内部失败。
        url: 绝对 HTTP 或 HTTPS 地址。
        headers: 保留重复字段的 HTTP 头容器。
        cookies: 当前请求或响应的 Cookie 数据，不表示跨请求会话。
        encoding: 显式文本编码，None 根据内容与响应头推断。
        reason: HTTP 状态或内部失败的说明。
        error: 当前结果携带的原始异常，正常结果为 None。
        cost: 请求耗时，单位秒，计时范围由下载器定义。
        request: 响应关联的独立请求快照。
        history: 按先后顺序保存的重定向响应快照。
    """

    __slots__ = (
        "content",
        "status_code",
        "url",
        "headers",
        "cookies",
        "encoding",
        "reason",
        "error",
        "cost",
        "request",
        "history",
    )

    content: bytes
    status_code: int
    url: str
    headers: Headers
    cookies: Cookies
    encoding: str | None
    reason: str
    error: Exception | None
    cost: float
    request: Request | None
    history: tuple[Response, ...]

    def __init__(
        self,
        content: bytes = b"",
        *,
        status_code: int = 200,
        url: str = "",
        headers: HeaderInput | None = None,
        cookies: Cookies
        | CookieJar
        | Iterable[Cookie]
        | Mapping[str, str]
        | None = None,
        encoding: str | None = None,
        reason: str = "",
        error: Exception | None = None,
        cost: float = 0,
        request: Request | None = None,
        history: Iterable[Response] = (),
    ) -> None:
        """构造响应或内部失败记录。

        Args:
            content: 响应内容字节，默认空字节；Hook 可重新赋值。
            status_code: 100–599 表示服务器响应；-1 表示未取得正常响应。
            url: 最终响应 URL；省略时使用关联请求地址。
            headers: 响应头映射或键值对，保留重复 Set-Cookie。
            cookies: 完整 Cookie 容器、标准库 CookieJar、Cookie 序列或简写字典；
                None 时从响应头解析，需要响应 URL。
            encoding: 显式文本编码；None 时按 BOM、响应头、UTF-8 顺序判断。
            reason: 状态说明；内部失败时省略则从异常消息或类名生成。
            error: 内部失败的 Exception 对象；正常 HTTP 响应不得携带异常。
            cost: 本次请求耗时，非负秒数，具体计时范围由下载器确定。
            request: 实际发送的请求，本类保存其独立副本。
            history: 按先后顺序排列的重定向响应，构造时复制。

        Raises:
            TypeError: 字段类型不合法。
            ValueError: 状态与异常组合不合法，或解析 Cookie 时缺少来源 URL。
            LookupError: 显式编码不存在或不是文本编码。
            ExecutionControlError: 传入引擎控制异常时原样传播。
        """

        if not isinstance(content, bytes):
            raise TypeError("response content must be bytes")
        if type(status_code) is not int or not (
            status_code == -1 or 100 <= status_code <= 999
        ):
            raise ValueError("status_code must be -1 or an HTTP status from 100 to 999")
        if not isinstance(url, str):
            raise TypeError("url must be a string")
        if url:
            http_url(url)
        if encoding is not None:
            if not isinstance(encoding, str):
                raise TypeError("encoding must be a codec name or None")
            _text_encoding(encoding)
        if not isinstance(reason, str):
            raise TypeError("reason must be a string")
        if isinstance(error, ExecutionControlError) or (
            isinstance(error, BaseException) and not isinstance(error, Exception)
        ):
            raise error
        if error is not None and not isinstance(error, Exception):
            raise TypeError("error must be an Exception or None")
        if status_code == -1:
            if error is None:
                raise ValueError("internal failure requires an exception")
            if not reason.strip():
                reason = str(error).strip() or type(error).__name__
        elif error is not None:
            raise ValueError("HTTP responses cannot carry an internal error")
        if request is not None and not isinstance(request, Request):
            raise TypeError("request must be a Request or None")
        history = tuple(history)
        if any(not isinstance(item, Response) for item in history):
            raise TypeError("history must contain only Response objects")
        if cost is None:
            raise TypeError("cost must be a number")
        url = url or (request.real_url if request is not None else "")
        response_headers = Headers(headers, validate_values=False)
        if cookies is None:
            if response_headers.get_all("Set-Cookie"):
                if not url:
                    raise ValueError("a response URL is required to parse Set-Cookie")
                response_cookies = Cookies.from_headers(response_headers, url=url)
            else:
                response_cookies = Cookies()
        else:
            response_cookies = Cookies(cookies, url=url or None)
        values = {
            "content": content,
            "status_code": status_code,
            "url": url,
            "headers": response_headers,
            "cookies": response_cookies,
            "encoding": encoding,
            "reason": reason,
            "error": error,
            "cost": duration(cost, "cost", allow_zero=True),
            "request": request.copy() if request is not None else None,
            "history": tuple(item.copy() for item in history),
        }
        for name, value in values.items():
            object.__setattr__(self, name, value)

    def __setattr__(self, name: str, value: Any) -> None:
        """修改内容或编码，其他字段使用 copy() 创建变体。

        Args:
            name: content 或 encoding。
            value: 字段新值。

        Raises:
            AttributeError: 字段只读或不存在。
            TypeError: 字段类型不合法。
            LookupError: 编码不合法。
        """

        if name == "content":
            if not isinstance(value, bytes):
                raise TypeError("response content must be bytes")
        elif name == "encoding":
            if value is not None:
                if not isinstance(value, str):
                    raise TypeError("encoding must be a codec name or None")
                _text_encoding(value)
        else:
            raise AttributeError(f"{name} is read-only; use copy()")
        object.__setattr__(self, name, value)

    def size(self) -> int:
        """返回当前 content 的字节数，不读取 Content-Length，不包含响应头。

        Returns:
            当前响应体的字节数，不包含响应头或对象内存开销。
        """

        return len(self.content)

    @property
    def resolved_encoding(self) -> str:
        """返回当前内容的有效编码：显式设置、BOM、响应头，最后回退 UTF-8。

        Returns:
            当前响应体适用的文本编码名称。
        """

        if self.encoding is not None:
            return self.encoding
        for marker, encoding in (
            (codecs.BOM_UTF32_LE, "utf-32"),
            (codecs.BOM_UTF32_BE, "utf-32"),
            (codecs.BOM_UTF8, "utf-8-sig"),
            (codecs.BOM_UTF16_LE, "utf-16"),
            (codecs.BOM_UTF16_BE, "utf-16"),
        ):
            if self.content.startswith(marker):
                return encoding
        message = Message()
        message["Content-Type"] = self.headers.get("Content-Type", "")
        charset = message.get_content_charset()
        if charset:
            try:
                return _text_encoding(charset)
            except LookupError:
                pass
        return "utf-8"

    @property
    def text(self) -> str:
        """按当前编码解码内容，非法字节替换为占位字符，不缓存解码结果。

        Returns:
            按当前编码解码后的文本，非法字节替换为占位字符。
        """

        return self.content.decode(self.resolved_encoding, errors="replace")

    def json(self, **kwargs: Any) -> Any:
        """将当前内容解析为 JSON，默认拒绝非标准数值常量。

        Args:
            **kwargs: 传给 json.loads 的解析选项。

        Returns:
            解析后的 Python 对象，不缓存结果。

        Raises:
            UnicodeDecodeError: 内容无法按当前编码解码。
            ValueError: 内容不是有效 JSON 或含有默认禁止的数值常量。
        """

        kwargs.setdefault("parse_constant", _invalid_constant)
        return json.loads(self.content.decode(self.resolved_encoding), **kwargs)

    @property
    def ok(self) -> bool:
        """状态码为 200–399 时返回 True，仅表示 HTTP 状态，不判断业务内容。

        Returns:
            满足当前操作的判断条件时返回 True，否则返回 False。
        """

        return 200 <= self.status_code < 400

    def get(self, expression: str, default: Any = None) -> Any:
        """对 json() 的解码结果执行 JMESPath 查询。

        Args:
            expression: JMESPath 表达式。
            default: 查询结果为 None（字段缺失或值为 null）时返回的值。

        Returns:
            查询结果，结果为 None 时返回 default。

        Raises:
            UnicodeDecodeError: 内容无法按当前编码解码。
            ValueError: 内容不是有效 JSON。
            jmespath.exceptions.JMESPathError: 表达式不合法。
        """

        result = _ENGINES["json"].extract(self.json(), expression)
        return default if result is None else result

    def extract(self, engine: Engine, rules: RuleSet) -> list[dict[str, Any]]:
        """用指定引擎按批量规则提取记录，等价于对应解析器的 match。

        json 与 jsonpath 读取 json() 的解码结果，css、xpath 与 regex 读取 text；
        需要 XML 格式、显式编码或自定义解析器时直接调用解析器的 match。

        Args:
            engine: json（JMESPath）、jsonpath、css、xpath 或 regex。
            rules: 字段映射、Rows、Product 或规则序列，语法见 `bricks.parsers.match`。

        Returns:
            提取出的记录列表。

        Raises:
            ValueError: engine 不受支持，或内容无法按引擎解码。
            UnicodeDecodeError: JSON 内容无法按当前编码解码。
        """

        if engine not in _ENGINES:
            raise ValueError(f"unsupported extract engine: {engine!r}")
        return _ENGINES[engine].match(self._source(engine), rules)

    def css(self, expression: str, **options: Any) -> list[Any]:
        """对 text 执行 CSS 选择，输出形式由 options 显式选择。

        Args:
            expression: CSS 选择器。
            **options: 传给 CssParser.extract 的选项，例如 output="text" 或 attribute="href"。

        Returns:
            命中结果列表。
        """
        return self._first_or_all("css", expression, options)

    def css_first(self, expression: str, default: Any = None, **options: Any) -> Any:
        """取 css 结果的首项。

        Args:
            expression: CSS 选择器。
            default: 没有命中时的返回值。
            **options: 传给 CssParser.extract 的选项。

        Returns:
            首项或 default。
        """
        return self._first_or_all(
            "css", expression, options, first=True, default=default
        )

    def xpath(self, expression: str, **options: Any) -> Any:
        """对 text 执行 XPath，保留列表、文本、数字或布尔结果。

        Args:
            expression: XPath 表达式。
            **options: 传给 XPathParser.extract 的选项，例如 variables。

        Returns:
            XPath 原始结果。
        """
        return self._first_or_all("xpath", expression, options)

    def xpath_first(self, expression: str, default: Any = None, **options: Any) -> Any:
        """取列表形式 XPath 结果的首项。

        Args:
            expression: 结果为列表的 XPath 表达式。
            default: 没有命中时的返回值。
            **options: 传给 XPathParser.extract 的选项。

        Returns:
            首项或 default。

        Raises:
            TypeError: 表达式结果不是列表，例如 count() 或 string()。
        """
        return self._first_or_all(
            "xpath", expression, options, first=True, default=default
        )

    def jsonpath(self, expression: str) -> list[Any]:
        """对 json() 的解码结果执行严格 JSONPath。

        Args:
            expression: JSONPath 表达式。

        Returns:
            命中值列表。
        """
        return self._first_or_all("jsonpath", expression, {})

    def jsonpath_first(self, expression: str, default: Any = None) -> Any:
        """取 jsonpath 结果的首项。

        Args:
            expression: JSONPath 表达式。
            default: 没有命中时的返回值。

        Returns:
            首项或 default。
        """
        return self._first_or_all(
            "jsonpath", expression, {}, first=True, default=default
        )

    def re(self, pattern: str, **options: Any) -> list[Any]:
        """对 text 执行正则，默认返回完整匹配。

        Args:
            pattern: 正则表达式。
            **options: 传给 RegexParser.extract 的选项，例如 group 或 flags。

        Returns:
            匹配结果列表。
        """
        return self._first_or_all("regex", pattern, options)

    def re_first(self, pattern: str, default: Any = None, **options: Any) -> Any:
        """取正则结果的首项。

        Args:
            pattern: 正则表达式。
            default: 没有匹配时的返回值。
            **options: 传给 RegexParser.extract 的选项。

        Returns:
            首项或 default。
        """
        return self._first_or_all(
            "regex", pattern, options, first=True, default=default
        )

    def _source(self, engine: str) -> Any:
        """按引擎选择解析输入：JSON 类读取 json()，其余读取 text。

        Returns:
            每次重新解码的输入。
        """
        return self.json() if engine in ("json", "jsonpath") else self.text

    def _first_or_all(
        self,
        engine: str,
        expression: str,
        options: Mapping[str, Any],
        *,
        first: bool = False,
        default: Any = None,
    ) -> Any:
        """用内置解析器执行单个表达式，按需取首项。

        Returns:
            原始结果，或列表首项及空列表时的 default。
        """
        parser = _ENGINES[engine]
        source = self._source(engine)
        if first:
            return parser.extract_first(source, expression, default=default, **options)
        return parser.extract(source, expression, **options)

    def copy(self, **changes: Any) -> Response:
        """复制响应，保留异常对象身份及原始异常链。

        Args:
            **changes: 与构造参数同名的覆盖项；覆盖 headers 而未指定 cookies
                时，会重新从响应头解析 Cookie。

        Returns:
            请求和历史响应副本独立的新响应。

        Raises:
            TypeError: 参数未知或类型不合法。
            ValueError: 字段值或状态与异常的组合不合法。
        """

        values = {name: getattr(self, name) for name in self.__slots__}
        if "headers" in changes and "cookies" not in changes:
            values["cookies"] = None
        values.update(changes)
        return type(self)(**values)

    def __copy__(self) -> Response:
        """返回字段共享的浅复制响应。"""

        values = {name: getattr(self, name) for name in self.__slots__}
        instance = type(self).__new__(type(self))
        for name, value in values.items():
            object.__setattr__(instance, name, value)
        return instance

    def __reduce__(self) -> tuple[object, tuple[dict[str, Any]]]:
        """通过响应构造器支持 pickle 与深复制。"""

        values = {name: getattr(self, name) for name in self.__slots__}
        values["request"] = self.request
        values["history"] = self.history
        return _restore_response, (values,)


def _restore_response(values: dict[str, Any]) -> Response:
    """从 pickle 字段快照重建响应。"""

    return Response(**values)
