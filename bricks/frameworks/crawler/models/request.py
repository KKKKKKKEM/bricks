"""可编辑的 HTTP 请求模型：描述请求数据，不承担执行状态、会话管理和重试。"""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping, MutableMapping, Sequence
from copy import deepcopy
from typing import Any, Literal, get_args
from urllib.parse import urlencode

from ._validation import duration, entries, http_url, token
from .body import BodyInput, BodyType, encode_json, encode_multipart
from .cookies import RequestCookies
from .headers import HeaderInput, MutableHeaders

HttpMethod = Literal[
    "GET", "HEAD", "POST", "PUT", "DELETE", "CONNECT", "OPTIONS", "TRACE", "PATCH"
]
_HTTP_METHODS = get_args(HttpMethod)
QueryValue = str | int | float | bool | None
# 映射中的列表值展开为重复参数；键值对序列保留原始顺序。
QueryInput = (
    Mapping[str, QueryValue | Sequence[QueryValue]] | Iterable[tuple[str, QueryValue]]
)


def _query_pairs(values: QueryInput | None) -> tuple[tuple[str, str], ...]:
    """规范化查询参数，省略 None 值。

    Args:
        values: 参数映射或有序键值对，None 表示空参数。

    Returns:
        保留重复参数的有序字符串对。

    Raises:
        TypeError: 参数名称、值或键值对结构不合法。
    """

    if values is None:
        return ()
    if isinstance(values, QueryParams):
        return values.raw
    result: list[tuple[str, str]] = []
    for key, value in entries(values, "query"):
        if not isinstance(key, str):
            raise TypeError("query keys must be strings")
        group = (
            value
            if isinstance(value, Sequence)
            and not isinstance(value, (str, bytes, bytearray))
            else (value,)
        )
        for item in group:
            if item is None:
                continue
            if not isinstance(item, (str, int, float, bool)):
                raise TypeError(
                    "query values must be strings, numbers, booleans or None"
                )
            result.append((key, str(item)))
    return tuple(result)


class QueryParams(MutableMapping[str, str]):
    """URL 查询参数容器，用于管理分页、筛选等请求参数。

    支持按名称查询、修改、删除和追加参数，并保留参数顺序与同名多值，
    可通过 raw 取得完整键值对，用于构造查询字符串。
    """

    def __init__(self, values: QueryInput | None = None) -> None:
        """构造独立的查询参数容器。

        Args:
            values: 参数映射或有序键值对，None 表示空参数。

        Raises:
            TypeError: 参数名称、值或键值对结构不合法。
        """

        self._pairs = _query_pairs(values)

    @property
    def raw(self) -> tuple[tuple[str, str], ...]:
        """返回包含重复字段的有序键值对快照，不允许原地修改。"""

        return self._pairs

    def get_all(self, name: str) -> tuple[str, ...]:
        """查询同名参数的全部值。

        Args:
            name: 区分大小写的参数名。

        Returns:
            按顺序排列的值，不存在时返回空元组。
        """

        return tuple(value for key, value in self._pairs if key == name)

    def __getitem__(self, name: str) -> str:
        """查询参数的最后一个值。

        Args:
            name: 区分大小写的参数名。

        Returns:
            最后一个匹配值。

        Raises:
            KeyError: 参数不存在。
        """

        values = self.get_all(name)
        if not values:
            raise KeyError(name)
        return values[-1]

    def __setitem__(self, name: str, value: QueryValue | Sequence[QueryValue]) -> None:
        """替换同名参数的全部值。

        Args:
            name: 区分大小写的参数名。
            value: 单值或序列，序列展开为重复参数，None 表示删除。

        Raises:
            TypeError: 参数名或值的类型不合法。
        """

        prepared = _query_pairs({name: value})
        self._pairs = tuple(pair for pair in self._pairs if pair[0] != name) + prepared

    def __delitem__(self, name: str) -> None:
        """删除同名参数的全部值。

        Args:
            name: 区分大小写的参数名。

        Raises:
            KeyError: 参数不存在。
        """

        self[name]
        self._pairs = tuple(pair for pair in self._pairs if pair[0] != name)

    def __iter__(self) -> Iterator[str]:
        """按首次出现顺序迭代不重复的参数名。"""

        return iter(dict(self._pairs))

    def __len__(self) -> int:
        """返回不同参数名的数量，不是键值对总数。"""

        return len(dict(self._pairs))

    def add(self, name: str, value: QueryValue) -> None:
        """追加参数，不覆盖已有同名值。

        Args:
            name: 区分大小写的参数名。
            value: 要追加的单值，None 表示不添加。

        Raises:
            TypeError: 参数名或值的类型不合法。
        """

        self._pairs += _query_pairs(((name, value),))


class Request:
    """HTTP 请求类，用于构造和编辑爬虫需要发送的请求。

    描述请求地址、方法、查询参数、请求头、Cookie、请求体及超时和代理设置。
    支持 JSON、表单、文件上传和原始内容编码，可以在请求前 Hook 中修改，
    也可以复制为独立请求，或与 cURL 命令相互转换。

    Request 负责组织请求数据，实际网络发送由下载器完成。
    """

    __slots__ = (
        "url",
        "method",
        "params",
        "headers",
        "cookies",
        "_body",
        "_body_source",
        "_body_type",
        "_body_content_type",
        "timeout",
        "allow_redirects",
        "proxy",
    )

    url: str  # 绝对 HTTP(S) 地址，可包含原始查询字符串和片段。
    method: HttpMethod  # HTTP 方法，赋值后统一为大写。
    params: QueryParams  # 追加到 URL 的有序查询参数，允许同名多值。
    headers: MutableHeaders  # 可编辑请求头，查询不区分大小写。
    cookies: RequestCookies  # 本次请求显式携带的 Cookie，不是会话 Cookie 容器。
    _body: bytes | None
    _body_source: BodyInput
    _body_type: BodyType
    _body_content_type: str | None
    timeout: float | None  # 请求时限，单位秒；None 表示不限时。
    allow_redirects: bool  # 是否允许下载器跟随 HTTP 重定向。
    proxy: str | None  # 本次请求的代理地址，None 表示未指定。

    def __init__(
        self,
        url: str,
        *,
        method: HttpMethod = "GET",
        params: QueryInput | None = None,
        headers: HeaderInput | None = None,
        cookies: Mapping[str, str] | None = None,
        body: BodyInput = None,
        body_type: BodyType = "auto",
        timeout: float | None = 30,
        allow_redirects: bool = True,
        proxy: str | None = None,
    ) -> None:
        """构造请求，复制参数容器并编码 body。

        Args:
            url: 绝对 HTTP 或 HTTPS 地址；不接受相对地址。
            method: HTTP 方法，默认 GET；不会根据 body 自动改为 POST。
            params: 查询参数映射或有序键值对；序列值展开，None 值省略。
            headers: 请求头映射或有序键值对，保留重复字段。
            cookies: 本次请求的 Cookie 名称和值。
            body: 请求体原始数据；None 表示无请求体，读取时返回 bytes。
            body_type: auto 将字典和序列编码为 JSON，文本和字节直接发送；
                json 序列化 JSON 值；form 编码表单；multipart 编码字段和
                UploadFile；raw 只接受文本或字节。文本统一按 UTF-8 编码。
            timeout: 正数秒数，默认 30；None 表示不限时。
            allow_redirects: 是否跟随重定向，默认 True。
            proxy: 代理地址，默认 None。

        Raises:
            TypeError: 字段类型错误或请求体无法编码。
            ValueError: 字段值非法。
        """

        values = {
            "url": url,
            "method": method,
            "params": params,
            "headers": headers,
            "cookies": cookies,
            "body_type": body_type,
            "body": body,
            "timeout": timeout,
            "allow_redirects": allow_redirects,
            "proxy": proxy,
        }
        for name, value in values.items():
            setattr(self, name, value)

    def __setattr__(self, name: str, value: Any) -> None:
        """校验并设置字段，校验失败时保留该字段原值。

        Args:
            name: 字段名。
            value: 待校验的新值。

        Raises:
            TypeError: 值的类型不合法。
            ValueError: 字段值不合法。
            AttributeError: 字段不存在。
        """

        if name == "url":
            value = http_url(value)
        elif name == "method":
            value = token(value, "method").upper()
            if value not in _HTTP_METHODS:
                raise ValueError(f"method must be one of {', '.join(_HTTP_METHODS)}")
        elif name == "params":
            value = QueryParams(value)
        elif name == "headers":
            value = MutableHeaders(value)
        elif name == "cookies":
            value = RequestCookies(value)
        elif name == "timeout":
            value = duration(value, "timeout")
        elif name == "allow_redirects":
            if type(value) is not bool:
                raise TypeError("allow_redirects must be a boolean")
        elif name == "proxy":
            if value is not None and (not isinstance(value, str) or not value.strip()):
                raise ValueError("proxy must be a non-empty string or None")
        object.__setattr__(self, name, value)

    @property
    def body(self) -> bytes | None:
        """返回编码后的请求体字节；None 表示无请求体。"""

        return self._body

    @body.setter
    def body(self, value: BodyInput) -> None:
        """按当前模式编码请求体，成功后清除旧 Content-Length。

        Args:
            value: 新的原始请求体，None 表示无请求体。

        Raises:
            TypeError: 当前模式不支持输入类型。
            ValueError: 请求体不能按当前模式编码。
        """

        self._set_body(value, self.body_type)

    @property
    def body_type(self) -> BodyType:
        """返回编码模式：auto、json、form、multipart 或 raw。"""

        return self._body_type

    @body_type.setter
    def body_type(self, value: BodyType) -> None:
        """切换编码模式，重新编码最后一次赋值的原始数据副本。

        Args:
            value: auto、json、form、multipart 或 raw。

        Raises:
            ValueError: 模式不存在或请求体无法编码。
            TypeError: 请求体不符合目标模式的要求。
        """

        if value not in ("auto", "json", "form", "multipart", "raw"):
            raise ValueError("body_type must be auto, json, form, multipart or raw")
        if hasattr(self, "_body_source"):
            self._set_body(self._body_source, value)
        else:
            self._body_type = value

    def _set_body(self, value: BodyInput, body_type: BodyType) -> None:
        """编码成功后统一更新数据副本、字节与自动请求头。

        Args:
            value: 原始请求体。
            body_type: 已校验的编码模式。
        """

        encoded: bytes | None
        content_type = None
        if value is None:
            encoded = None
        elif body_type == "multipart":
            encoded, content_type = encode_multipart(value)
        elif body_type == "json" or (
            body_type == "auto" and isinstance(value, (dict, list, tuple))
        ):
            encoded = encode_json(value)
            content_type = "application/json"
        elif body_type == "form":
            if isinstance(value, (str, bytes)):
                encoded = value.encode("utf-8") if isinstance(value, str) else value
            elif isinstance(value, (dict, list, tuple)):
                encoded = urlencode(_query_pairs(value)).encode("ascii")
            else:
                raise TypeError("form body must be fields, encoded text or bytes")
            content_type = "application/x-www-form-urlencoded"
        elif isinstance(value, (str, bytes)):
            encoded = value.encode("utf-8") if isinstance(value, str) else value
        else:
            raise TypeError("raw/auto body must be text or bytes; use json for scalars")
        source = deepcopy(value)
        if hasattr(self, "_body"):
            self.headers.pop("Content-Length", None)
        previous = getattr(self, "_body_content_type", None)
        if previous is not None and self.headers.get_all("Content-Type") == (previous,):
            del self.headers["Content-Type"]
        if body_type == "multipart" and content_type is not None:
            self.headers.pop("Content-Type", None)
        generated = None
        if content_type is not None and "Content-Type" not in self.headers:
            self.headers["Content-Type"] = content_type
            generated = content_type
        self._body_content_type = generated
        self._body_source = source
        self._body_type = body_type
        self._body = encoded

    @property
    def real_url(self) -> str:
        """返回移除片段并追加 params 的请求地址，不重新编码 URL 原有查询字符串。"""

        base = self.url.split("#", 1)[0]
        if not self.params:
            return base
        separator = "?" if "?" not in base else "" if base.endswith(("?", "&")) else "&"
        return base + separator + urlencode(self.params.raw)

    def copy(self, **changes: Any) -> Request:
        """复制请求，并应用指定的字段覆盖项。

        headers、params、cookies 和原始 body 副本相互隔离。未覆盖 body 或
        body_type 时保留实际请求体字节，包括 multipart boundary。
        Args:
            **changes: 与构造参数同名的覆盖项，仍须通过构造校验。

        Returns:
            可变容器和原始数据副本独立的新请求。

        Raises:
            TypeError: 覆盖项未知或类型不合法。
            ValueError: 覆盖值不合法。
        """

        values = {
            name: getattr(self, name)
            for name in Request.__slots__
            if not name.startswith("_")
        }
        headers = MutableHeaders(self.headers)
        if self._body_content_type is not None and headers.get_all("Content-Type") == (
            self._body_content_type,
        ):
            del headers["Content-Type"]
        values.update(body=self._body_source, body_type=self.body_type, headers=headers)
        values.update(changes)
        if "body" in changes or "body_type" in changes:
            headers = MutableHeaders(values["headers"])
            headers.pop("Content-Length", None)
            values["headers"] = headers
        copied = type(self)(**values)
        if not {"body", "body_type"}.intersection(changes):
            # 不改变请求体时，复制必须保留原始字节及对应的 multipart boundary。
            copied._body = self._body
            if self._body_content_type is not None and self.body_type == "multipart":
                copied.headers["Content-Type"] = self._body_content_type
                copied._body_content_type = self._body_content_type
        return copied

    def to_curl(self, *, body_file: str | None = None) -> str:
        """返回可用于 POSIX shell 的单条 cURL 命令，不执行命令。

        Args:
            body_file: 可选的请求体文件路径，输出为 --data-binary @路径。
                本方法不写文件，调用方须将完整 request.body 存入该路径。
                含 NUL 或不能按 UTF-8 解码的请求体必须提供此参数。

        Returns:
            包含原始认证头和 Cookie 的命令文本，不做脱敏。

        Raises:
            ValueError: 请求体或参数不能表示为命令行参数。
        """

        from .curl import render_curl

        return render_curl(self, body_file)

    @classmethod
    def from_curl(
        cls, command: str, *, files: Mapping[str, bytes] | None = None
    ) -> Request:
        """解析单条 cURL 命令并返回请求，不执行 shell、不展开变量、不读取文件。

        Args:
            command: 以 curl 开头的 POSIX 命令文本，支持引号和行间续行。
            files: 命令引用的文件名到 bytes 的映射；包含 @文件或上传字段时，
                调用方必须显式提供对应内容，也不会自动读取标准输入。

        Returns:
            使用 cURL 默认设置的请求：GET、不跟随重定向、不限时。
            已编码数据保留原始字节，不自动还原为 JSON 对象。

        Raises:
            TypeError: command 不是文本或文件内容不是 bytes。
            ValueError: 多 URL、未知选项、缺少文件内容或存在不支持的组合。
        """

        from .curl import parse_curl

        return cls(**parse_curl(command, files))
