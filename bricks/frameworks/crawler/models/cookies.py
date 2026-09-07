"""响应 Cookie 容器，保留属性并提供请求头字符串转换。"""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping, MutableMapping
from copy import deepcopy
from email.message import Message
from http.client import HTTPResponse
from http.cookiejar import Cookie, CookieJar, DefaultCookiePolicy
from typing import cast
from urllib.parse import urlsplit
from urllib.request import Request as URLRequest

from ._validation import cookies as validate_cookies
from ._validation import http_url, request_cookies
from .headers import Headers


class RequestCookies(MutableMapping[str, str]):
    """Editable request cookies with validation on every write."""

    def __init__(self, values: Mapping[str, str] | None = None) -> None:
        self._values = dict(request_cookies(values))

    def __getitem__(self, name: str) -> str:
        return self._values[name]

    def __setitem__(self, name: str, value: str) -> None:
        self._values.update(request_cookies({name: value}))

    def __delitem__(self, name: str) -> None:
        del self._values[name]

    def __iter__(self) -> Iterator[str]:
        return iter(self._values)

    def __len__(self) -> int:
        return len(self._values)


class _CookieResponse:
    def __init__(self, headers: Headers) -> None:
        self._headers = Message()
        for name, value in headers.raw:
            self._headers[name] = value

    def info(self) -> Message:
        return self._headers


class Cookies:
    """保存响应 Cookie 的容器，用于查询值、查看属性和生成 Cookie 请求头。

    保留同名但域或路径不同的记录；对外返回副本，避免意外修改响应数据。
    to_string() 与 str() 可以直接提取 Cookie 字符串。
    """

    def __init__(
        self,
        values: Cookies | CookieJar | Iterable[Cookie] | Mapping[str, str] = (),
        *,
        url: str | None = None,
    ) -> None:
        """创建独立的 Cookie 容器。

        Args:
            values: Cookies、CookieJar、Cookie 序列或名称到值的简写字典。
            url: 为简写字典中的 Cookie 指定来源主机。

        Raises:
            TypeError: Cookie 记录或名称和值的类型不合法。
            ValueError: 来源 URL 或 Cookie 名称和值不合法。
        """

        if url is not None:
            http_url(url)
        if isinstance(values, Mapping):
            domain = urlsplit(url).hostname if url else ""
            records = [
                Cookie(
                    version=0,
                    name=name,
                    value=value,
                    port=None,
                    port_specified=False,
                    domain=domain or "",
                    domain_specified=False,
                    domain_initial_dot=False,
                    path="/",
                    path_specified=True,
                    secure=False,
                    expires=None,
                    discard=True,
                    comment=None,
                    comment_url=None,
                    rest={},
                )
                for name, value in validate_cookies(
                    cast(Mapping[str, str], values)
                ).items()
            ]
        else:
            records = list(values)
        jar = CookieJar()
        for cookie in records:
            if not isinstance(cookie, Cookie):
                raise TypeError("cookies must contain http.cookiejar.Cookie objects")
            validate_cookies({cookie.name: cookie.value or ""})
            jar.set_cookie(deepcopy(cookie))
        self._records = tuple(jar)

    @classmethod
    def from_headers(cls, headers: Headers, *, url: str) -> Cookies:
        """从响应头提取 Cookie。

        Args:
            headers: 包含原始 Set-Cookie 的响应头。
            url: 来源 URL，用于默认域、路径和来源校验。

        Returns:
            按标准库策略接受的 Cookie 容器。

        Raises:
            ValueError: 来源 URL 不合法。
        """

        http_url(url)
        jar = CookieJar(
            policy=DefaultCookiePolicy(
                strict_ns_domain=DefaultCookiePolicy.DomainStrictNonDomain,
            )
        )
        # CookieJar 实际只要求 info()，类型声明却限定为完整 HTTPResponse。
        jar.extract_cookies(
            cast(HTTPResponse, _CookieResponse(headers)), URLRequest(url)
        )
        return cls(jar)

    def __iter__(self) -> Iterator[Cookie]:
        """迭代包含域、路径、有效期等属性的 Cookie 副本。"""

        return (deepcopy(cookie) for cookie in self._records)

    def __len__(self) -> int:
        """返回 Cookie 记录数量，同名但域或路径不同的记录分别计数。"""

        return len(self._records)

    def get(
        self,
        name: str,
        default: str | None = None,
        *,
        domain: str | None = None,
        path: str | None = None,
    ) -> str | None:
        """按名称查询 Cookie 的值。

        Args:
            name: Cookie 名称。
            default: 没有匹配记录时的返回值。
            domain: 可选的精确域限定。
            path: 可选的精确路径限定。

        Returns:
            匹配记录的值，无匹配时返回 default。

        Raises:
            ValueError: 匹配多个记录，需要指定 domain 或 path。
        """

        matches = [
            cookie
            for cookie in self._records
            if cookie.name == name
            and (domain is None or cookie.domain == domain)
            and (path is None or cookie.path == path)
        ]
        if len(matches) > 1:
            raise ValueError("multiple cookies match; specify domain and path")
        return matches[0].value if matches else default

    def to_string(self, *, url: str | None = None) -> str:
        """返回 name=value; name2=value2 格式的字符串。

        Args:
            url: 目标 URL；None 表示提取全部记录，否则按域、路径、Secure
                和有效期筛选。未指定来源域的简写 Cookie 不参与 URL 筛选。

        Returns:
            Cookie 请求头的值，不包含 Cookie: 前缀。

        Raises:
            ValueError: 目标 URL 不合法。
        """

        if url is None:
            return "; ".join(
                cookie.name if cookie.value is None else f"{cookie.name}={cookie.value}"
                for cookie in self._records
            )
        http_url(url)
        jar = CookieJar(
            policy=DefaultCookiePolicy(
                strict_ns_domain=DefaultCookiePolicy.DomainStrictNonDomain,
            )
        )
        for cookie in self._records:
            if cookie.domain:
                jar.set_cookie(deepcopy(cookie))
        request = URLRequest(url)
        jar.add_cookie_header(request)
        return request.get_header("Cookie", "")

    def __str__(self) -> str:
        """返回全部 Cookie 的字符串，与不传参数的 to_string() 一致。"""

        return self.to_string()
