# -*- coding: utf-8 -*-
# @Time    : 2023-12-11 13:21
# @Author  : Kem
# @Desc    :
import typing
from http.cookiejar import CookieJar, Cookie
from http.cookies import SimpleCookie

CookieTypes = typing.Union[
    "Cookies", CookieJar, typing.Dict[str, str], typing.List[typing.Tuple[str, str]]
]


# Code stolen from curl_cffi.requests.cookies.Cookies
class Cookies(typing.MutableMapping[str, str]):
    """
    HTTP Cookies, as a mutable mapping.
    """

    def __init__(self, cookies: typing.Optional[CookieTypes] = None) -> None:
        if cookies is None or isinstance(cookies, dict):
            self.jar = CookieJar()
            if isinstance(cookies, dict):
                for key, value in cookies.items():
                    self.set(key, value)
        elif isinstance(cookies, list):
            self.jar = CookieJar()
            for key, value in cookies:
                self.set(key, value)
        elif isinstance(cookies, Cookies):
            self.jar = CookieJar()
            for cookie in cookies.jar:
                self.jar.set_cookie(cookie)
        else:
            self.jar = cookies

    def set(self, name: str, value: str, domain: str = "", path: str = "/") -> None:
        """
        Set a cookie value by name. May optionally include domain and path.
        """
        kwargs = {
            "version": 0,
            "name": name,
            "value": value,
            "port": None,
            "port_specified": False,
            "domain": domain,
            "domain_specified": bool(domain),
            "domain_initial_dot": domain.startswith("."),
            "path": path,
            "path_specified": bool(path),
            "secure": False,
            "expires": None,
            "discard": True,
            "comment": None,
            "comment_url": None,
            "rest": {"HttpOnly": None},
            "rfc2109": False,
        }
        cookie = Cookie(**kwargs)  # type: ignore
        self.jar.set_cookie(cookie)

    def get(  # type: ignore
            self,
            name: str,
            default: typing.Optional[str] = None,
            domain: typing.Optional[str] = None,
            path: typing.Optional[str] = None,
    ) -> typing.Optional[str]:
        """
        Get a cookie by name. May optionally include domain and path
        in order to specify exactly which cookie to retrieve.
        """
        value = None
        matched_domain = ""
        for cookie in self.jar:
            if cookie.name == name:
                if domain is None or cookie.domain == domain:
                    if path is None or cookie.path == path:
                        # if cookies on two different domains do not share a same value
                        if (
                                value is not None
                                and not matched_domain.endswith(cookie.domain)
                                and not str(cookie.domain).endswith(matched_domain)
                                and value != cookie.value
                        ):
                            message = (
                                f"Multiple cookies exist with name={name} on "
                                f"{matched_domain} and {cookie.domain}"
                            )
                            raise ValueError(message)
                        value = cookie.value
                        matched_domain = cookie.domain or ""

        if value is None:
            return default
        return value

    def delete(
            self,
            name: str,
            domain: typing.Optional[str] = None,
            path: typing.Optional[str] = None,
    ) -> None:
        """
        Delete a cookie by name. May optionally include domain and path
        in order to specify exactly which cookie to delete.
        """
        if domain is not None and path is not None:
            return self.jar.clear(domain, path, name)

        remove = [
            cookie
            for cookie in self.jar
            if (
                    cookie.name == name
                    and (domain is None or cookie.domain == domain)
                    and (path is None or cookie.path == path)
            )
        ]

        for cookie in remove:
            self.jar.clear(cookie.domain, cookie.path, cookie.name)

    def clear(
            self, domain: typing.Optional[str] = None, path: typing.Optional[str] = None
    ) -> None:
        """
        Delete all cookies. Optionally include a domain and path in
        order to only delete a subset of all the cookies.
        """
        args = []
        if domain is not None:
            args.append(domain)
        if path is not None:
            assert domain is not None
            args.append(path)
        self.jar.clear(*args)

    def update(self, cookies: typing.Optional[CookieTypes] = None) -> None:  # noqa
        cookies = Cookies(cookies)
        for cookie in cookies.jar:
            self.jar.set_cookie(cookie)

    def __setitem__(self, name: str, value: str) -> None:
        return self.set(name, value)

    def __getitem__(self, name: str) -> str:
        value = self.get(name)
        if value is None:
            raise KeyError(name)
        return value

    def __delitem__(self, name: str) -> None:
        return self.delete(name)

    def __len__(self) -> int:
        return len(self.jar)

    def __iter__(self) -> typing.Iterator[str]:
        return (cookie.name for cookie in self.jar)

    def __bool__(self) -> bool:
        for _ in self.jar:
            return True
        return False

    def __repr__(self) -> str:
        cookies_repr = ", ".join(
            [
                f"<Cookie {cookie.name}={cookie.value} for {cookie.domain} />"
                for cookie in self.jar
            ]
        )

        return f"<Cookies[{cookies_repr}]>"

    @classmethod
    def by_jar(cls, jar):
        cookies = cls()
        for cookie in jar:
            if isinstance(cookie, dict):
                cookies.set(name=cookie["name"], value=cookie["value"], domain=cookie.get('domain'), path=cookie.get('path'))
            else:
                cookies.set(name=cookie.name, value=cookie.value, domain=cookie.domain, path=cookie.path)
        return cookies

    def load(self, set_cookie_string: str):
        simple_cookie = SimpleCookie()
        simple_cookie.load(set_cookie_string)
        # 遍历解析后的 SimpleCookie 来创建 Cookie 对象并添加到 CookieJar
        for key, morsel in simple_cookie.items():
            self.set(name=key, value=morsel.value, domain=morsel["domain"], path=morsel["path"])


if __name__ == '__main__':
    _cookies = Cookies()
    _cookies.load(
        "__Secure-3PSIDCC=ACA-OxNpMQHV4kdNyNK8OnUTF3laah3P7YWmsN0x4ypKJ2FVI1xL3r37iLUtpGMYvhhxN4K1iw; expires=Tue, 10-Dec-2024 05:36:40 GMT; path=/; domain=.youtube.com; Secure; HttpOnly; priority=high; SameSite=none"
    )
    print(_cookies)
