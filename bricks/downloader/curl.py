# -*- coding: utf-8 -*-
# @Time    : 2023-12-10 21:07
# @Author  : Kem
# @Desc    : pycurl 下载器

import copy
import io
import os
import random
import urllib.parse
from typing import Union

from bricks.downloader import AbstractDownloader
from bricks.lib.cookies import Cookies
from bricks.lib.request import Request
from bricks.lib.response import Response
from bricks.utils import pandora

pandora.require("pycurl")

import pycurl  # noqa: E402
import certifi  # noqa: E402
import six  # noqa: E402


class Downloader(AbstractDownloader):
    """
    对 pycurl 进行的一层包装, pycurl 太难装了, 不推荐用


    """

    def __init__(
            self,
            ciphers: list = None,
            random_ciphers=False,
            httpversion: str = '1.1',
            sslversion: str = '1.2'
    ):
        self._ciphers = ciphers
        self.random_ciphers = random_ciphers
        self.sslversion = sslversion
        self.httpversion = httpversion

    def fetch(self, request: Union[Request, dict]) -> Response:
        """
        真使用 requests 发送请求并获取响应

        :param request:
        :return: `Response`

        """

        def with_header(raw_header_line):
            # HTTP standard specifies that headers are encoded in iso-8859-1.
            header_line = raw_header_line.decode("iso-8859-1")

            # Header lines include the first status line (HTTP/1.x ...).
            # We are going to ignore all lines that don't have a colon in them.
            # This will botch headers that are split on multiple lines...
            if ":" not in header_line:
                return

            # Save the header line for later parsing cookies
            k, v = header_line.split(":", 1)
            k, v = k.strip(), v.strip()
            headers[k] = v

        def make_cookie():
            cookies = Cookies()
            for cookie in curl.getinfo(pycurl.INFO_COOKIELIST):
                cookie_lis = cookie.split("\t")
                cookies.set(
                    name=cookie_lis[5],
                    value=cookie_lis[6],
                    domain=cookie_lis[0],
                    path=cookie_lis[2],
                )
            return cookies

        if request.use_session:
            curl = request.get_options("$session") or self.get_session()
        else:
            curl = pycurl.Curl()
        next_url = request.real_url

        options = {
            pycurl.SSL_CIPHER_LIST: self.set_cipher,
            pycurl.AUTOREFERER: 1,
            pycurl.VERBOSE: 0,
            pycurl.FOLLOWLOCATION: 0,
            pycurl.HEADERFUNCTION: with_header,
            pycurl.URL: next_url,
            pycurl.COOKIEFILE: "",
            # pycurl.ENCODING: "gzip"
        }

        options.update(self.build_headers_options(request.headers))
        options.update(self.build_cookie_options(request))
        options.update(self.build_body_options(request))
        options.update(self.build_http_method_options(request))
        options.update(self.build_timeout_options(request))
        options.update(self.build_ca_options(request))
        options.update(self.build_cert_options(request))
        options.update(self.build_proxy_options(request))
        options.update(self.build_version_options(request))
        _referer = request.options.pop("$referer", False)
        options.update(request.options)

        res = Response.make_response(request=request)

        _redirect_count = 0

        try:

            for option, value in options.items():
                curl.setopt(option, value)

            while True:
                assert _redirect_count < 999, "已经超过最大重定向次数: 999"
                body, headers = io.BytesIO(), {}
                curl.setopt(pycurl.URL, next_url)
                curl.setopt(pycurl.WRITEFUNCTION, body.write)
                curl.setopt(pycurl.HTTPHEADER, self.build_headers_options(request.headers)[pycurl.HTTPHEADER])
                curl.perform()

                next_url = headers.get('Location') or headers.get('location')

                if request.allow_redirects and next_url:
                    next_url = urllib.parse.urljoin(options[pycurl.URL], next_url)
                    _redirect_count += 1
                    res.history.append(
                        Response(
                            content=body.getvalue(),
                            status_code=curl.getinfo(pycurl.HTTP_CODE),
                            headers=headers,
                            url=options[pycurl.URL],
                            request=Request(
                                url=curl.getinfo(pycurl.EFFECTIVE_URL),
                                method=request.method,
                                headers=copy.deepcopy(request.headers),
                            ),
                            cookies=make_cookie()
                        )
                    )
                    _referer and request.headers.update(Referer=options[pycurl.URL])
                    options[pycurl.URL] = next_url

                else:

                    res.content = body.getvalue()
                    res.status_code = curl.getinfo(pycurl.HTTP_CODE)
                    res.headers = headers
                    res.url = curl.getinfo(pycurl.EFFECTIVE_URL)
                    res.cookies = make_cookie()
                    res.request = request
                    return res

        finally:
            not request.use_session and curl.close()

    @property
    def set_cipher(self):

        if not self._ciphers:
            self._ciphers = [
                'TLS_AES_128_GCM_SHA256', 'TLS_AES_256_GCM_SHA384', 'TLS_CHACHA20_POLY1305_SHA256',
                'ECDHE-ECDSA-AES128-GCM-SHA256', 'ECDHE-RSA-AES128-GCM-SHA256', 'ECDHE-ECDSA-AES256-GCM-SHA384',
                'ECDHE-RSA-AES256-GCM-SHA384', 'ECDHE-ECDSA-CHACHA20-POLY1305', 'ECDHE-RSA-CHACHA20-POLY1305',
                'ECDHE-RSA-AES128-SHA', 'ECDHE-RSA-AES256-SHA', 'AES128-GCM-SHA256', 'AES256-GCM-SHA384',
                'AES128-SHA,AES256-SHA'
            ]

        self.random_ciphers and random.shuffle(self._ciphers)
        return ','.join(self._ciphers)

    @staticmethod
    def build_headers_options(req_headers):
        """Returns a dict with the pycurl option for the headers."""
        req_headers = req_headers.copy()
        headers = [
            "{name}: {value}".format(name=name, value=value)
            for name, value in six.iteritems(req_headers)
        ]
        options = {pycurl.HTTPHEADER: headers}
        if req_headers.get("accept-encoding", "").__contains__("gzip"):
            options.update({pycurl.ENCODING: "gzip"})

        return options

    @staticmethod
    def build_cookie_options(request: Request):
        """Returns a dict with the pycurl option for the headers."""
        if request.cookies:
            cookie_string = '; '.join([f'{key}={value}' for key, value in request.cookies.items()])
            options = {pycurl.COOKIE, cookie_string}
        else:
            options = {}

        return options

    @staticmethod
    def build_http_method_options(request: Request):
        method = request.method
        method = method.upper() if method else "GET"

        if method == "GET":
            return {}
        else:
            return {pycurl.CUSTOMREQUEST: method}

    def build_body_options(self, request: Request):

        if request.method == "HEAD":
            # Body is not allowed for HEAD
            return {pycurl.NOBODY: True}

        elif request.body:
            body = self.parse_data(request)
            is_encoded_form = body['type'] == "application/x-www-form-urlencoded"

            if is_encoded_form:
                return {pycurl.POSTFIELDS: body['data']}
            else:
                _body_stream = six.BytesIO(six.ensure_binary(body['data']))

                return {
                    pycurl.UPLOAD: True,
                    pycurl.READFUNCTION: _body_stream.read,
                }

        else:
            return {}

    @staticmethod
    def build_timeout_options(request: Request):
        """Returns the curl timeout options."""
        if request.timeout is ...: request.timeout = 5
        if isinstance(request.timeout, (tuple, list)):
            conn_timeout, read_timeout = request.timeout
            total_timeout = conn_timeout + read_timeout
            return {
                pycurl.TIMEOUT_MS: int(1000 * total_timeout),
                pycurl.CONNECTTIMEOUT_MS: int(1000 * conn_timeout),
            }
        elif request.timeout:
            return {pycurl.TIMEOUT_MS: int(1000 * request.timeout)}
        else:
            return {}

    @staticmethod
    def build_ca_options(request: Request):
        """Configures the CA of this curl request."""
        verify = request.options.get("verify", False)
        if verify:
            ca_value = (
                verify
                if isinstance(verify, six.string_types)
                else certifi.where()
            )

            # Requests allows the verify parameter to be a file or a directory. This requires
            # a different CURL option for each case
            ca_opt = pycurl.CAPATH if os.path.isdir(ca_value) else pycurl.CAINFO

            return {
                pycurl.SSL_VERIFYHOST: 2,
                pycurl.SSL_VERIFYPEER: 2,
                ca_opt: ca_value,
            }
        else:
            return {
                pycurl.SSL_VERIFYHOST: 0,
                pycurl.SSL_VERIFYPEER: 0,
            }

    @staticmethod
    def build_cert_options(request: Request):
        """Configures the SSL certificate of this curl request."""
        cert = request.options.get("cert", False)
        if cert:
            if isinstance(cert, six.string_types):
                cert_path = cert
                return {pycurl.SSLCERT: cert_path}
            else:
                cert_path, key_path = cert
                return {
                    pycurl.SSLCERT: cert_path,
                    pycurl.SSLKEY: key_path,
                }
        else:
            return {}

    @staticmethod
    def build_proxy_options(request: Request):
        if request.proxies:
            components = urllib.parse.urlparse(request.proxies)
            s2t = {
                "http": pycurl.PROXYTYPE_HTTP,
                # "https": pycurl.PROXYTYPE_HTTPS,
                "socks5": pycurl.PROXYTYPE_SOCKS5,
                "socks4": pycurl.PROXYTYPE_SOCKS4,
            }
            options = {
                pycurl.PROXYTYPE: s2t.get(components.scheme) or pycurl.PROXYTYPE_HTTP,
                pycurl.PROXY: f'{components.scheme}://{components.hostname}:{components.port}',
            }

            if components.username and components.password:
                options.update({
                    pycurl.PROXYAUTH: pycurl.HTTPAUTH_BASIC,
                    pycurl.PROXYUSERPWD: f'{components.username}:{components.password}',
                })

            return options
        else:
            return {}

    def build_version_options(self, request: Request):
        options = {}

        httpversion = {
            "1.0": pycurl.CURL_HTTP_VERSION_1_0,
            "1.1": pycurl.CURL_HTTP_VERSION_1_1,
            "2.0": pycurl.CURL_HTTP_VERSION_2_0,
        }

        sslversion = {
            "1": pycurl.SSLVERSION_TLSv1,
            "1.0": pycurl.SSLVERSION_TLSv1_0,
            "1.1": pycurl.SSLVERSION_TLSv1_1,
            "1.2": pycurl.SSLVERSION_TLSv1_2,
            "1.3": pycurl.SSLVERSION_TLSv1_3,  # noqa
        }
        c_httpversion = request.options.get('httpversion') or self.httpversion
        c_sslversion = request.options.get('sslversion') or self.sslversion
        options.update({
            pycurl.HTTP_VERSION: httpversion.get(c_httpversion) or pycurl.CURL_HTTP_VERSION_1_1,
            pycurl.SSLVERSION: sslversion.get(c_sslversion) or pycurl.SSLVERSION_TLSv1_2,
            pycurl.SSL_ENABLE_NPN: 0,
        })

        try:
            options.update({
                pycurl.SSL_ENABLE_ALPS: 1,  # noqa
                pycurl.SSL_CERT_COMPRESSION: "brotli",  # noqa
                pycurl.HTTP2_PSEUDO_HEADERS_ORDER: "masp",  # noqa
            })
        except:  # noqa
            pass

        return options

    def make_session(self):
        return pycurl.Curl()


if __name__ == '__main__':
    downloader = Downloader()
    rsp = downloader.fetch(Request(url="https://httpbin.org/cookies/set?freeform=123", use_session=True))
    print(rsp.cookies)
    rsp = downloader.fetch(Request(url="https://httpbin.org/cookies", use_session=True))
    print(rsp.text)
