# -*- coding: utf-8 -*-
# @Time    : 2023-11-14 20:42
# @Author  : Kem
# @Desc    : Request Model
import argparse
import json
import re
import shlex
import urllib.parse
from collections import OrderedDict
from http.cookies import SimpleCookie
from typing import Union, Optional, Dict

from bricks.lib.headers import Header


class Request:

    def __init__(
            self,
            url: str,
            params: Optional[dict] = None,
            method: str = 'GET',
            body: Union[str, dict] = None,
            headers: Union[Header, dict] = None,
            cookies: Union[Dict[str, str]] = None,
            options: dict = None,
            timeout: int = ...,
            allow_redirects: bool = True,
            proxies: Optional[str] = None,
            proxy: Optional[dict] = None,
            status_codes: Optional[dict] = ...,
            retry: int = 0,
            max_retry: int = 5,
    ) -> None:
        self.url = url
        self.params = params
        self.cookies = cookies
        self.method = method.upper()
        self.body = body
        self._headers = Header(headers, _dtype=str)
        self.options = options or {}
        self.timeout = timeout
        self.allow_redirects = allow_redirects
        self.proxies = proxies
        self.proxy = proxy
        self.status_codes = status_codes
        self.retry = retry
        self.max_retry = max_retry

    headers: Header = property(
        fget=lambda self: self._headers,
        fset=lambda self, v: setattr(self, "_headers", Header(v, _dtype=str)),
        fdel=lambda self: setattr(self, "_headers", Header({}, _dtype=str)),
        doc="请求头"
    )

    @property
    def real_url(self):
        # 解析原始URL
        parsed_url = urllib.parse.urlparse(self.url)
        # 提取查询字符串并解析为字典
        original_params = dict(urllib.parse.parse_qsl(parsed_url.query))

        # 更新原始参数字典
        original_params.update(self.params or {})

        # 重新构建查询字符串
        query_string = urllib.parse.urlencode(original_params).replace('+', '%20')

        # 构建新的URL
        new_url = urllib.parse.urlunparse((
            parsed_url.scheme,
            parsed_url.netloc,
            parsed_url.path,
            parsed_url.params,
            query_string,
            parsed_url.fragment
        ))

        return new_url

    @property
    def curl(self):
        # 开始构建curl命令
        parts = ["curl", f"-X {self.method.upper()}"]

        # 添加请求方法

        # 添加请求头
        if self.headers:
            for header, value in self.headers.items():
                parts.append(f"-H {shlex.quote(f'{header}: {value}')}")

        # 添加请求头
        content_type = None
        if self.headers:
            for header, value in self.headers.items():
                parts.append(f"-H {shlex.quote(f'{header}: {value}')}")
                if header.lower() == 'content-type':
                    content_type = value

        # 添加请求体
        if self.body:
            if content_type == 'application/json':
                # JSON格式的请求体
                json_data = json.dumps(self.body)
                parts.append(f"-d {shlex.quote(json_data)}")
            elif content_type == 'application/x-www-form-urlencoded':
                # URL编码格式的请求体
                form_data = urllib.parse.urlencode(self.body)
                parts.append(f"--data {shlex.quote(form_data)}")
            else:
                if isinstance(self.body, dict):
                    body_data = urllib.parse.urlencode(self.body)
                else:
                    body_data = self.body
                # 其他或未知类型，假定为字符串
                parts.append(f"--data {shlex.quote(body_data)}")

        # 添加URL（包括查询参数）
        parts.append(shlex.quote(self.real_url))

        # 转换为字符串格式的命令
        return " ".join(parts)

    @classmethod
    def from_curl(cls, curl_command):
        """
        从 curl 中导入

        :param curl_command:
        :return:
        """

        _parser = argparse.ArgumentParser()
        _parser.add_argument('command')
        _parser.add_argument('url')
        _parser.add_argument('-d', '--data')
        _parser.add_argument('-c', '--cookie', default=None)
        _parser.add_argument('-r', '--request', default=None)
        _parser.add_argument('-p', '--proxy', default=None)
        _parser.add_argument('-b', '--data-binary', '--data-raw', default=None)
        _parser.add_argument('-X', default='')
        _parser.add_argument('-H', '--header', action='append', default=[])
        _parser.add_argument('-du', '--data-urlencode', action='append', default=[], type=lambda x: x.split("="))
        _parser.add_argument('--compressed', action='store_true')
        _parser.add_argument('--location', action='store_true')
        _parser.add_argument('-k', '--insecure', action='store_true')
        _parser.add_argument('--user', '-u', default=())
        _parser.add_argument('-i', '--include', action='store_true')
        _parser.add_argument('-s', '--silent', action='store_true')

        if isinstance(curl_command, str):
            curl_command = curl_command.replace('curl --location', 'curl')
            tokens = shlex.split(curl_command.replace(" \\\n", " "))
        else:
            tokens = curl_command

        parsed_args = _parser.parse_args(tokens)

        if parsed_args.data_urlencode:
            post_data = {i[0]: i[1] for i in parsed_args.data_urlencode}
        else:
            post_data = parsed_args.data or parsed_args.data_binary

        method = parsed_args.request.lower() if parsed_args.request else 'post' if post_data else "get"

        if parsed_args.X:
            method = parsed_args.X.lower()

        cookie_dict = OrderedDict()
        if parsed_args.cookie:
            cookie = SimpleCookie(bytes(parsed_args.cookie, "ascii").decode("unicode-escape"))
            for key in cookie:
                cookie_dict[key] = cookie[key].value

        quoted_headers = OrderedDict()

        for curl_header in parsed_args.header:
            if curl_header.startswith(':'):
                occurrence = [m.start() for m in re.finditer(':', curl_header)]
                header_key, header_value = curl_header[:occurrence[1]], curl_header[occurrence[1] + 1:]
            else:
                header_key, header_value = curl_header.split(":", 1) if ':' in curl_header else (curl_header, "")

            if header_key.lower().strip("$") == 'cookie':
                cookie = SimpleCookie(bytes(header_value, "ascii").decode("unicode-escape"))
                for key in cookie:
                    cookie_dict[key] = cookie[key].value
            else:
                quoted_headers[header_key] = header_value.strip()

        # 解析原始URL
        parsed_url = urllib.parse.urlparse(parsed_args.url)
        # 提取查询字符串并解析为字典
        params = dict(urllib.parse.parse_qsl(parsed_url.query))

        return cls(
            url=parsed_url.scheme + "://" + parsed_url.netloc + parsed_url.path,
            params=params,
            method=method,
            body=post_data,
            headers=quoted_headers,
            cookies=cookie_dict,
        )

    def __str__(self):
        return f'<Request [{self.real_url}]>'

    __repr__ = __str__


if __name__ == '__main__':
    # req = Request(
    #     url="https://example.com/path",
    #     params={"query": "value"},
    #     method="POST",
    #     body="key=value",
    #     # headers={"Content-Type": "application/json"}
    # )
    # print(req.curl)
    req = Request.from_curl(
        """
       curl --location 'https://api.m.jd.com/api' \
--header 'Host: api.m.jd.com' \
--header 'Connection: keep-alive' \
--header 'X-Referer-Page: /pages/product/product' \
--header 'xweb_xhr: 1' \
--header 'X-Rp-Client: mini_2.0.0' \
--header 'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36 MicroMessenger/6.8.0(0x16080000) NetType/WIFI MiniProgramEnv/Mac MacWechat/WMPF MacWechat/3.8.5(0x1308050b)XWEB/31023' \
--header 'X-Referer-Package: wx862d8e26109609cb' \
--header 'Accept: */*' \
--header 'Sec-Fetch-Site: cross-site' \
--header 'Sec-Fetch-Mode: cors' \
--header 'Sec-Fetch-Dest: empty' \
--header 'Referer: https://servicewechat.com/wx862d8e26109609cb/134/page-frame.html' \
--header 'Accept-Language: zh-CN,zh;q=0.9' \
--header 'Content-Type: application/x-www-form-urlencoded' \
--data-urlencode 'appid=jd_healthy' \
--data-urlencode 'functionId=jdh_mini_viewHealthy' \
--data-urlencode 'body={"t":"1699945509078","fromType":"wxapp","apolloId":"0bffab44e51a45dd9abba25bb1a837d4","apolloSecret":"63776250363d4f58be246942e1d7dffc","clientVersion":"6.0.0","moudleId":"product","source":7,"wareId":"10035789319416","pageParams":{"wareId":"10032049399319"}}' \
--data-urlencode 'client=jdh_android' \
--data-urlencode 'clientVersion=191' \
--data-urlencode 'uuid=c916c23b-3f47-44df-9676-991a4572d9b7' \
--data-urlencode 'h5st=20231114150509103;39nitmmzz55zz5t9;3c4d4;tk03a89ae1bce18pMngxKzF4M3gx-8IEi7HovD4yyS7t9CS44Pf87FKEDnwhVM_flAfOnMwpsj0nCM527xQUv_NvUQIs;7169fa348bca9b8d1a2f69abcbe9a69c;4.1;1699945509103;275420328beca586eb4d58ea10bc056f1633a9c9f91541c365a5c67afa45389b1b104ca161b54546027decda1169ec06e8acaf7230ba14f851bd4921d74e865e63ccb34bc0b1c584632a2e22048ed0158b238cf99ee17e8a7bde6196c7a66692' \
--data-urlencode 'x-api-eid-token=jdd01w49LHCK5MMEEHEICAQSQJG7A5GGN6I3JZ3QOV5JYACPJPDQ5VWLWCTG3LJZW2LZOE336QNRLTNC6FOMNDCWIEUDXRD6VN2H2BPKLEACLMCUVOPELA5XV2DMG3GWB6MPTYIT'
        """
    )

    print(req.curl)
