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
from typing import Union, Optional, Dict, List, Callable

from bricks.core import signals
from bricks.lib.headers import Header


class Request:

    def __init__(
            self,
            url: str,
            params: Optional[dict] = None,
            method: str = 'GET',
            body: Optional[Union[str, dict]] = None,
            headers: Union[Header, dict] = None,
            cookies: Dict[str, str] = None,
            options: dict = None,
            timeout: int = ...,
            allow_redirects: bool = True,
            proxies: Optional[str] = None,
            proxy: Optional[Union[dict, str, List[Union[dict, str]]]] = None,
            ok: Optional[Union[str, Dict[str, Union[type(signals.Signal), Callable]]]] = ...,
            retry: int = 0,
            max_retry: [int, float] = 5,
            use_session: bool = False,
    ) -> None:
        """

        :param url: 请求 URL
        :param params: 请求 URL 参数
        :param method: 请求方法, 默认 GET
        :param body: 请求 Body, 支持字典 / 字符串, 传入为字典的时候, 具体编码方式看 headers 里面的 Content-type
                    # 请求的 body, 全部设置为字典时需要和 headers 配合, 规则如下
                    # 如果是 json 格式, headers 里面设置 Content-Type 为 application/json
                    # 如果是 form urlencoded 格式, headers 里面设置 Content-Type 为 application/x-www-form-urlencoded
                    # 如果是 form data 格式, headers 里面设置 Content-Type 为 multipart/form-data
        :param headers: 请求头
        :param cookies: 请求 cookies
        :param options: 请求其他额外选项, 可以用于配合框架 / 下载器
        :param timeout: 请求超时时间, 填入 ... 为默认
        :param allow_redirects: 是否允许重定向
        :param proxies: 请求代理 Optional[str], 如 http://127.0.0.1:7890, 理解为 current proxy
        :param proxy: 代理 Key, 理解为 proxy from
        :param ok: 判断成功动态脚本, 字符串形式, 如通过 403 状态码可以写为: 200 <= response.status_code < 400 or response.status_code == 403
        :param retry: 当前重试次数
        :param max_retry: 最大重试次数
        :param use_session: 是否使用下载器的 session 模式
        """
        self.url = url
        self.use_session = use_session
        self.params = params
        self.cookies = cookies
        self.method = method.upper()
        self.body = body
        self._headers = Header(headers)
        self.options = options or {}
        self.timeout = timeout
        self.allow_redirects = allow_redirects
        self.proxies = proxies
        self.proxy: Optional[Union[dict, str, List[Union[dict, str]]]] = proxy
        self.ok = ok
        self.retry = retry
        self.max_retry = max_retry

    headers: Header = property(
        fget=lambda self: self._headers,
        fset=lambda self, v: setattr(self, "_headers", Header(v)),
        fdel=lambda self: setattr(self, "_headers", Header({})),
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

        # 添加请求头

        if self.cookies:
            cookie_str = "; ".join([f'{k}={v}' for k, v in self.cookies.items()])
        else:
            cookie_str = ""

        content_type = None
        if self.headers:
            for header, value in self.headers.items():
                if header.lower() == 'cookie':
                    cookie_str = "; ".join(list(filter(None, [value, cookie_str])))
                else:
                    if header.lower() == 'content-type':
                        content_type = value.lower()
                    parts.append(f"-H {shlex.quote(f'{header}: {value}')}")

        cookie_str and parts.append(f"-H {shlex.quote(f'Cookie: {cookie_str}')}")

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
                quoted_headers[header_key] = str(header_value).strip()

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

    def put_options(self, key: str, value, action="update"):
        func = getattr(self.options, action)
        return func(**{key: value})

    def get_options(self, key: str, default=None, action="get"):
        func = getattr(self.options, action)
        return func(key, default)

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
