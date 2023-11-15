# -*- coding: utf-8 -*-
# @Time    : 2023-11-15 14:14
# @Author  : Kem
# @Desc    :

import copy
import urllib.parse
from typing import Union

from curl_cffi import requests

from bricks.downloader import primordial
from bricks.lib.request import Request
from bricks.lib.response import Response


class Downloader(primordial.Downloader):
    """
    对 cffi 进行的一层包装, 类似 requests, tls 与浏览器保持一致
    兼容 Windows / Mac / Linux


    """

    def __init__(self, impersonate: Union[requests.BrowserType, str] = None):
        if isinstance(impersonate, requests.BrowserType):
            impersonate = impersonate.value

        self.impersonate = impersonate

    def fetch(self, request: Union[Request, dict]) -> Response:
        """
        真使用 requests 发送请求并获取响应

        :param request:
        :return: `Response`

        """

        res = Response.make_response(request=request)
        options = {
            'method': request.method,
            'headers': request.headers,
            'cookies': request.cookies,
            "data": self.parse_data(request)['data'],
            'files': request.options.get('files'),
            'auth': request.options.get('auth'),
            'timeout': 5 if request.timeout is ... else request.timeout,
            'allow_redirects': False,
            'proxies': request.proxies,
            'verify': request.options.get("verify", False),
            'impersonate': request.options.get("impersonate") or self.impersonate,
        }

        next_url = request.real_url
        _redirect_count = 0

        while True:
            assert _redirect_count < 999, "已经超过最大重定向次数: 999"
            response = requests.request(**{**options, "url": next_url})
            last_url, next_url = next_url, response.headers.get('location') or response.headers.get('Location')
            if request.allow_redirects and next_url:
                next_url = urllib.parse.urljoin(response.url, next_url)
                _redirect_count += 1
                res.history.append(
                    Response(
                        content=response.content,
                        headers=response.headers,
                        cookies=response.cookies,
                        url=response.url,
                        status_code=response.status_code,
                        request=Request(
                            url=last_url,
                            method=request.method,
                            headers=copy.deepcopy(options.get('headers'))
                        )
                    )
                )
                request.options.get('auto_referer', True) and options['headers'].update(Referer=response.url)

            else:
                res.content = response.content
                res.headers = response.headers
                res.cookies = response.cookies
                res.url = response.url
                res.status_code = response.status_code
                res.request = request

                return res


if __name__ == '__main__':
    downloader = Downloader()
    res = downloader.fetch(Request.from_curl(
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
    ))

    print(res.text)
