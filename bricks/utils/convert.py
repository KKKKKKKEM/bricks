# -*- coding: utf-8 -*-
# @Time    : 2023-12-12 13:12
# @Author  : Kem
# @Desc    :
import contextlib
import inspect
import json
from typing import Union, Callable

from bricks.lib.items import Items
from bricks.lib.request import Request
from bricks.lib.response import Response
from bricks.spider.air import Spider
from bricks.utils import pandora


def curl2resp(curl_cmd: str, options: dict = None) -> Response:
    """
    curl 转响应

    :param curl_cmd:
    :param options:
    :return:
    """
    request = Request.from_curl(curl_cmd)
    return req2resp(request, options)


def req2resp(request: Request, options: dict = None) -> Response:
    """
    请求转响应

    :param request:
    :param options:
    :return:
    """
    dispatcher = contextlib.nullcontext()
    options = options or {}
    plugins: Union[dict, type(...), None] = options.pop("plugins", ...)

    spider = Spider(**options)

    # 不需要任何插件
    if not plugins:
        for plugin in spider.plugins:
            plugin.unregister()

    # 使用默认插件
    elif plugins is ...:
        pass

    else:
        for plugin in spider.plugins:
            plugin.unregister()
        for form, plugin in plugins:
            spider.use(form, *pandora.iterable(plugin))

    if inspect.iscoroutinefunction(spider.downloader.fetch):
        dispatcher = spider.dispatcher

    with dispatcher:
        context = spider.make_context(
            request=request,
            next=spider.on_request,
            flows={
                spider.on_request: None,
                spider.on_retry: spider.on_request
            },
        )
        context.failure = lambda shutdown: context.flow({"next": None})
        spider.on_consume(context=context)
        return context.response


def resp2items(
        response: Response,
        engine: Union[str, Callable],
        rules: dict,
        rename: dict = None,
        default: dict = None,
        factory: dict = None,
        show: dict = None,
) -> Items:
    """
    响应转换为 items

    :param response:
    :param engine:
    :param rules:
    :param rename:
    :param default:
    :param factory:
    :param show:
    :return:
    """
    items = response.extract(engine=engine, rules=rules)
    pandora.clean_rows(*items, rename=rename, default=default, factory=factory, show=show)
    return Items(items)


def source2items(
        obj: Union[str, bytes, dict, list],
        engine: Union[str, Callable],
        rules: dict,
        rename: dict = None,
        default: dict = None,
        factory: dict = None,
        show: dict = None,
):
    def ensure_bytes(_obj):
        if isinstance(_obj, str):
            return _obj.encode("utf-8")
        return _obj

    if isinstance(obj, (dict, list)):
        obj = json.dumps(obj)
        encoding = "utf-8"
    else:
        encoding = None
    res = Response(content=ensure_bytes(obj), encoding=encoding)

    return resp2items(
        response=res,
        engine=engine,
        rules=rules,
        rename=rename,
        default=default,
        factory=factory,
        show=show
    )


if __name__ == '__main__':
    cmd = """curl 'https://www.baidu.com/home/weather/getweather?citycode=3873&bsToken=65d1dc2bea5bf3da7bcc670692baa1e4&indextype=manht&_req_seqid=0xa39c109600111838&asyn=1&t=1702368225746&sid=39712_39790_39679_39817_39837_39842_39904_39909_39934_39936_39933_39944_39940_39939_39930_39783' \
  -H 'Accept: text/plain, */*; q=0.01' \
  -H 'Accept-Language: zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6,pt;q=0.5' \
  -H 'Cache-Control: no-cache' \
  -H 'Connection: keep-alive' \
  -H 'Cookie: BDUSS=XNGMGlIU25XM20tRjlpZ3dRdmo4Q0Z3UGktbTNBLTYzdWVJU0JuMS1vTWZILXhrSVFBQUFBJCQAAAAAAAAAAAEAAAChYIhqS0tLS0tLS0VNAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAB-SxGQfksRkY; BDUSS_BFESS=XNGMGlIU25XM20tRjlpZ3dRdmo4Q0Z3UGktbTNBLTYzdWVJU0JuMS1vTWZILXhrSVFBQUFBJCQAAAAAAAAAAAEAAAChYIhqS0tLS0tLS0VNAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAB-SxGQfksRkY; BAIDUID=7937ED60D9778A0F931CC9525E285E36:FG=1; BAIDUID_BFESS=7937ED60D9778A0F931CC9525E285E36:FG=1; H_WISE_SIDS=114550_213353_214804_110085_244718_265881_264353_269231_271172_271254_234296_234207_260335_273136_273239_273382_272262_263619_274158_275017_276125_276196_276572_271563_277031_276923_253022_275870_270102_277355_251972_277631_277643_277620_275733_259642_278054_278164_278166_278574_278620_278636_278792_274779_278841_277542_278388_256739_278921_279021_279040_277522_278236_279267_276574_279366_279384_279085_279087_279610_279605_279741_277757_274947_279856_279307_276311_279011_279711_279703_279920_276694_276690_279974_279998_278249_278213_280121_280210_277699_280150_280226_280312_275085_280486_270366_278415_276929_275856_256223_280651_280562_280768_280809_279847_280105_280585_280782_280558_278791_278954_280636_280560_281036_280923_274283_280270_280587_281191_281218_277969_281148_281233_281029_281367_279490_281379_279203_280488_281387_280375_281519_280862; H_WISE_SIDS_BFESS=114550_213353_214804_110085_244718_265881_264353_269231_271172_271254_234296_234207_260335_273136_273239_273382_272262_263619_274158_275017_276125_276196_276572_271563_277031_276923_253022_275870_270102_277355_251972_277631_277643_277620_275733_259642_278054_278164_278166_278574_278620_278636_278792_274779_278841_277542_278388_256739_278921_279021_279040_277522_278236_279267_276574_279366_279384_279085_279087_279610_279605_279741_277757_274947_279856_279307_276311_279011_279711_279703_279920_276694_276690_279974_279998_278249_278213_280121_280210_277699_280150_280226_280312_275085_280486_270366_278415_276929_275856_256223_280651_280562_280768_280809_279847_280105_280585_280782_280558_278791_278954_280636_280560_281036_280923_274283_280270_280587_281191_281218_277969_281148_281233_281029_281367_279490_281379_279203_280488_281387_280375_281519_280862; MSA_WH=1167_944; BAIDU_WISE_UID=wapp_1698988063436_882; BIDUPSID=7937ED60D9778A0F931CC9525E285E36; PSTM=1700243119; BD_UPN=123253; ispeed_lsm=2; baikeVisitId=ff6df9f9-7dd5-4b01-8eb2-238b514b5395; COOKIE_SESSION=12466669_0_7_2_5_13_1_2_7_4_0_1_135199_0_0_0_1681709415_0_1700907583%7C9%230_0_1700907583%7C1; sugstore=1; ZFY=6pJAETLn6vA0dC1zJqjgeXPvJa:Ab:Ah4UQ8Y3DhRwUwo:C; RT="z=1&dm=baidu.com&si=8509399d-11c6-4a37-9fb3-5f4c2b47196b&ss=lpuwz84d&sl=2&tt=3sf&bcn=https%3A%2F%2Ffclog.baidu.com%2Flog%2Fweirwood%3Ftype%3Dperf&ld=4xk&nu=7qgtmgrv&cl=7qx&ul=gp1&hd=gqa"; MCITY=-158%3A; H_PS_PSSID=39712_39790_39679_39817_39837_39842_39904_39909_39934_39936_39933_39944_39940_39939_39930_39783; BA_HECTOR=8k8l858l058l81a0ala425831ing4uv1q' \
  -H 'DNT: 1' \
  -H 'Pragma: no-cache' \
  -H 'Ps-Dataurlconfigqid: 0xa39c109600111838' \
  -H 'Referer: https://www.baidu.com/' \
  -H 'Sec-Fetch-Dest: empty' \
  -H 'Sec-Fetch-Mode: cors' \
  -H 'Sec-Fetch-Site: same-origin' \
  -H 'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36 Edg/118.0.2088.57' \
  -H 'X-Requested-With: XMLHttpRequest' \
  -H 'sec-ch-ua: "Chromium";v="118", "Microsoft Edge";v="118", "Not=A?Brand";v="99"' \
  -H 'sec-ch-ua-mobile: ?0' \
  -H 'sec-ch-ua-platform: "macOS"' \
  --compressed"""
    resp = curl2resp(cmd, options={"proxy": {"ref": "bricks.lib.proxies.CustomProxy", "key": "127.0.0.1:7890"}})
    print(resp.text)
