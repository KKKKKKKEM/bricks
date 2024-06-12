# -*- coding: utf-8 -*-
# @Time    : 2023-12-05 20:18
# @Author  : Kem
# @Desc    :
import copy
import inspect
from dataclasses import dataclass, field
from typing import Optional, Union, List, Dict, Callable

from bricks import Request, Response
from bricks.core import events as _events, signals
from bricks.lib.headers import Header
from bricks.lib.items import Items
from bricks.lib.nodes import RenderNode
from bricks.lib.queues import Item
from bricks.spider import air, form
from bricks.utils import pandora

Init = form.Init
Layout = form.Layout


class Context(air.Context):
    def next_step(self, *objs, call_later=False, signpost: int = ...):
        """
        切换配置

        :param objs: 要切换配置的对象, 可以理解为种子, 默认为当前 seeds
        :param call_later: 是否稍后调用
        :param signpost: 需要切换的 config id, 默认为 当前的 +1
        :return:
        """
        if signpost is ...:
            signpost = self.signpost + 1

        if not objs:
            new = [{**self.seeds, "$config": signpost}]

        else:
            new = [{**self.seeds, **obj, "$config": signpost} for obj in objs]

        return self.submit(*new, call_later=call_later)

    @property
    def signpost(self):
        return self.seeds.setdefault("$config", 0)


class Task(_events.Task, RenderNode):
    ...


@dataclass
class Download(RenderNode):
    # 请求 URL
    url: str = ...
    # 请求 URL 参数
    params: Optional[dict] = None
    # 请求方法, 默认 GET
    method: str = 'GET'
    # body: 请求 Body, 支持字典 / 字符串, 传入为字典的时候, 具体编码方式看 headers 里面的 Content - type
    # 请求的 body, 全部设置为字典时需要和 headers 配合, 规则如下
    # 如果是 json 格式, headers 里面设置 Content-Type 为 application/json
    # 如果是 form urlencoded 格式, headers 里面设置 Content-Type 为 application/x-www-form-urlencoded
    # 如果是 form data 格式, headers 里面设置 Content-Type 为 multipart/form-data
    body: Optional[Union[str, dict]] = None
    # headers: 请求头
    headers: Union[Header, dict] = None
    # cookies: 请求 cookies
    cookies: Dict[str, str] = None
    # options: 请求其他额外选项, 可以用于配合框架 / 下载器
    options: dict = None
    # timeout: 请求超时时间, 填入 ... 为默认
    timeout: int = ...
    # allow_redirects: 是否允许重定向
    allow_redirects: bool = True
    # proxies: 请求代理 Optional[str], 如 http://127.0.0.1:7890, 理解为 current proxy
    proxies: Optional[str] = None
    # proxy: 代理 Key, 理解为 proxy from
    proxy: Optional[dict] = None
    # 判断成功动态脚本, 字符串形式 / 字典形式 / None
    # 字符串形式, 如通过 403 状态码可以写为: 200 <= response.status_code < 400 or response.status_code == 403
    # 字典形式, 如 {"response.status_code == 404": signals.Success}, 标识状态码为 404 的时候直接删除种子
    # 字典形式, 如 {"response.status_code == 403": signals.Pass}, 标识状态码为 403 的时候让其通过默认的拦截器
    # 字典形式, 如 {"response.status_code == 403": Callable Func}, 标识状态码为 403 的时候让Callable Func 来处理后续逻辑
    ok: Optional[Union[str, Dict[str, Union[type(signals.Signal), Callable]]]] = ...
    # 当前重试次数
    retry: int = 0
    # 最大重试次数
    max_retry: int = 5
    # 是否使用下载器的 session 模式
    use_session: bool = False

    def to_request(self) -> Request:
        return Request(
            url=self.url,
            params=self.params,
            method=self.method,
            body=self.body,
            headers=self.headers,
            cookies=self.cookies,
            options=self.options,
            timeout=self.timeout,
            allow_redirects=self.allow_redirects,
            proxies=self.proxies,
            proxy=self.proxy,
            ok=self.ok,
            retry=self.retry,
            max_retry=self.max_retry,
            use_session=self.use_session
        )


@dataclass
class Parse(RenderNode):
    func: Union[str, Callable] = ...
    args: Optional[list] = None
    kwargs: Optional[dict] = None
    layout: Optional[Layout] = None


@dataclass
class Pipeline(RenderNode):
    func: Union[str, Callable] = ...
    args: Optional[list] = None
    kwargs: Optional[dict] = None
    success: bool = False
    layout: Optional[Layout] = None
    match: Optional[Union[Callable, str]] = None


@dataclass
class Config:
    init: Optional[List[Init]] = field(default_factory=lambda: [])
    events: Optional[Dict[str, List[Task]]] = field(default_factory=lambda: {})
    download: List[Download] = field(default_factory=lambda: [])
    parse: List[Parse] = field(default_factory=lambda: [])
    pipeline: List[Pipeline] = field(default_factory=lambda: [])


class Spider(air.Spider):
    Context = Context

    @property
    def config(self) -> Config:
        raise NotImplementedError

    def make_seeds(self, context: Context, **kwargs):
        if not self.config.init:
            return

        for node in pandora.iterable(self.config.init):
            node: Init
            engine = node.func
            node_args = node.args or []
            node_kwargs = node.kwargs or {}
            layout = node.layout or Layout()

            if not callable(engine):
                engine = pandora.load_objects(engine)

            seeds = pandora.invoke(
                func=engine,
                args=node_args,
                kwargs={**kwargs, **node_kwargs},
                annotations={Context: context},
                namespace={"context": context}
            )
            layout = layout.render(seeds)

            if inspect.isgenerator(seeds):
                for seed in seeds:
                    pandora.clean_rows(
                        *pandora.iterable(seed),
                        rename=layout.rename,
                        default=layout.default,
                        factory=layout.factory,
                        show=layout.show,
                    )
                    yield seed
            else:
                pandora.clean_rows(
                    *pandora.iterable(seeds),
                    rename=layout.rename,
                    default=layout.default,
                    factory=layout.factory,
                    show=layout.show,
                )
                yield seeds or []

    def make_request(self, context: Context) -> Request:
        signpost: int = context.signpost
        configs = pandora.iterable(self.config.download)
        node: Download = configs[signpost % len(configs)]
        s = node.render(context.seeds)
        request = s.to_request()
        return request

    def parse(self, context: Context) -> Union[List[dict], Items]:
        signpost: int = context.signpost
        configs = pandora.iterable(self.config.parse)
        node: Parse = configs[signpost % len(configs)]
        engine = node.func
        args = node.args or []
        kwargs = node.kwargs or {}
        layout = node.layout or Layout()
        layout = layout.render(context.seeds)

        if str(engine).lower() in ["json", "xpath", "jsonpath", "regex"]:
            items = pandora.invoke(
                func=context.response.extract,
                args=[engine.lower(), *args],
                kwargs=kwargs,
                annotations={
                    Context: context,
                    Response: context.response,
                    Request: context.request,
                    Item: context.seeds
                },
                namespace={
                    "context": context,
                    "response": context.response,
                    "request": context.request,
                    "seeds": context.seeds
                }
            )
        else:
            if not callable(engine):
                engine = pandora.load_objects(engine)

            items = pandora.invoke(
                func=engine,
                args=args,
                kwargs=kwargs,
                annotations={
                    Context: context,
                    Response: context.response,
                    Request: context.request,
                    Item: context.seeds
                },
                namespace={
                    "context": context,
                    "response": context.response,
                    "request": context.request,
                    "seeds": context.seeds
                }
            )

        pandora.clean_rows(
            *pandora.iterable(items),
            rename=layout.rename,
            default=layout.default,
            factory=layout.factory,
            show=layout.show,
        )
        return items or []

    def item_pipeline(self, context: Context):
        nodes: List[Pipeline] = pandora.iterable(self.config.pipeline)

        for node in nodes:
            engine = node.func
            args = node.args or []
            kwargs = node.kwargs or {}
            layout = node.layout or Layout()
            layout = layout.render(context.seeds)
            if callable(node.match):
                ok = pandora.invoke(
                    func=node.match,
                    annotations={
                        Context: context,
                        Response: context.response,
                        Request: context.request,
                        Item: context.seeds,
                        Items: context.items
                    },
                    namespace={
                        "context": context,
                        "response": context.response,
                        "request": context.request,
                        "seeds": context.seeds,
                        "items": context.items
                    }
                )

            elif isinstance(node.match, str):
                ok = eval(node.match, {
                    "context": context,
                    "response": context.response,
                    "request": context.request,
                    "seeds": context.seeds,
                    "items": context.items
                })

            else:
                ok = True

            if not ok:
                continue

            if not callable(engine):
                engine = pandora.load_objects(engine)

            backup = context.items
            try:
                context.items = pandora.clean_rows(
                    *copy.deepcopy(context.items),
                    rename=layout.rename,
                    default=layout.default,
                    factory=layout.factory,
                    show=layout.show,
                )
                pandora.invoke(
                    func=engine,
                    args=args,
                    kwargs=kwargs,
                    annotations={
                        Context: context,
                        Response: context.response,
                        Request: context.request,
                        Item: context.seeds,
                        Items: context.items
                    },
                    namespace={
                        "context": context,
                        "response": context.response,
                        "request": context.request,
                        "seeds": context.seeds,
                        "items": context.items
                    }
                )
            finally:
                context.items = backup

            node.success and context.success()

    def install(self):
        super().install()

        for _form, events in (self.config.events or {}).items():
            self.use(_form, *events)
