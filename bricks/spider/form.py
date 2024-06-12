# -*- coding: utf-8 -*-
# @Time    : 2023-11-18 10:47
# @Author  : Kem
# @Desc    :
import copy
import inspect
from dataclasses import dataclass
from typing import Optional, Union, List, Dict, Callable

from loguru import logger

from bricks import Request, Response, state
from bricks.core import signals, events as _events
from bricks.lib.headers import Header
from bricks.lib.items import Items
from bricks.lib.nodes import RenderNode
from bricks.lib.queues import Item
from bricks.spider import air
from bricks.utils import pandora


class Context(air.Context):
    target: "Spider"

    def __init__(self, target: "Spider", form: str = state.const.ON_CONSUME, **kwargs) -> None:
        super().__init__(target, form, **kwargs)

    def retry(self):
        super().retry()
        # $bookmark -> 指向 当前下载节点的位置
        bookmark = self.seeds.get("$bookmark", 0)
        self.seeds["$signpost"] = bookmark

    def submit(self, *obj: Union[Item, dict], call_later=False, attrs: dict = None) -> List["Context"]:
        signpost = self.seeds.get('$bookmark', 0)
        attrs = attrs or {}
        if not call_later:
            attrs.setdefault('next', self.target.on_flow)
        return super().submit(*[{**o, "$signpost": signpost} for o in obj], call_later=call_later, attrs=attrs)

    def get_node(self, signpost=None):
        if signpost is None:
            cursor = max(0, self.signpost - 1)
        else:
            cursor = signpost

        return self.target.config.spider[cursor]

    def archive(self, signpost: int, **kwargs):
        return self.replace({**self.seeds, "$signpost": signpost, **kwargs})

    @property
    def signpost(self):
        return self.seeds.setdefault("$signpost", 0)


@dataclass
class Layout(RenderNode):
    rename: dict = None
    show: dict = None
    factory: dict = None
    default: dict = None


class Task(_events.Task, RenderNode):
    archive: bool = False


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
    # 判断成功动态脚本, 字符串形式 / 字典形式 /
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
    archive: bool = False

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
    archive: bool = False


@dataclass
class Pipeline(RenderNode):
    func: Union[str, Callable] = ...
    args: Optional[list] = None
    kwargs: Optional[dict] = None
    success: bool = False
    layout: Optional[Layout] = None
    archive: bool = False


@dataclass
class Init(RenderNode):
    func: Union[str, Callable] = ...
    args: Optional[list] = None
    kwargs: Optional[dict] = None
    layout: Optional[Layout] = None


@dataclass
class Config:
    spider: List[Union[Download, Parse, Task, Pipeline]] = None
    init: Optional[List[Init]] = None
    events: Optional[Dict[str, List[Task]]] = None


class Spider(air.Spider):
    Context = Context

    @property
    def flows(self):
        return {
            self.on_consume: self.on_flow,
            # run spider 拿到种子后 -> self.on_seeds, 此时 signpost 都是 0
            # 那么直接交给 on_flow 进行分配
            self.on_seeds: self.on_flow,
            self.make_request: self.on_request,
            self.on_request: self.on_flow,
            self.on_retry: self.on_flow,
            self.on_response: self.on_flow,
            self.on_pipeline: self.on_flow,
        }

    @property
    def config(self) -> Config:
        raise NotImplementedError

    def on_flow(self, context: Context):
        if not self.config.spider:
            logger.warning('没有配置 Spider 节点流程..')
            raise signals.Exit

        while True:
            signpost: int = context.signpost
            context.seeds.setdefault('$bookmark', 0)

            try:
                node: Union[Download, Task, Parse, Pipeline] = context.get_node(signpost)
            except IndexError:
                context.flow({"next": None})

                raise signals.Switch()
            else:
                context.seeds['$signpost'] += 1

                # 种子 -> Request
                if isinstance(node, Download):
                    if context.next.prev and context.next.prev.root == self.on_retry:
                        # 从重试请求那边过来的
                        context.flow({"next": self.on_request})
                    else:
                        # 将种子 -> request -> 发送请求
                        context.seeds['$bookmark'] = signpost
                        node.archive and context.archive(signpost)
                        context.flow({"next": self.make_request})
                    raise signals.Switch()

                # Request -> Response
                elif isinstance(node, Parse):
                    node.archive and context.archive(signpost)
                    context.flow({"next": self.on_response})
                    raise signals.Switch()

                elif isinstance(node, Pipeline):
                    node.archive and context.archive(signpost)
                    context.flow({"next": self.on_pipeline})
                    raise signals.Switch()

                elif isinstance(node, Task):
                    node.archive and context.archive(signpost)
                    pandora.invoke(
                        func=node.func,
                        args=node.args,
                        kwargs=node.kwargs,
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

                else:
                    raise TypeError(f"Unknown node type: {type(node)}")

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
        node: Download = context.get_node()
        s = node.render(context.seeds)
        request = s.to_request()
        context.flow({"request": request})
        return request

    def parse(self, context: Context) -> Union[List[dict], Items]:
        node: Parse = context.get_node()
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
        return Items(items or [])

    def item_pipeline(self, context: Context):
        node: Pipeline = context.get_node()
        engine = node.func
        args = node.args or []
        kwargs = node.kwargs or {}
        layout = node.layout or Layout()
        layout = layout.render(context.seeds)

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

        for form, events in (self.config.events or {}).items():
            self.use(form, *events)
