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
from bricks.utils import pandora, convert


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
    url: str = ...
    params: Optional[dict] = None
    method: str = 'GET'
    body: Union[str, dict] = None
    headers: Union[Header, dict] = None
    cookies: Dict[str, str] = None
    options: dict = None
    timeout: int = ...
    allow_redirects: bool = True
    proxies: Optional[str] = None
    proxy: Optional[dict] = None
    is_success: Optional[str] = ...
    retry: int = 0
    max_retry: int = 5
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
            is_success=self.is_success,
            retry=self.retry,
            max_retry=self.max_retry,
            use_session=self.use_session
        )

    def to_response(self, options: dict = None) -> Response:
        return convert.req2resp(self.to_request(), options)


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

                raise signals.Switch
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
                    raise signals.Switch

                # Request -> Response
                elif isinstance(node, Parse):
                    node.archive and context.archive(signpost)
                    context.flow({"next": self.on_response})
                    raise signals.Switch

                elif isinstance(node, Pipeline):
                    node.archive and context.archive(signpost)
                    context.flow({"next": self.on_pipeline})
                    raise signals.Switch

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
