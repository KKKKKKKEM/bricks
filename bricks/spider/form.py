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
from bricks.lib.nodes import RenderNode, SignPost, Post
from bricks.lib.queues import Item
from bricks.spider import air
from bricks.utils import pandora, convert


class Context(air.Context):
    target: "Spider"

    def __init__(self, target: "Spider", form: str = state.const.ON_CONSUME, **kwargs) -> None:
        super().__init__(target, form, **kwargs)
        self.signpost: SignPost = kwargs.get("signpost") or SignPost()

    def retry(self):
        super().retry()
        self.signpost.action = "retry"

    def submit(self, obj: Union[Request, Item, dict], call_later=False, signpost: SignPost = None) -> "Context":
        assert obj.__class__ in [Request, Item, dict], f"不支持的类型: {obj.__class__}"
        if obj.__class__ in [Item, dict]:
            signpost = SignPost(cursor=Post(self.signpost.download.value)) if signpost is None else signpost

            if call_later:
                self.task_queue.put(self.queue_name, obj)
                return self
            else:
                self.task_queue.put(self.queue_name, obj, qtypes="temp")
                return self.branch({
                    "seeds": obj,
                    "signpost": signpost
                })
        else:
            signpost = signpost or SignPost()
            return self.branch({"request": obj, "next": self.target.on_request, "signpost": signpost})


class Task(_events.Task, RenderNode):
    ...


@dataclass
class Layout(RenderNode):
    rename: dict = None
    show: dict = None
    factory: dict = None
    default: dict = None
    strict: str = "fix"


@dataclass
class Download(RenderNode):
    url: str
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
    status_codes: Optional[dict] = ...
    retry: int = 0
    max_retry: int = 5
    strict: str = "fix"

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
            status_codes=self.status_codes,
            retry=self.retry,
            max_retry=self.max_retry
        )

    def to_response(self, spider: "Spider" = None) -> Response:
        return convert.req2resp(self.to_request(), spider)


@dataclass
class Parse(RenderNode):
    func: Union[str, Callable]
    args: Optional[list] = None
    kwargs: Optional[dict] = None
    strict: str = "fix"
    layout: Optional[Layout] = None


@dataclass
class Pipeline(RenderNode):
    func: Union[str, Callable]
    args: Optional[list] = None
    kwargs: Optional[dict] = None
    strict: str = "fix"
    success: bool = False
    layout: Optional[Layout] = None


@dataclass
class Init(RenderNode):
    func: Union[str, Callable]
    args: Optional[list] = None
    kwargs: Optional[dict] = None
    strict: str = "fix"
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

        # 这是重试回来了
        if context.signpost.action.value == "retry":
            # 找到下载节点前面不是 Task 的节点
            for i in range(context.signpost.download.value - 1, -1, -1):
                node = self.config.spider[i]
                if not isinstance(node, Task):
                    context.signpost.cursor = i + 1
                    break
            else:
                context.signpost.cursor = context.signpost.download.value

            context.signpost.action = ""

        while True:
            try:
                node: Union[Download, Task, Parse, Pipeline] = self.config.spider[context.signpost.cursor.value]
            except IndexError:
                context.flow({"next": None})
                raise signals.Switch
            else:
                context.signpost.cursor = context.signpost.cursor.value + 1

                # 种子 -> Request
                if isinstance(node, Download):
                    # 记录下载节点的位置
                    context.signpost.download = context.signpost.cursor.value - 1
                    context.download = node
                    context.flow({"next": self.make_request})
                    raise signals.Switch

                # Request -> Response
                elif isinstance(node, Parse):
                    context.signpost.parse = context.signpost.cursor.value - 1
                    context.parse = node
                    context.flow({"next": self.on_response})
                    raise signals.Switch

                elif isinstance(node, Pipeline):
                    context.signpost.pipeline = context.signpost.cursor.value - 1
                    context.pipeline = node
                    context.flow({"next": self.on_pipeline})
                    raise signals.Switch

                elif isinstance(node, Task):
                    context.task = node
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
        node: Download = context.obtain("download")
        s = node.render(context.seeds)
        request = s.to_request()
        context.flow({"request": request})
        return request

    def parse(self, context: Context):
        node: Parse = context.obtain("parse")
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

        if inspect.isgenerator(items):
            for item in items:
                pandora.clean_rows(
                    *pandora.iterable(item),
                    rename=layout.rename,
                    default=layout.default,
                    factory=layout.factory,
                    show=layout.show,
                )
                yield item
        else:
            pandora.clean_rows(
                *pandora.iterable(items),
                rename=layout.rename,
                default=layout.default,
                factory=layout.factory,
                show=layout.show,
            )
            yield items or []

    def item_pipeline(self, context: Context):
        node: Pipeline = context.obtain("pipeline")
        engine = node.func
        args = node.args or []
        kwargs = node.kwargs or {}
        layout = node.layout or Layout()
        layout = layout.render(context.seeds)

        if not callable(engine):
            engine = pandora.load_objects(engine)
        back = context.items
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
            context.items = back

        node.success and context.success()

    def install(self):
        super().install()

        for form, events in (self.config.events or {}).items():
            self.use(form, *events)


if __name__ == '__main__':
    down = Download(url="http://www.baidu.com")
    resp = down.to_response()
    print(resp.text)
