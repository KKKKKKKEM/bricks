# -*- coding: utf-8 -*-
# @Time    : 2023-11-18 10:47
# @Author  : Kem
# @Desc    :
import copy
import inspect
import re
from dataclasses import dataclass, fields, is_dataclass
from typing import Optional, Union, List, Dict, Callable, Any

from loguru import logger

from bricks import Request, Response, const
from bricks.core import signals, events as _events
from bricks.downloader import cffi
from bricks.downloader import genesis
from bricks.lib.headers import Header
from bricks.lib.items import Items
from bricks.lib.queues import TaskQueue, Item
from bricks.spider import air
from bricks.utils import pandora

FORMAT_REGEX = re.compile(r'{(\w+)(?::(\w+))?}')


@dataclass
class Post:
    value: Any = None
    prev: "Post" = None


@dataclass
class SignPost:
    """
    流程游标

    """

    cursor: Union[Post, int] = Post(0)
    download: Union[Post, int] = Post(0)
    parse: Union[Post, int] = Post(0)
    pipeline: Union[Post, int] = Post(0)
    action: Union[Post, str] = Post("")

    def __setattr__(self, key, value):
        if not isinstance(value, Post):
            value = Post(value)
            value.prev = getattr(self, key, Post())

        return super().__setattr__(key, value)

    def rollback(self, names: list = None):

        names = names or [field.name for field in fields(self)]

        for name in names:
            v = getattr(self, name, None)
            if v is None:
                continue
            else:
                setattr(self, name, v.prev)


class Context(air.Context):
    target: "Spider"

    def __init__(self, target: "Spider", form: str = const.ON_CONSUME, **kwargs) -> None:
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


@dataclass
class Node:

    @classmethod
    def format(cls, value, base: dict, errors: str = "raise"):
        if isinstance(value, str):
            while True:
                try:
                    return value.format(**base)
                except ValueError:

                    placeholders = FORMAT_REGEX.findall(value)
                    # 有多个, 那最终肯定还是字符串
                    convert_value = len(placeholders) == 1
                    for placeholder, type_str in placeholders:

                        if placeholder not in base:
                            if errors == 'raise':
                                raise ValueError(f"Missing key in base: {placeholder}")
                            elif errors == 'ignore':
                                return value
                            else:
                                base.setdefault(placeholder, "")

                        placeholder_value = base[placeholder]
                        if type_str:
                            placeholder_value = cls.convert(placeholder_value, type_str)
                            value = value.replace(f"{{{placeholder}:{type_str}}}", str(placeholder_value))
                        else:
                            value = value.replace(f"{{{placeholder}}}", str(placeholder_value))

                        if convert_value:
                            value = cls.convert(value, type(placeholder_value))

                    return value

                except KeyError as e:
                    if errors == "raise":
                        raise ValueError(f"Missing key in base: {e}")

                    elif errors == 'ignore':
                        return value

                    else:
                        base.setdefault(e.args[0], "")

        elif isinstance(value, list):
            return [cls.format(item, base, errors=errors) for item in value]
        elif isinstance(value, dict):
            return {k: cls.format(v, base, errors=errors) for k, v in value.items()}
        elif is_dataclass(value):
            return value.render(base)
        return value

    @staticmethod
    def convert(value, type_str):
        maps = {
            "int": {
                "action": int,
                "default": 0
            },
            int: {
                "action": int,
                "default": 0
            },
            "str": {
                "action": str,
                "default": ""
            },
            str: {
                "action": str,
                "default": ""
            },
            "float": {
                "action": str,
                "default": 0.0
            },
            float: {
                "action": str,
                "default": 0.0
            },
            "json": {
                "action": pandora.json_or_eval,
                "default": None
            },
            list: {
                "action": pandora.json_or_eval,
                "default": None
            },
            dict: {
                "action": pandora.json_or_eval,
                "default": None
            },
        }
        if type_str in maps:
            try:
                return maps[type_str]['action'](value)
            except ValueError:
                return maps[type_str]['default']
        else:
            return value

    def render(self, context: Context):
        base = context.seeds
        # 创建一个新的实例，避免修改原始实例
        node = copy.deepcopy(self)
        for field in fields(self):
            value = getattr(node, field.name)
            new_value = self.format(value, base, errors=getattr(self, "strict", "fix"))
            setattr(node, field.name, new_value)
        return node


class Task(_events.Task, Node):
    ...


@dataclass
class Download(Node):
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

    def to_response(self, downloader: genesis.Downloader = None) -> Response:
        request = self.to_request()
        downloader = downloader or cffi.Downloader()
        return downloader.fetch(request)


@dataclass
class Parse(Node):
    func: Union[str, Callable]
    args: Optional[list] = None
    kwargs: Optional[dict] = None
    strict: str = "fix"


@dataclass
class Pipeline(Node):
    func: Union[str, Callable]
    args: Optional[list] = None
    kwargs: Optional[dict] = None
    strict: str = "fix"
    success: bool = False


@dataclass
class Init(Node):
    func: Union[str, Callable]
    args: Optional[list] = None
    kwargs: Optional[dict] = None
    strict: str = "fix"


@dataclass
class Config:
    spider: List[Union[Download, Parse, Task, Pipeline]] = None
    init: Optional[List[Init]] = None
    events: Optional[Dict[str, List[Task]]] = None


class Spider(air.Spider):
    Context = Context

    def __init__(self, concurrency: Optional[int] = 1, survey: Optional[Union[dict, List[dict]]] = None,
                 downloader: Optional[Union[str, genesis.Downloader]] = None, task_queue: Optional[TaskQueue] = None,
                 queue_name: Optional[str] = "", proxy: Optional[dict] = None, forever: Optional[bool] = False,
                 **kwargs) -> None:
        super().__init__(concurrency, survey, downloader, task_queue, queue_name, proxy, forever, **kwargs)

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
            engine = node.func
            args = node.args or []
            kwargs = node.kwargs or {}
            # todo: 暂时没有内置引擎, 后面需要加几个常用的
            if str(engine).lower() in []:
                pass

            else:
                if not callable(engine):
                    engine = pandora.load_objects(engine)

                seeds = pandora.invoke(
                    func=engine,
                    args=[context, *args],
                    kwargs=kwargs,
                    annotations={Context: context},
                    namespace={"context": context}
                )

                if inspect.isgenerator(seeds):
                    for seed in seeds:
                        yield seed
                else:
                    yield seeds or []

    def make_request(self, context: Context) -> Request:
        node: Download = context.obtain("download")
        s = node.render(context)
        request = s.to_request()
        context.flow({"request": request})
        return request

    def parse(self, context: Context):
        node: Parse = context.obtain("parse")
        engine = node.func
        args = node.args or []
        kwargs = node.kwargs or {}

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
                yield item
        else:
            yield items or []

    def item_pipeline(self, context: Context):
        node: Pipeline = context.obtain("pipeline")
        engine = node.func
        args = node.args or []
        kwargs = node.kwargs or {}

        # todo: 暂时没有内置引擎, 后面需要加几个常用的
        if str(engine).lower() in []:
            pass
        else:
            if not callable(engine):
                engine = pandora.load_objects(engine)

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

        node.success and context.success()

    def before_start(self):
        super().before_start()
        for form, events in (self.config.events or {}).items():
            self.use(form, *events)


if __name__ == '__main__':
    s = SignPost(1)
    s.cursor = 2
    print(s.cursor, s.download)
    s.rollback()
    print(s.cursor, s.download)
