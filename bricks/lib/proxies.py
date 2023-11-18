# -*- coding: utf-8 -*-
# @Time    : 2023-11-17 22:20
# @Author  : Kem
# @Desc    :
import contextlib
import itertools
import json
import math
import queue
import re
import threading
import time
import urllib.parse
from typing import Optional, Callable

from loguru import logger

from bricks.db.redis_ import Redis
from bricks.downloader import cffi
from bricks.utils import pandora

IP_MATCH_RULE = re.compile(r'(http://)?\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}:\d+/?')
IP_EXTRACT_RULE = re.compile(r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}:\d+')
URL_MATCH_RULE = re.compile(
    r'^(?:http|ftp)s?://'  # http:// or https://
    r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|'  # domain...
    r'localhost|'  # localhost...
    r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
    r'(?::\d+)?'  # optional port
    r'(?:/?|[/?]\S+)$', re.IGNORECASE
)


class MetaClass(type):
    def __new__(cls, name, bases, dct):  # noqa
        def wrapper(raw_method):
            def inner(self, *args, **kwargs):
                proxy = raw_method(self, *args, **kwargs)
                proxy.proxy = self.strict(proxy=proxy.proxy)
                proxy.auth = self.auth
                proxy.recover = self.recover
                proxy.threshold = self.threshold
                proxy.derive = self
                return proxy

            return inner

        # 在这里可以修改类的定义
        dct['get'] = wrapper(dct['get'])
        # 创建并返回新的类
        return super(MetaClass, cls).__new__(cls, name, bases, dct)


class Proxy:
    def __init__(
            self,
            proxy: Optional[str] = None,
            auth: Optional[Callable] = None,
            recover: Optional[Callable] = ...,
            threshold: int = math.inf,
            derive: "BaseProxy" = None,
            rkey: str = None
    ):
        """
        代理类

        :param proxy: 代理 value
        :param auth: 认证信息
        :param recover: 回收函数
        :param threshold: 使用阈值
        """
        self.threshold = threshold
        self.counter = itertools.count()
        self.proxy = proxy
        self.auth = auth
        self.recover = recover
        self.derive = derive
        self.rkey = rkey

    def use(self):
        """
        时候之后调用

        :return:
        """
        if self.threshold == math.inf:
            return True

        value = next(self.counter)
        if value >= self.threshold:
            callable(self.recover) and self.recover(self)
            return False
        else:
            return True

    def __bool__(self):
        return bool(self.proxy)

    def __str__(self):
        return self.proxy


class BaseProxy(metaclass=MetaClass):

    def __init__(
            self,
            scheme: str = "http",
            username: str = None,
            password: str = None,
            auth: Optional[Callable] = None,
            recover: Optional[Callable] = ...,
            threshold: int = math.inf
    ):
        self.scheme = scheme
        self.username = username
        self.password = password
        self.auth = auth
        self.threshold = threshold
        self.recover = recover

    def get(self, timeout=None) -> Proxy:
        raise NotImplementedError

    def fmt(self, proxy: str) -> str:
        if not proxy:
            return ""

        parsed = urllib.parse.urlparse(proxy)
        if self.username and self.password:
            prefix = f'{self.username}:{self.password}@'
        else:
            prefix = ""

        # proxy:port
        if parsed.path and not parsed.netloc and not parsed.scheme:
            proxy = f"{self.scheme}://{prefix}{proxy}"
        # //proxy:port
        elif parsed.netloc and not parsed.scheme:
            proxy = f'{self.scheme}://{prefix}{proxy[2:]}'
        # scheme://proxy:port
        elif parsed.scheme and parsed.netloc:
            proxy = f'{self.scheme}://{prefix}{parsed.netloc}'

        # scheme://username:password@proxy:port
        elif parsed.username and parsed.password:
            proxy = f'{self.scheme}://{parsed.username}:{parsed.password}@{parsed.hostname}"{parsed.port}'

        return proxy


class ApiProxy(BaseProxy):
    def __init__(
            self,
            key,
            scheme: str = "http",
            username: Optional[str] = None,
            password: Optional[str] = None,
            auth: Optional[Callable] = None,
            threshold: int = math.inf,
            options: Optional[dict] = None,
            handle_response: Optional[Callable] = None,
            recover: Optional[Callable] = ...
    ):
        """
        直接从 API 获取代理的代理类型

        :param key: 请求 api
        :param scheme: 协议
        :param username: 账号
        :param password: 密码
        :param auth: 其他认证回调
        :param threshold: 代理使用阈值, 到达阈值会回收这个代理
        :param options: 其他请求参数
        :param handle_response: 处理响应的回调, 默认使用匹配
        :param recover: 处理响应的回调, 默认使用匹配
        """
        self.key = key
        self.options = options
        self.downloader = cffi.Downloader()
        self.handle_response = handle_response or (lambda res: IP_EXTRACT_RULE.findall(res.text))
        self.container = queue.Queue()
        self.lock = threading.Lock()
        super().__init__(
            scheme=scheme,
            username=username,
            password=password,
            auth=auth,
            threshold=threshold,
            recover=(lambda proxy: self.container.put(proxy.proxy)) if recover is ... else recover
        )

    def get(self, timeout=None) -> Proxy:
        # 这个要加锁, 不然多线程会都去提取代理
        with self.lock:
            try:
                proxy = self.container.get(timeout=1)
            except queue.Empty:
                self.fetch(timeout)
            else:
                return Proxy(proxy)

    def fetch(self, timeout=None):
        if timeout is None:
            timeout = math.inf

        options = self.options
        options.setdefault("method", "GET")
        start = time.time()
        while True:
            res = self.downloader.fetch({"url": self.key, **options})
            if not res:
                logger.warning(f"[获取代理失败]  ref: {self}")
                if time.time() - start > timeout: raise TimeoutError
                time.sleep(1)

            else:
                proxies = self.handle_response(res)
                for proxy in proxies: self.container.put(proxy)
                return

    def __str__(self):
        return f'<ApiProxy key={self.key}| options={self.options}>'


class RedisProxy(BaseProxy):

    def __init__(
            self,
            key: str,
            options: dict = None,
            scheme: str = "http",
            username: str = None,
            password: str = None,
            auth: Optional[Callable] = None,
            threshold: int = math.inf,
            recover: Optional[Callable] = ...
    ):
        """
        从 redis 的 key 里面提取代理

        :param key: redis key name
        :param options: 实例化 redis 的其他参数
        :param scheme: 协议
        :param username: 用户名
        :param password: 密码
        :param auth: 鉴权回调
        :param threshold: 代理使用阈值, 到达阈值会回收
        """
        self.options = options or {}
        self.key = key
        self.container = Redis(**self.options)
        super().__init__(
            scheme=scheme,
            username=username,
            password=password,
            auth=auth,
            threshold=threshold,
            recover=(lambda proxy: self.container.add(self.key, proxy.proxy)) if recover is ... else recover
        )

    def get(self, timeout=None) -> Proxy:
        if timeout is None:
            timeout = math.inf

        start = time.time()
        while True:
            proxy = self.container.pop(self.key)
            if not proxy:
                if time.time() - start > timeout: raise TimeoutError
                logger.warning(f'[获取代理失败] ref: {self}')
                time.sleep(1)
            else:
                return Proxy(proxy)

    def __str__(self):
        return f'<RedisProxy [key: {self.key} | options:{self.options}]>'


class CustomProxy(BaseProxy):

    def __init__(
            self,
            key: str,
            scheme: str = "http",
            username: str = None,
            password: str = None,
            auth: Optional[Callable] = None,
            threshold: int = math.inf,
            recover: Optional[Callable] = None
    ):
        self.key = key
        super().__init__(
            scheme=scheme,
            username=username,
            password=password,
            auth=auth,
            threshold=threshold,
            recover=recover
        )

    def get(self, timeout=None) -> Proxy:
        return Proxy(self.key)


class Manager:

    def __init__(self):
        self._local = threading.local()
        self._context = contextlib.nullcontext()
        self.container = {}

    def get_proxy(self, *configs: dict, timeout: int = None) -> Proxy:
        """

        获取代理

        :param configs: 获取代理的配置 -> {"ref": "指向代理类", ... 这些其他的都是实例化类的参数}
        :param timeout: 获取代理的超时时间, timeout 为 None 代表一直等待, 超时会直接使用空代理
        :return:
        """
        with self._context:
            for config in configs:
                config["ref"] = pandora.load_objects(config["ref"])
                rkey = self.get_rkey(config)

                if not hasattr(self._local, rkey):
                    if rkey not in self.container:
                        self.container[rkey] = pandora.invoke(config["ref"], kwargs=config)

                    # 获取代理模型
                    ins: BaseProxy = self.container[rkey]
                    try:
                        proxy = ins.get(timeout=timeout)
                    except TimeoutError:
                        proxy = Proxy()

                    proxy and setattr(self._local, rkey, proxy)

                temp = getattr(self._local, rkey, Proxy())
                temp.rkey = rkey
                if temp:
                    return temp
            else:
                return Proxy()

    def clear_proxy(self, *configs: dict):
        """
        清除代理

        :param configs:
        :return:
        """
        with self._context:
            for config in configs:
                rkey = self.get_rkey(config)
                if hasattr(self._local, rkey):
                    delattr(self._local, rkey)

    def current_proxy(self, *configs: dict) -> Proxy:
        """
        获取当前代理

        :param configs:
        :return:
        """
        with self._context:
            for config in configs:
                rkey = self.get_rkey(config)
                if hasattr(self._local, rkey):
                    return getattr(self._local, rkey)
            else:
                return Proxy()

    def recover_proxy(self, *configs: dict):
        """
        回收代理

        :param configs:
        :return:
        """
        with self._context:
            for config in configs:
                rkey = self.get_rkey(config)
                if hasattr(self._local, rkey):
                    proxy: Proxy = getattr(self._local, rkey)
                    callable(proxy.recover) and proxy.recover(proxy.proxy)
                    delattr(self._local, rkey)

    def use_proxy(self, proxy: Proxy):
        state = proxy.use()
        if state is False and hasattr(self._local, proxy.rkey):
            delattr(self._local, proxy.rkey)

    def fresh_proxy(self, *configs: dict) -> Proxy:
        """
        刷新代理

        :param configs:
        :return:
        """
        with self._context:
            self.clear_proxy(*configs)
            return self.get_proxy(*configs)

    @staticmethod
    def get_rkey(config: dict):
        return str(hash(json.dumps(config, default=str)))

    def set_mode(self, mode=0):
        # 线程隔离
        if mode == 0:
            self._local = threading.local()
            self._context = contextlib.nullcontext()

        # 线程共享
        else:
            self._local = object()
            self._context = threading.Lock()


manager = Manager()

if __name__ == '__main__':
    p = CustomProxy("127.0.0.1:7890")
    print(p.get())
    print(pandora.prepare(CustomProxy))
