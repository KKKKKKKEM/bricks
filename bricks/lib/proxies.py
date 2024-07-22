# -*- coding: utf-8 -*-
# @Time    : 2023-11-17 22:20
# @Author  : Kem
# @Desc    :
import base64
import contextlib
import itertools
import json
import math
import queue
import re
import threading
import time
import typing
import urllib.parse
from typing import Optional, Callable, Type, Literal

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
DOWNLOADER = cffi.Downloader()


class Proxy:
    def __init__(
            self,
            proxy: Optional[str] = None,
            auth: Optional[Callable] = None,
            recover: Optional[Callable] = ...,
            clear: Optional[Callable] = ...,
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
        self.counter = itertools.count(1)
        self.proxy = proxy
        self.auth = auth
        self.recover = recover
        self.clear = clear
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


@pandora.with_metaclass(singleton=True)
class BaseProxy:

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

    @classmethod
    def build(cls, **options):
        prepared = pandora.prepare(cls.__init__, kwargs=options, ignore=[0])
        return cls(**prepared.kwargs)

    def clear(self, proxy: Proxy):
        pass

    def _when_get(self, raw_method):
        def inner(*args, **kwargs):
            proxy = raw_method(*args, **kwargs)
            if not isinstance(proxy, Proxy):
                proxy = Proxy(proxy, auth=self.auth, recover=self.recover, threshold=self.threshold, derive=self)
            proxy.proxy = self.fmt(proxy=proxy.proxy)
            proxy.auth = self.auth
            proxy.clear = self.clear
            proxy.recover = self.recover
            proxy.threshold = self.threshold
            proxy.derive = self
            return proxy

        return inner


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
            while True:
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
            res = DOWNLOADER.fetch({"url": self.key, **options})
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


class ClashProxy(BaseProxy):
    """
    针对 clash 做的一层封装, 会自动将 clash 切换为 global 模式后, 循环使用 global 内的节点

    """

    def __init__(
            self,
            key: str,
            secret: Optional[str] = None,
            cache_ttl: int = -1,
            selector: str = "GLOBAL",
            match: Optional[Callable] = ...,
            scheme: str = "http",
            auth: Optional[Callable] = None,
            threshold: [int, float, Callable] = ...,
            recover: Optional[Callable] = ...
    ):
        """
        直接从 API 获取代理的代理类型

        :param key: clash 请求 api (配置文件的 external-controller), 如: 127.0.0.1:9090
        :param secret: 访问 clash
        :param cache_ttl: 缓存刷新时间，到了这个时间后会刷新缓存，重新获取配置和节点
        :param selector: 使用的节点组
        :param match: 用于过滤 selector 的函数， 默认是： lambda x: str(x) not in ['DIRECT', 'REJECT']
        :param scheme: 访问控制端的时候需要输入的 Authorization， Bearer 后面这一截
        :param auth: 其他认证回调
        :param threshold: 代理使用阈值, 到达阈值会回收这个代理
        :param recover: 回收, 一般不需要
        """
        key = key.strip()
        if not key.startswith("http" + "://"):
            key = "http" + "://" + key
        if match is ...:
            match = (lambda x: str(x) not in ['DIRECT', 'REJECT'])

        self.key = key
        self.selector = selector
        self.cache_ttl = cache_ttl
        self.ts = time.time()
        self.match = match
        self.secret = secret
        self._nodes = itertools.repeat(None)
        self._configs = None
        self.now = None
        self._proxy: Proxy = Proxy()
        self.lock = threading.Lock()

        super().__init__(
            scheme=scheme,
            auth=auth,
            threshold=threshold,
            recover=recover
        )

        if self.selector.upper() == "GLOBAL":
            self.configs = {"mode": "Global"}

    def nodes(self, name: str = ""):
        """
        查询代理信息

        :param name: 不传入则获取所有可以使用的代理节点名称
        :return:
        """

        if name:
            action = f'/proxies/{name}'
        else:
            action = f'/proxies'

        resp = self._run_cmd(action)
        data = resp.json()
        if "proxies" in data:
            now = resp.get(f'proxies.{self.selector}.now')
            nodes = resp.get(f'proxies.{self.selector}.all')
            if now in nodes:
                nodes = [*nodes[nodes.index(now) + 1:], *nodes[:nodes.index(now) + 1]]
            self._nodes = self.iter_node(list(filter(self.match, nodes)))
            return nodes
        else:
            return data

    def iter_node(self, nodes):
        def get_times_policy():
            count = itertools.count()

            def inner():
                nonlocal count
                flag = next(count) >= self.threshold
                if flag:
                    count = itertools.count()
                return flag

            return inner

        def get_time_policy():
            t1 = time.time()

            def inner():
                nonlocal t1
                flag = time.time() - t1 > self.threshold
                if flag:
                    t1 = time.time()

                return flag

            return inner

        # 使用次数
        if isinstance(self.threshold, int):
            ju = get_times_policy()

        elif isinstance(self.threshold, float):
            ju = get_time_policy()
        elif callable(self.threshold):
            ju = self.threshold

        else:
            raise ValueError(f"error threshold type: {self.threshold}")

        cycle = itertools.cycle(nodes)
        node = next(cycle)
        while True:
            if ju():
                node = next(cycle)
                self.switch(node)
            yield node

    @property
    def configs(self):
        if not self._configs:
            resp = self._run_cmd(f'/configs')
            self._configs = resp.json()
            key_parsed = urllib.parse.urlparse(self.key)
            if self.scheme == "http":
                port = self._configs.get("mixed-port") or self._configs.get("port")
            else:
                port = self._configs.get("mixed-port") or self._configs.get("socks-port")
            self._proxy.proxy = f'{key_parsed.hostname}:{port}'

        return self._configs

    @configs.setter
    def configs(self, v: dict):
        if "path" in v:
            force = v.get('force', 1)
            path = v["path"]

            self._run_cmd(
                f'/configs',
                method="PUT",
                params={"force": force},
                body={"path": path},
                headers={"Content-Type": "application/json"}
            )

        else:
            self._run_cmd(
                f'/configs',
                method="PATCH",
                body=v,
                headers={"Content-Type": "application/json"}
            )

        del self.configs

    @configs.deleter
    def configs(self):
        self._configs = None

    def delay(self, name: str, timeout: int = 1, url: str = "https://www.github.com"):
        resp = self._run_cmd(
            uri=f'/proxies/{name}/delay',
            params={
                "timeout": timeout,
                "url": url
            }
        )
        return resp.json()

    def switch(self, name: str, selector: str = None):
        resp = self._run_cmd(
            f'/proxies/{selector or self.selector}',
            body={"name": name},
            method="PUT",
            headers={"Content-Type": "application/json"}
        )
        return resp.ok

    def rules(self):
        resp = self._run_cmd(f'/rules')
        return resp.json()

    def _run_cmd(self, uri: str, method: str = "GET", retry: int = 5, **kwargs):
        uri = urllib.parse.urljoin(self.key, uri)
        headers = kwargs.setdefault('headers', {})
        self.secret and headers.setdefault("Authorization", f'Bearer {self.secret}')
        for _ in range(retry):
            try:
                resp = DOWNLOADER.fetch({
                    "url": uri,
                    "method": method.upper(),
                    **kwargs
                })
                assert resp.ok, f"请求失败: {resp.text}"
            except Exception as e:
                logger.warning(str(e))
            else:
                return resp
        else:
            raise RuntimeError(f"[clash 指令执行失败]: uri: {uri}, method: {method}")

    def fresh_cache(self, force: bool = False):
        if force or (self.cache_ttl != -1 and time.time() - self.ts > self.cache_ttl):
            self._nodes = itertools.repeat(None)
            del self.configs

        if not self._configs:
            _ = self.configs

        if isinstance(self._nodes, itertools.repeat):
            self.nodes()

    def get(self, timeout=None) -> Proxy:
        with self.lock:
            self.fresh_cache()
            self.now = next(self._nodes)
            return self._proxy

    def clear(self, proxy: Proxy):
        resp = self._run_cmd("/proxies")
        now = resp.get(f'proxies.{self.selector}.now')
        nodes = resp.get(f'proxies.{self.selector}.all')
        new = nodes[(nodes.index(now) + 1) % len(nodes)]
        self.switch(new)

    @staticmethod
    def cfg_tpl(http_port: int = 7890, socks_port: int = 7891):
        return {
            "port": http_port,
            "socks-port": socks_port,
            "redir-port": 14334,
            "allow-lan": True,
            "bind-address": "*",
            "mode": "rule",
            "log-level": "info",
            "external-controller": "0.0.0.0:9090",
            "proxies": [],
            "proxy-groups": [
                {
                    "name": "Proxy",
                    "type": "select",
                    "proxies": []
                }
            ],
            "tun": {
                "enable": False,
                "stack": "system",
                "macOS-auto-route": True,
                "macOS-auto-detect-interface": True,
                "dns-hijack": [
                    "tcp://8.8.8.8:53",
                    "tcp://8.8.4.4:53"
                ]
            },
            "rules": [
                "MATCH,Proxy"
            ]
        }

    @staticmethod
    def extract_nodes(
            path: str,
            form: typing.Literal["clash-sub", "clash", "v2ray", "v2ray-sub"] = 'clash-sub',
            env: dict = None,
    ):
        try:
            import yaml
        except ImportError:
            raise ImportError("请先安装 yaml")

        def extract_from_v2ray(content: bytes):
            data = base64.b64decode(content).decode()
            data = [json.loads(base64.b64decode(i.replace("vmess://", "", 1)).decode()) for i in data.split()]
            ret = []
            for i in data:
                node = {
                    "name": i['ps'],
                    "server": i['add'],
                    "port": int(i['port']),
                    "type": "vmess",
                    "uuid": i['id'],
                    "alterId": int(i['aid']),
                    "cipher": "auto",
                    "tls": bool(i['tls']),
                    "skip-cert-verify": False
                }
                if i.get("path"):
                    node.update({
                        "network": "ws",
                        "ws-opts": {
                            "path": i['path'],
                            "headers": {
                                "Host": i['host']
                            }
                        },
                        "ws-path": i['path'],
                        "headers": {
                            "ws-headers": i['host']
                        }
                    })

                ret.append(node)

            return ret

        def extract_from_clash(content: str):
            def fmt(x):
                if isinstance(x, str) and x.isdigit():
                    return f'str: {x}'
                else:
                    return x

            data = yaml.safe_load(content) or {}
            return [{k: fmt(v) for k, v in i.items()} for i in (data.get("proxies") or [])]

        def send_req(req: dict, retry: int = 5):
            headers = req.setdefault("headers", {})
            headers.setdefault("User-Agent", "ClashforWindows/0.19.10")
            req.setdefault('timeout', 30)
            req.setdefault('method', "GET")

            for _ in range(retry):
                try:
                    res = DOWNLOADER.fetch(req)
                    assert res.ok, f"响应失败，状态码为：{res.status_code}"
                    logger.debug(f"【请求成功】, req: {req['url']}")
                    return res
                except Exception as e:
                    logger.error(f"【请求失败】, req: {req}, error: {e}")
                    time.sleep(1)

            raise RuntimeError(f'【请求失败】, req: {req}')

        env = env or {}

        if form == 'clash-sub':
            resp = send_req({"url": path, **env})
            return extract_from_clash(resp.text)
        elif form == 'v2ray-sub':
            resp = send_req({"url": path, **env})
            return extract_from_v2ray(resp.content)
        elif form == 'v2ray':
            return extract_from_v2ray(path.encode())
        elif form == 'clash':
            return extract_from_clash(path)
        else:
            raise RuntimeError(f"不支持的订阅类型 path: {path}, form: {form}")

    @classmethod
    def subscribe(
            cls,
            *subs: dict,
            http_port: int = 7890,
            socks_port: int = 7891,
    ):
        """
        获取订阅， 生成一个配置文件（字符串形式）

        :param subs: 订阅列表
        :param http_port:
        :param socks_port:
        :return:
        """
        try:
            import yaml
        except ImportError:
            raise ImportError("请先安装 yaml")

        config = cls.cfg_tpl(http_port=http_port, socks_port=socks_port)
        counter = itertools.count()
        for sub in subs:
            try:
                nodes = cls.extract_nodes(sub['uri'], sub['type'])
            except Exception as e:
                logger.warning(f'提取节点失败: {e}')
                continue

            for node in nodes:
                node['name'] = f"{node['name']}-{next(counter)}"
                node not in config['proxies'] and config['proxies'].append(node)

        nodes_name = [i["name"] for i in config['proxies']]
        for group in config['proxy-groups']: group['proxies'] = nodes_name

        if not config['proxies']:
            return ""

        return yaml.safe_dump(config, default_flow_style=False, encoding='utf-8', allow_unicode=True).decode()


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
        while time.time() - start < timeout:
            proxy = self.container.pop(self.key)
            if not proxy:
                logger.warning(f'[获取代理失败] ref: {self}')
                time.sleep(1)
            else:
                return Proxy(proxy)
        raise TimeoutError

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

    def build(self, obj: (dict, BaseProxy)) -> BaseProxy:

        rkey = self.get_rkey(obj)
        if rkey not in self.container:
            if isinstance(obj, BaseProxy):
                self.container[rkey] = obj
            else:
                ref: Type[BaseProxy] = pandora.load_objects(obj["ref"])
                self.container[rkey] = ref.build(**obj)

        return self.container[rkey]

    def get(self, *objs: (dict, BaseProxy), timeout: int = None) -> Proxy:
        """

        获取代理

        :param objs: 获取代理的配置 -> {"ref": "指向代理类", ... 这些其他的都是实例化类的参数}
        :param timeout: 获取代理的超时时间, timeout 为 None 代表一直等待, 超时会直接使用空代理
        :return:
        """
        with self._context:
            for obj in objs:
                rkey = self.get_rkey(obj)

                if not hasattr(self._local, rkey):
                    pins: BaseProxy = self.build(obj)
                    try:
                        proxy = pins.get(timeout=timeout)
                    except TimeoutError:
                        proxy = Proxy()

                    proxy and setattr(self._local, rkey, proxy)

                temp = getattr(self._local, rkey, Proxy())
                temp.rkey = rkey
                if temp:
                    return temp
            else:
                return Proxy()

    def clear(self, *objs: (dict, BaseProxy)):
        """
        清除代理

        :param objs:
        :return:
        """
        with self._context:
            for config in objs:
                rkey = self.get_rkey(config)
                if hasattr(self._local, rkey):
                    proxy: Proxy = getattr(self._local, rkey)
                    callable(proxy.clear) and pandora.invoke(proxy.clear, args=[proxy])
                    delattr(self._local, rkey)

    def now(self, *objs: (dict, BaseProxy)) -> Proxy:
        """
        获取当前代理

        :param objs:
        :return:
        """
        with self._context:
            for config in objs:
                rkey = self.get_rkey(config)
                if hasattr(self._local, rkey):
                    return getattr(self._local, rkey)
            else:
                return Proxy()

    def recover(self, *objs: (dict, BaseProxy)):
        """
        回收代理

        :param objs:
        :return:
        """
        with self._context:
            for config in objs:
                rkey = self.get_rkey(config)
                if hasattr(self._local, rkey):
                    proxy: Proxy = getattr(self._local, rkey)
                    callable(proxy.recover) and pandora.invoke(proxy.recover, args=[proxy])
                    delattr(self._local, rkey)

    def fresh(self, *objs: (dict, BaseProxy)) -> Proxy:
        """
        刷新代理

        :param objs:
        :return:
        """
        with self._context:
            self.clear(*objs)
            return self.get(*objs)

    def use(self, proxy: Proxy):
        state = proxy.use()
        if state is False and hasattr(self._local, proxy.rkey):
            delattr(self._local, proxy.rkey)

    @staticmethod
    def get_rkey(obj: (dict, BaseProxy)):
        if isinstance(obj, BaseProxy):
            rkey = hash(BaseProxy)
        else:
            rkey = hash(json.dumps(obj, default=str))

        return str(rkey)

    def set_mode(self, mode: Literal[0, 1] = 0):
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
    # p = ClashProxy("http://127.0.0.1:9097")
    cfg = ClashProxy.subscribe({
        "uri": "https://sub1.smallstrawberry.com/api/v1/client/subscribe?token=xxx&flag=clash",
        "type": "clash-sub"
    })
    print(cfg)
