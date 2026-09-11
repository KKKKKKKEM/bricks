"""代理来源、Slot 隔离、显式回收及 Air 重试资源的迁移契约。"""

import base64
import json
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from uuid import uuid4

import pytest

from interlace import Slot

from bricks import Request, Response, hooks
from bricks.plugins.on_request import Before
from bricks.proxies import (
    ApiProxy,
    BaseProxy,
    ClashProxy,
    CustomProxy,
    Manager,
    Proxy,
    RedisProxy,
    extract_nodes,
    format_proxy,
    subscribe,
)
from bricks.spider.air import Spider


class Supply(BaseProxy):
    """按取得次数返回不同地址的可观察供应源。

    Attributes:
        issued: 已获取代理，初始为空。
        recovered: 已归还代理，初始为空。
        discarded: 已丢弃代理，初始为空。
    """

    def __init__(self, **options):
        """初始化测试供应源。

        Args:
            options: BaseProxy 策略参数。
        """
        super().__init__(**options)
        self.issued = []
        self.recovered = []
        self.discarded = []

    def get(self, timeout=30):
        """生成唯一的测试代理。

        Args:
            timeout: 调用方等待预算。

        Returns:
            本次代理。
        """
        value = self._make(f"127.0.0.1:{8000 + len(self.issued)}")
        self.issued.append(value)
        return value

    def recover(self, proxy):
        """记录回收。

        Args:
            proxy: 被归还的代理。
        """
        self.recovered.append(proxy)

    def clear(self, proxy):
        """记录丢弃。

        Args:
            proxy: 被丢弃的代理。
        """
        self.discarded.append(proxy)


def test_proxy_address_and_credentials_are_explicit():
    """格式化支持认证转义和 IPv6，非法地址不静默改成直连。"""
    assert (
        format_proxy("[::1]:1080", scheme="socks5", username="a@b", password="x:y")
        == "socks5://a%40b:x%3Ay@[::1]:1080"
    )
    assert format_proxy("https://u:p@proxy.test:443") == "https://u:p@proxy.test:443"
    for value in (
        "",
        "proxy.test",
        "http://proxy.test:0",
        "ftp://proxy.test:80",
        "http://proxy.test:80/path",
        "http://proxy.test:80?key=x",
    ):
        with pytest.raises(ValueError):
            format_proxy(value)
    with pytest.raises(ValueError):
        Proxy("localhost:80", threshold=True)


def test_manager_slot_isolation_threshold_and_ownership():
    """不同 Slot 不共享代理，阈值推迟到下一次获取，重名管理器明确失败。"""
    source = Supply(threshold=2)
    manager = Manager(source)
    first, second = Slot(), Slot()
    a, b = manager.get(first), manager.get(second)
    assert a != b
    manager.use(first)
    assert manager.get(first) is a
    manager.use(first)
    assert manager.now(first) is a
    assert manager.get(first) != a
    assert source.recovered == [a]
    assert manager.now(second) is b
    with pytest.raises(ValueError, match="owner"):
        Manager(source).get(first)
    with pytest.raises(TypeError, match="Slot"):
        manager.get(None)
    manager.clear(second)
    assert source.discarded == [b] and manager.now(second) is None


def test_manager_concurrent_slots_and_failed_recovery():
    """共享管理器可服务多个 Slot，回收失败不会伪称占用已移除。"""
    source = Supply()
    manager = Manager(source)
    slots = [Slot() for _ in range(8)]
    with ThreadPoolExecutor(max_workers=4) as executor:
        proxies = list(executor.map(manager.get, slots))
    assert len({proxy.proxy for proxy in proxies}) == 8

    def fail(proxy):
        """模拟回收服务不可用。

        Args:
            proxy: 当前占用。

        Raises:
            ConnectionError: 模拟存储失败。
        """
        raise ConnectionError("return unavailable")

    source.recover = fail
    with pytest.raises(ConnectionError):
        manager.recover(slots[0])
    assert manager.now(slots[0]) is proxies[0]


def test_async_proxy_release_is_rejected_without_losing_slot():
    """异步释放不能被误报为已归还，未等待协程会关闭而占用保留。"""
    source = Supply()
    manager, slot = Manager(source), Slot()
    proxy = manager.get(slot)

    async def invalid_release(proxy):
        """提供违反同步供应源协议的释放回调。

        Args:
            proxy: 当前代理。
        """

    source.recover = invalid_release
    with pytest.raises(TypeError, match="synchronously"):
        manager.recover(slot)
    assert manager.now(slot) is proxy


def test_api_batch_cache_recovery_and_bounded_failure(server):
    """真实 HTTP API 响应可批量解析和回收，空响应按预算失败。

    Args:
        server: 本地 HTTP 服务。
    """
    base, seen, _ = server
    source = ApiProxy(
        base + "/echo",
        handle_response=lambda response: ["127.0.0.1:8001", "127.0.0.1:8002"],
    )
    first = source.get()
    second = source.get(timeout=0)
    assert first != second and len(seen) == 1
    source.recover(first)
    assert source.get(timeout=0) is first
    empty = ApiProxy(
        base + "/echo", handle_response=lambda response: [], poll_interval=0.001
    )
    with pytest.raises(TimeoutError):
        empty.get(timeout=0.015)
    fallback = Manager(empty, CustomProxy("localhost:7890"))
    assert fallback.get(Slot(), timeout=0).proxy == "http://localhost:7890"


@pytest.mark.parametrize("key_type", ["set", "list"])
def test_real_redis_proxy_pop_recover_clear(key_type):
    """专用 Redis 代理池原子取出、归还与丢弃，客户端保持调用方所有权。

    Args:
        key_type: set 或 list 模式。
    """
    url = os.environ.get("BRICKS_TEST_REDIS_URL")
    if not url:
        pytest.skip("set BRICKS_TEST_REDIS_URL for proxy integration")
    from redis import Redis

    key = "bricks-proxy:" + uuid4().hex
    with Redis.from_url(url, socket_timeout=2) as client:
        try:
            if key_type == "set":
                client.sadd(key, "localhost:7890")
            else:
                client.rpush(key, "localhost:7890")
            source = RedisProxy(key, client, key_type=key_type)
            proxy = source.get(timeout=0)
            with pytest.raises(TimeoutError):
                source.get(timeout=0)
            source.recover(proxy)
            assert source.get(timeout=0) == proxy
            source.clear(proxy)
            with pytest.raises(TimeoutError):
                source.get(timeout=0)
            assert client.ping()
        finally:
            client.delete(key)


def test_clash_encoded_control_and_exclusive_rotation():
    """控制路径编码组名，同一控制器被占用时禁止第二个 Slot 切换出口。"""
    requests = []

    class Controller:
        """模拟公开控制器 HTTP 合约。"""

        def fetch(self, request):
            """记录命令并返回配置或选择组。

            Args:
                request: 明确的控制请求。

            Returns:
                控制器响应。
            """
            requests.append(request)
            assert request.headers["Authorization"] == "Bearer test-secret"
            if request.method != "GET":
                return Response(status_code=204, request=request)
            data = (
                {"mixed-port": 7890}
                if request.url.endswith("/configs")
                else {"all": ["DIRECT", "A", "B"]}
            )
            return Response(json.dumps(data).encode(), status_code=200, request=request)

    source = ClashProxy(
        "http://localhost:9090",
        selector="A/B 中文",
        secret="test-secret",
        downloader=Controller(),
        threshold=1,
    )
    manager = Manager(source)
    slot = Slot()
    first = manager.get(slot)
    assert first.proxy == "http://localhost:7890"
    with pytest.raises(TimeoutError, match="available"):
        manager.get(Slot(), timeout=0.1)
    with pytest.raises(RuntimeError, match="leased"):
        source.switch("B")
    manager.use(slot)
    second = manager.get(slot)
    assert first is not second
    switches = [request for request in requests if request.method == "PUT"]
    assert [json.loads(request.body)["name"] for request in switches] == ["A", "B"]
    assert all("A%2FB%20%E4%B8%AD%E6%96%87" in request.url for request in switches)
    manager.recover(slot)
    source.configure({"mode": "global"})
    assert requests[-1].method == "PATCH"


def test_subscription_parsing_and_nonmutating_config():
    """YAML 和 VMess 内容明确解析，重名节点生成稳定配置且不修改来源。"""
    nodes = extract_nodes(
        "proxies:\n  - name: node\n    type: http\n    server: localhost\n    port: 8080\n"
    )
    config = subscribe(nodes, nodes)
    import yaml

    parsed = yaml.safe_load(config)
    assert [node["name"] for node in parsed["proxies"]] == ["node", "node-1"]
    assert nodes[0]["name"] == "node" and parsed["allow-lan"] is False
    payload = {
        "ps": "vmess",
        "add": "localhost",
        "port": "443",
        "id": "test-uuid",
        "aid": "0",
        "tls": "tls",
        "net": "ws",
        "path": "/ws",
        "host": "example.com",
    }
    link = "vmess://" + base64.b64encode(json.dumps(payload).encode()).decode()
    content = base64.b64encode(link.encode()).decode()
    result = extract_nodes(content, form="v2ray")
    assert result[0]["tls"] is True and result[0]["ws-opts"]["path"] == "/ws"
    with pytest.raises(yaml.constructor.ConstructorError):
        extract_nodes("!!python/object:builtins.object {}")


@pytest.mark.parametrize("rotate", [False, True])
def test_air_retries_pin_proxy_and_close_recovers_owned_slots(rotate):
    """默认重试保留代理，只有显式 clear_proxy 才切换，关闭时归还占用。

    Args:
        rotate: 是否在业务重试回调中显式丢弃代理。
    """
    source = Supply(threshold=1)
    seen = []
    attempts = {}

    class Download:
        """记录当前请求实际使用的代理。"""

        def fetch(self, request):
            """前两次返回可重试状态。

            Args:
                request: 当前下载请求。

            Returns:
                第三次起成功的响应。
            """
            attempts[request.url] = attempts.get(request.url, 0) + 1
            seen.append((request.url, request.proxy, request.headers["User-Agent"]))
            return Response(
                b"{}",
                status_code=503 if attempts[request.url] < 3 else 200,
                request=request,
            )

    class RetrySpider(Spider):
        """在同一槽中依次消费两个种子。"""

        def make_seeds(self, context, **kwargs):
            """提供两个独立目标。

            Args:
                context: 初始化上下文。
                kwargs: 恢复记录。

            Returns:
                种子列表。
            """
            return [{"url": "https://example.com/1"}, {"url": "https://example.com/2"}]

        def install(self):
            """注册一次生成后在重试中保留的 UA。"""
            super().install()
            self.use(
                hooks.BEFORE_REQUEST,
                {"func": Before.fake_ua, "kwargs": {"factory": lambda: uuid4().hex}},
            )

        def on_retry(self, context):
            """仅在显式策略要求时丢弃代理。

            Args:
                context: 当前重试上下文。
            """
            if rotate:
                context.clear_proxy()

    spider = RetrySpider(proxy=source, downloader=Download(), max_retry=2)
    assert spider.run().counts["acked"] == 2
    assert len(source.issued) == (6 if rotate else 2)
    assert source.recovered == (source.issued[2::3] if rotate else source.issued)
    assert len(source.discarded) == (4 if rotate else 0)
    assert len(seen) == 6
    assert len({value[1] for value in seen[:3]}) == (3 if rotate else 1)
    assert len({value[2] for value in seen[:3]}) == 1
    assert seen[0][1] != seen[3][1]


def test_explicit_request_proxy_wins_and_default_imports_are_lazy():
    """显式请求代理不触发供应商，默认导入不加载 Mongo、Redis 或 YAML。"""
    source = Supply()
    context = SimpleNamespace(
        target=SimpleNamespace(proxy=Manager(source)),
        slot=Slot(),
        request=Request("https://example.com", proxy="http://localhost:9999"),
    )
    Before.set_proxy(context)
    assert source.issued == []
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys; import bricks.spider, bricks.proxies; from bricks.plugins import storage, make_seeds, scripts; assert not {'redis', 'pymongo', 'yaml'} & set(sys.modules)",
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
