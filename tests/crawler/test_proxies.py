"""代理来源、Slot 隔离、显式回收及 Air 重试资源的迁移契约。"""

import base64
import json
import os
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from threading import Event, Lock
from uuid import uuid4

import pytest
from interlace.engine import Slot

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
    cfg_tpl,
    extract_nodes,
    fetch_subscription,
    format_proxy,
    subscribe,
)
from bricks.spider.air import Spider


class FakeClock:
    """供应源轮询使用的可控单调时钟，sleep 只推进时间而不真实等待。

    Attributes:
        now: 当前虚拟单调时间，单位秒，从 0 开始。
    """

    def __init__(self):
        """从零时刻开始。"""
        self.now = 0.0

    def monotonic(self):
        """读取虚拟单调时间。

        Returns:
            当前虚拟秒数。
        """
        return self.now

    def sleep(self, seconds):
        """推进虚拟时间。

        Args:
            seconds: 请求等待的秒数。
        """
        self.now += seconds


@pytest.fixture
def clock(monkeypatch):
    """让 HTTP API 与 Redis 供应源的等待预算只受虚拟时钟控制。

    Args:
        monkeypatch: pytest 属性替换夹具。

    Returns:
        已替换到 bricks.proxies.sources 的虚拟时钟。
    """
    fake = FakeClock()
    monkeypatch.setattr("bricks.proxies.sources.time", fake)
    return fake


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
    # 解析错误消息不回显含认证信息的原地址。
    for value in ("http://user:p℀secret@proxy.test:80", "http://user:secret@[bad:80"):
        with pytest.raises(ValueError) as caught:
            format_proxy(value)
        assert "secret" not in str(caught.value)


def test_proxy_sources_reject_missing_intervals_at_construction():
    """轮询间隔和缓存时长不能为 None，构造时明确失败而不是使用时才出错。"""
    with pytest.raises(TypeError, match="poll_interval"):
        ApiProxy("http://provider.test/list", poll_interval=None)
    with pytest.raises(TypeError, match="poll_interval"):
        RedisProxy("pool", object(), poll_interval=None)
    with pytest.raises(TypeError, match="cache_ttl"):
        ClashProxy("http://localhost:9090", cache_ttl=None)


def test_proxy_repr_redacts_credentials():
    """代理值及其 Slot 占用表示均隐藏账号和密码。"""
    password = "secret"
    proxy = Proxy(f"http://user:{password}@localhost:8080")
    manager, slot = (
        Manager(CustomProxy("localhost:8080", username="user", password=password)),
        Slot(),
    )
    manager.get(slot)
    assert password not in repr(proxy)
    assert "user" not in repr(proxy)
    assert password not in repr(slot[manager.namespace])
    assert "user" not in repr(slot[manager.namespace])


def test_redis_proxy_recover_preserves_raw_pool_value():
    """无服务测试确认 Redis recover 写回池内原始地址。"""

    class Client:
        """记录最小 Redis 列表操作。"""

        def __init__(self):
            self.values = ["localhost:8080"]

        def lpop(self, key):
            return self.values.pop(0) if self.values else None

        def rpush(self, key, value):
            self.values.append(value)

    client = Client()
    source = RedisProxy(
        "pool", client, key_type="list", username="user", password="secret"
    )
    proxy = source.get(timeout=0)
    source.recover(proxy)
    assert client.values == ["localhost:8080"]


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


def test_manager_ttl_namespace_and_fresh():
    """ttl 到期和 fresh 会替换占用，不同命名空间可共存。"""
    first = Manager(Supply(), namespace="first")
    second = Manager(Supply(), namespace="second")
    slot = Slot()
    a, b = first.get(slot), second.get(slot)
    assert first.now(slot) is a and second.now(slot) is b
    fresh = first.fresh(slot)
    assert fresh is not a and second.now(slot) is b
    expiring = Manager(Supply(ttl=0.01))
    expiring_slot = Slot()
    previous = expiring.get(expiring_slot)
    time.sleep(0.02)
    assert expiring.get(expiring_slot) is not previous


def test_manager_fallback_propagates_non_timeout_errors():
    """供应源发生程序错误时不继续 fallback。"""

    class Broken(Supply):
        """取得代理时产生程序错误。"""

        def get(self, timeout=30):
            raise ValueError("provider configuration is invalid")

    manager = Manager(Broken(), CustomProxy("localhost:7890"))
    with pytest.raises(ValueError, match="configuration"):
        manager.get(Slot())


def test_manager_provider_wait_does_not_block_other_slots():
    """一个 Slot 等待供应源时，其他 Slot 可继续取得代理。"""
    started, release = Event(), Event()

    class Provider(Supply):
        """仅第一次获取阻塞的测试供应源。"""

        def __init__(self):
            super().__init__()
            self.calls = 0
            self.guard = Lock()

        def get(self, timeout=30):
            with self.guard:
                self.calls += 1
                call = self.calls
            if call == 1:
                started.set()
                if not release.wait(timeout):
                    raise TimeoutError("test provider wait expired")
            return super().get(timeout)

    manager = Manager(Provider())
    first, second = Slot(), Slot()
    with ThreadPoolExecutor(max_workers=2) as executor:
        pending = executor.submit(manager.get, first, timeout=1)
        assert started.wait(1)
        assert manager.get(second, timeout=0.5)
        release.set()
        assert pending.result(timeout=1)


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


def test_api_batch_cache_recovery_and_bounded_failure(server, clock):
    """真实 HTTP API 响应可批量解析和回收，空响应按预算失败。

    Args:
        server: 本地 HTTP 服务。
        clock: 供应源虚拟时钟。
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
        base + "/echo", handle_response=lambda response: [], poll_interval=0.25
    )
    with pytest.raises(TimeoutError):
        empty.get(timeout=1)
    assert clock.now == 1
    fallback = Manager(empty, CustomProxy("localhost:7890"))
    assert fallback.get(Slot(), timeout=0).proxy == "http://localhost:7890"


@pytest.mark.parametrize("status", [429, 500, -1])
def test_api_proxy_retry_statuses_preserve_transport_error(status, clock):
    """API 对可重试状态有限等待，传输失败保留原因。

    Args:
        status: 供应商响应状态。
        clock: 供应源虚拟时钟。
    """
    error = OSError("network down")
    request = Request("https://provider.example/api")

    class Downloader:
        """返回固定状态的供应商响应。"""

        def fetch(self, request):
            return Response(
                status_code=status,
                request=request,
                error=error if status == -1 else None,
            )

    source = ApiProxy(request.url, downloader=Downloader(), poll_interval=0.25)
    with pytest.raises(TimeoutError) as caught:
        source.get(timeout=1)
    assert clock.now == 1
    if status == -1:
        assert caught.value.__cause__ is error


def test_api_proxy_rejects_client_error_and_filters_invalid_addresses():
    """API 4xx 明确失败，默认解析器过滤越界端口与 IPv4 八位组。"""
    from bricks.proxies.sources import extract_addresses

    valid = ".".join(("127", "0", "0", "1")) + ":80"
    response = Response(
        f"{valid} 999.1.1.1:80 {valid.rsplit(':', 1)[0]}:70000".encode(),
        status_code=200,
    )
    assert extract_addresses(response) == [valid]
    request = Request("https://provider.example/api")

    class Downloader:
        """返回固定 4xx 响应。"""

        def fetch(self, request):
            return Response(status_code=403, request=request)

    with pytest.raises(RuntimeError, match="403"):
        ApiProxy(request.url, downloader=Downloader()).get(timeout=1)


def test_subscription_fetch_cfg_and_malformed_vmess():
    """订阅下载、模板默认值与 VMess 畸形输入按契约处理。"""
    request = Request("https://provider.example/sub")

    class Downloader:
        """返回一个有效 Clash 节点。"""

        def fetch(self, request):
            return Response(
                b"proxies:\n  - name: node\n    type: http\n    server: localhost\n    port: 8080\n",
                status_code=200,
                request=request,
            )

    assert fetch_subscription(request, downloader=Downloader())[0]["name"] == "node"
    config = cfg_tpl(secret="token")
    assert config["secret"] == "token" and config["proxy-groups"][0]["name"] == "GLOBAL"
    payloads = [
        "[]",
        "{}",
        json.dumps(
            {"ps": "x", "add": "h", "port": "1", "id": "u", "aid": "", "type": "http"}
        ),
    ]
    for payload in payloads:
        link = "vmess://" + base64.b64encode(payload.encode()).decode()
        content = base64.b64encode(link.encode()).decode()
        with pytest.raises(ValueError):
            extract_nodes(content, form="v2ray")


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

    matched = []
    source = ClashProxy(
        "http://localhost:9090",
        selector="A/B 中文",
        secret="test-secret",
        downloader=Controller(),
        match=lambda name: matched.append(name) is None and name in {"A", "B"},
        threshold=1,
    )
    manager = Manager(source)
    slot = Slot()
    first = manager.get(slot)
    assert first.proxy == "http://localhost:7890"
    # 预算远大于断言上限：重叠获取必须立即失败，而不是耗尽等待预算。
    began = time.monotonic()
    with pytest.raises(TimeoutError, match="available"):
        manager.get(Slot(), timeout=10)
    assert time.monotonic() - began < 2
    with pytest.raises(RuntimeError, match="leased"):
        source.switch("B")
    manager.use(slot)
    second = manager.get(slot)
    assert first is not second
    switches = [request for request in requests if request.method == "PUT"]
    assert [json.loads(request.body)["name"] for request in switches] == ["A", "B"]
    assert all("A%2FB%20%E4%B8%AD%E6%96%87" in request.url for request in switches)
    assert matched == ["DIRECT", "A", "B"]
    assert (
        sum(
            request.method == "GET" and not request.url.endswith("/configs")
            for request in requests
        )
        == 1
    )
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
    assert parsed["mode"] == "global" and parsed["proxy-groups"][0]["name"] == "GLOBAL"
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
    payload["aid"] = ""
    link = "vmess://" + base64.b64encode(json.dumps(payload).encode()).decode()
    content = base64.b64encode(link.encode()).decode()
    assert extract_nodes(content, form="v2ray")[0]["alterId"] == 0
    with pytest.raises(yaml.constructor.ConstructorError):
        extract_nodes("!!python/object:builtins.object {}")
    for content in ("error: token expired", "proxies: []"):
        with pytest.raises(ValueError):
            extract_nodes(content)


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

    spider = RetrySpider(
        proxy=source,
        downloader=Download(),
        max_retry=2,
        slot_session_reuse=False,
    )
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

    class Download:
        """返回固定成功响应。"""

        def fetch(self, request):
            """返回当前请求的空响应。

            Args:
                request: 已保留显式代理的请求。

            Returns:
                固定成功响应。
            """
            return Response(b"{}", status_code=200, request=request)

    class ExplicitProxySpider(Spider):
        """通过默认 set_proxy 插件验证请求级代理优先。"""

        def make_seeds(self, context, **kwargs):
            """提供一条测试种子。

            Args:
                context: 初始化 Context。
                kwargs: 初始化记录。

            Returns:
                单条种子。
            """
            return {"url": "https://example.com"}

        def make_request(self, context):
            """构造带显式代理的请求。

            Args:
                context: 消费 Context。

            Returns:
                当前请求。
            """
            return Request(context.seeds["url"], proxy="http://localhost:9999")

    spider = ExplicitProxySpider(
        proxy=source, downloader=Download(), slot_session_reuse=False
    )
    assert spider.run().counts["acked"] == 1
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
