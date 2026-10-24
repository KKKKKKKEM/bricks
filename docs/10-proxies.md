# 第十章：代理管理

`bricks.proxies` 保留固定、API、Redis、Clash 供应源和管理工具的业务能力。
调用方显式创建对象；模块不创建全局 `manager`、单例、下载会话或客户端。

## 在 Air 中使用

```python
from bricks.proxies import ApiProxy

provider = ApiProxy(
    "https://proxy-provider.example/api",
    handle_response=lambda response: response.json()["addresses"],
)
spider = MySpider(proxy=provider)
spider.run()
```

`proxy` 接受地址字符串、`ProxyProvider` 或 `Manager`。供应源自动装配成实例级 Manager，
缓存保存在当前 Slot。`Request.proxy` 的显式地址优先，并在重试中始终保留。
`threshold` 是允许的新请求次数，`ttl` 是最大占用秒数，两者都可以为 None。
Air 自动会话模式按 `Request.proxy` 把请求路由到当前 Slot 内对应地址的会话（见 `ProxySessions`），
托管代理按请求轮换，`auth`、`threshold` 与 `ttl` 照常生效。

默认注册的 `Before.set_proxy` 在重试时更换代理：它选定的地址一旦用于下载，重试回到 before_request 时会调用
`context.clear_proxy()` 丢弃该地址（托管代理不放回供应池），再取得新地址及对应会话。下载前触发的 Retry
尚未使用代理，不会轮换。需要在重试中沿用原代理（例如保持会话 Cookie）时，把执行配置 `crawler.proxy.rotate`
设为 False；它可以作为 Spider 默认值，也可以只覆盖个别任务：

```python
spider = MySpider(proxy=provider, options={"crawler.proxy.rotate": False})
context.submit(seed, options={"crawler.proxy.rotate": False})
```

代理连接失败（见 Spider 的 `is_proxy_error`）先消耗独立的 `crawler.proxy.retry`（默认 5），不占用 `crawler.retry.max`。

| 供应源 | 行为 |
| --- | --- |
| `CustomProxy(key)` | 固定 URL 或 host:port，支持显式认证编码 |
| `ApiProxy(key, request=..., downloader=..., handle_response=...)` | API 批量缓冲，默认提取 IPv4:port；其他响应格式用同步解析函数 |
| `RedisProxy(key, client, key_type='set')` | 从显式客户端的 set/list 原子弹出地址，recover 归还 |
| `ClashProxy(key, selector=..., proxy=...)` | 控制选择组轮换共享出口，同一实例拒绝多个未归还占用 |

统一 `BaseProxy` 参数包括 `scheme`、`username`、`password`、同步 `auth(request)`、`threshold`、`ttl`。
自定义供应源只需实现 `get(timeout)->Proxy`、`recover(proxy)`、`clear(proxy)`，不必继承基类。
无代理时明确抛出 TimeoutError，不回退直连。程序、配置、认证、解析和存储异常继续传播。
API 只重试网络失败、429、5xx 和空地址结果；可配置有限等待，取消仍遵循同步协作式边界。

## 显式 Slot 管理

```python
from interlace.engine import Slot
from bricks.proxies import CustomProxy, Manager

manager = Manager(CustomProxy("localhost:7890", threshold=10))
slot = Slot()
proxy = manager.get(slot, timeout=5)
manager.use(slot)
assert manager.now(slot) is proxy
manager.recover(slot)
```

`get` 取得或复用当前代理，`use` 登记一次使用，`now` 只查看；`recover` 归还可用代理，
`clear` 丢弃失效代理，`fresh` 丢弃后获取。回收失败时保留占用并报告错误。
同一 Slot 的不同 Manager 必须提供不同 `namespace`，不会覆盖彼此的资源。
多个供应源可按顺序作为 fallback；只有 TimeoutError 才尝试下一项，等待预算共同使用。

Air 自建 Slot 的代理在停止执行后归还。借用 Slot 时，应在 Spider 停止后由装配方调用
`manager.recover(slot)`；管理器不会枚举其他进程的 Slot 或关闭借用客户端。
Redis 代理供应源使用弹出/归还语义，不是任务队列租约，进程退出可能需要外部代理池补充。

## 与会话配合

单个会话仍在 prepare_session 时固定地址，代理管理工具不会修改已有 HTTP 或浏览器会话的代理：

```python
from bricks.downloaders.curl_cffi import CurlCffiDownloader

factory = CurlCffiDownloader()
proxy = manager.get(slot)
session = factory.prepare_session(proxy=proxy.proxy)
slot["crawler.downloaders"] = {"default": session}
```

Air 使用自建 Slot 和同步 SessionDownloader 时，每个 Slot 装配一个
[`ProxySessions`](03-crawler-download.md#按代理分会话)：按请求代理惰性创建会话，转发前清除 Request.proxy，
超过 `spider.session.proxies` 时关闭最久未用的会话，执行停止后关闭全部会话并归还代理。Cookie 按“Slot × 代理”隔离，
更换代理即使用另一会话，Cookie 不迁移。直接使用 [Slot 会话下载节点](03-crawler-download.md)、
异步 Session 或注入自定义 SlotPool 时，调用方仍按上述顺序显式管理。

## Clash 控制与订阅

```python
from bricks.proxies import ClashProxy

provider = ClashProxy(
    "http://127.0.0.1:9090",
    selector="Proxy",
    proxy="http://127.0.0.1:7890",
    threshold=10,
)
nodes = provider.nodes()
configs = provider.configs
# 只有显式调用才修改模式；构造函数不会修改运行配置。
provider.configure({"mode": "rule"})
```

`nodes`、`configs`、`rules` 查询控制器；`switch`、`configure` 显式修改。
`delay(name, url=..., timeout=5000)` 的 timeout 和结果 delay 使用毫秒，遵循
[Mihomo 控制 API](https://wiki.metacubex.one/en/api/)。

一个控制器的选路会影响共享出口。同一实例已有占用时，第二次 get 立即失败，switch/configure 也会拒绝。
并发账号应装配独立控制器或独立出口，不能用多个 Slot 包装同一全局选择组来宣称隔离。
本实例无法阻止外部应用或其他进程切换该控制器。框架固定的是代理配置，不承诺供应商出口 IP 不轮换。

YAML 工具通过 `uv sync --locked --extra clash` 安装；`cfg_tpl()` 默认使用 `GLOBAL` 选择组并生成随机控制器 secret，
也可显式传入 secret 后配置 `ClashProxy(secret=...)`。

```python
from bricks import Request
from bricks.proxies import extract_nodes, fetch_subscription, subscribe

# 已有 YAML 或 Base64 VMess 内容，用纯解析函数。
nodes = extract_nodes(subscription_text, form="clash")
# 网络订阅必须显式调用，不把内容字符串自动解释成 URL 或路径。
remote_nodes = fetch_subscription(
    Request("https://provider.example/subscription", timeout=20), form="clash"
)
config_text = subscribe(nodes, remote_nodes, http_port=7890, socks_port=7891)
```

Clash 使用 safe_load；VMess 订阅支持 TCP/WebSocket，其他传输明确失败。
配置合并复制节点，重名使用稳定数字后缀，默认仅监听本机，不静默忽略错误订阅。

[返回目录](README.md) · [下一章：从 dev 迁移](11-dev-migration.md)
