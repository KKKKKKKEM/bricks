# 代理管理

Bricks 内置了完整的代理管理体系，支持从 API 拉取、从 Redis 获取、通过 Clash 切换、以及直接使用固定代理。所有代理类型都通过统一的 `Manager` 进行管理，支持阈值回收、自动轮换。

---

## 代理类型概览

| 类 | 特点 |
|---|---|
| `ApiProxy` | 从 HTTP API 批量获取代理，内建本地队列缓冲 |
| `RedisProxy` | 从 Redis 队列中弹出代理（由外部代理池维护）|
| `ClashProxy` | 控制本地 Clash 客户端，循环切换节点 |
| `CustomProxy` | 直接使用固定代理字符串 |

---

## ApiProxy — HTTP API 代理池

通过调用代理供应商的 API 批量获取代理地址，并在本地维护缓冲队列。当队列为空时自动再次调用 API 补充。

```python
from bricks.lib.proxies import ApiProxy

proxy_pool = ApiProxy(
    key="https://api.proxy-provider.com/get?count=10&type=http",
    scheme="http",
    threshold=10,    # 每个代理最多使用 10 次后回收
)
```

### 参数说明

| 参数 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `key` | `str` | 必填 | 代理 API 的 URL |
| `scheme` | `str` | `"http"` | 代理协议（`http` / `https` / `socks5`）|
| `username` | `str` | `None` | 代理认证用户名 |
| `password` | `str` | `None` | 代理认证密码 |
| `threshold` | `int` | `inf` | 每个代理最大使用次数，超出后回收并获取新代理 |
| `handle_response` | `Callable` | 自动正则提取 IP:Port | 自定义解析 API 响应的函数 |
| `options` | `dict` | `{}` | 请求 API 的额外参数（如 headers、timeout 等）|
| `recover` | `Callable` | 回收到队列 | 代理达到阈值后的回收处理函数 |

### 自定义响应解析

当 API 返回非标准格式时，可自定义解析逻辑：

```python
import json
from bricks.lib.proxies import ApiProxy

proxy_pool = ApiProxy(
    key="https://api.example.com/proxy?format=json",
    handle_response=lambda res: [
        item["ip"] + ":" + str(item["port"])
        for item in json.loads(res.text)["data"]
    ],
)
```

---

## RedisProxy — Redis 代理池

从 Redis 中弹出代理，由外部程序（如代理采集器）向 Redis 队列写入代理。

```python
from bricks.lib.proxies import RedisProxy

proxy_pool = RedisProxy(
    key="proxy_pool:http",    # Redis key 名称
    options={                  # Redis 连接配置
        "host": "127.0.0.1",
        "port": 6379,
        "password": None,
        "database": 0,
    },
    scheme="http",
    threshold=5,               # 每个代理最多使用 5 次后回收（放回 Redis）
)
```

### 参数说明

| 参数 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `key` | `str` | 必填 | Redis key 名称 |
| `options` | `dict` | `{}` | Redis 连接参数 |
| `scheme` | `str` | `"http"` | 代理协议 |
| `threshold` | `int` | `inf` | 每个代理最大使用次数 |
| `recover` | `Callable` | 回收到 Redis | 超出阈值后的回收逻辑 |

---

## ClashProxy — Clash 节点轮换

通过控制本地运行的 Clash 客户端，自动在 Global 节点组内循环切换代理节点。

```python
from bricks.lib.proxies import ClashProxy

proxy_pool = ClashProxy(
    key="127.0.0.1:9090",   # Clash 外部控制器地址（external-controller）
    secret="your-secret",    # Clash API 密钥（如果设置了的话）
    selector="GLOBAL",       # 使用的节点组名称
    threshold=10,            # 每 10 次请求切换一次节点（int=次数, float=秒数）
    scheme="http",           # 代理协议
)
```

### 参数说明

| 参数 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `key` | `str` | 必填 | Clash 外部控制器地址，如 `127.0.0.1:9090` |
| `secret` | `str` | `None` | Clash API 密钥 |
| `selector` | `str` | `"GLOBAL"` | 使用的节点组名称 |
| `threshold` | `int\|float\|Callable` | `inf` | 切换节点的策略：int=使用次数，float=持续秒数，Callable=自定义 |
| `cache_ttl` | `int` | `-1` | 节点缓存刷新间隔（秒），-1 表示不刷新 |
| `match` | `Callable` | 过滤 DIRECT/REJECT | 过滤节点的函数 |

### 生成 Clash 配置（`subscribe`）

可以从订阅链接生成 Clash 配置文件：

```python
config = ClashProxy.subscribe(
    {
        "uri": "https://your-subscribe-url.com/token=xxx&flag=clash",
        "type": "clash-sub",   # clash-sub / v2ray-sub / clash / v2ray
    },
    http_port=7890,
    socks_port=7891,
)
# 返回 YAML 格式的 Clash 配置字符串
print(config)
```

---

## CustomProxy — 固定代理

直接使用一个固定的代理地址，不做任何轮换：

```python
from bricks.lib.proxies import CustomProxy

proxy = CustomProxy(
    key="http://127.0.0.1:7890",
    scheme="http",
)
```

---

## 在爬虫中使用代理

### 方式一：直接传入代理字符串

```python
from bricks.spider import air

class MySpider(air.Spider):
    ...

spider = MySpider(proxy="http://127.0.0.1:7890")
spider.run()
```

### 方式二：传入代理池对象

```python
from bricks.spider import air
from bricks.lib.proxies import ApiProxy

class MySpider(air.Spider):
    ...

proxy_pool = ApiProxy(key="https://api.example.com/proxy?count=10")
spider = MySpider(proxy=proxy_pool)
spider.run()
```

### 方式三：在 `make_request` 中动态设置

```python
from bricks import Request
from bricks.spider.air import Context
from bricks.lib.proxies import ApiProxy

proxy_pool = ApiProxy(key="https://api.example.com/proxy?count=10")

def make_request(self, context: Context) -> Request:
    return Request(
        url="https://example.com",
        proxies=str(proxy_pool.get()),  # 获取一个代理
    )
```

### 方式四：使用代理配置字典

```python
spider = MySpider(
    proxy={
        "ref": "bricks.lib.proxies.ApiProxy",   # 代理类的路径
        "key": "https://api.example.com/proxy",
        "threshold": 5,
    }
)
```

---

## Manager — 代理生命周期管理

`bricks.lib.proxies.manager` 是一个全局代理管理器实例，负责代理的获取、回收、刷新，并实现线程隔离（每个线程使用独立的代理）。

```python
from bricks.lib.proxies import manager, ApiProxy

proxy_config = {
    "ref": "bricks.lib.proxies.ApiProxy",
    "key": "https://api.example.com/proxy",
    "threshold": 10,
}

# 获取代理（同一线程内复用）
proxy = manager.get(proxy_config)

# 标记使用（到达 threshold 时自动清除）
manager.use(proxy)

# 手动清除代理（触发 clear 回调）
manager.clear(proxy_config)

# 回收代理（触发 recover 回调，如放回 API 队列）
manager.recover(proxy_config)

# 刷新代理（先清除，再获取新代理）
new_proxy = manager.fresh(proxy_config)

# 查看当前线程持有的代理
current = manager.now(proxy_config)
```

### 线程共享模式

默认情况下，每个线程拥有独立的代理（线程隔离）。可以切换为线程共享模式：

```python
# 线程共享（所有线程使用同一个代理）
manager.set_mode(1)

# 恢复线程隔离（默认）
manager.set_mode(0)
```

---

## 代理阈值与回收策略

通过 `threshold` 控制每个代理的使用寿命，到达阈值后自动触发回收：

```python
from bricks.lib.proxies import ApiProxy

# 每个代理最多使用 20 次
proxy_pool = ApiProxy(
    key="https://api.example.com/proxy",
    threshold=20,
    # 自定义回收逻辑（默认是放回内部队列）
    recover=lambda proxy: print(f"代理 {proxy.proxy} 已达上限，回收中..."),
)
```

`ClashProxy` 还支持按时间切换：

```python
from bricks.lib.proxies import ClashProxy

# 每 30 秒切换一次节点
proxy_pool = ClashProxy(
    key="127.0.0.1:9090",
    threshold=30.0,   # float 表示秒数
)
```
