# 第二章：爬虫领域模型

`bricks` 提供请求、响应与记录模型，以及[下载器和下载节点](03-crawler-download.md)。
当前还没有爬虫调度器或流式上传实现。

## Request 的请求体

```python
from bricks import Request, UploadFile

form = Request(
    "https://example.com/submit", method="POST",
    body={"name": "Alice", "tag": ["a", "b"]}, body_type="form",
)
upload = Request(
    "https://example.com/upload", method="POST", body_type="multipart",
    body={"description": "report", "file": UploadFile("report.txt", b"contents", "text/plain")},
)
```

| body_type | 输入与编码 |
| --- | --- |
| `auto`（默认） | 字典、列表、元组编码为 JSON；字符串按 UTF-8 编码，字节原样保留 |
| `json` | JSON 对象、数组、字符串、数字、布尔值进行 JSON 序列化；不接受 bytes |
| `form` | 字典或有序键值对编码为 URL 编码表单；列表值展开为重复字段，None 字段省略；字符串和字节视为已编码表单 |
| `multipart` | 字典或有序键值对，字段值为普通标量、字节或 UploadFile；列表值展开为重复字段或文件 |
| `raw` | 字符串按 UTF-8 编码，字节原样保留；XML 等内容通过 headers 指定媒体类型 |

所有模式中，`body=None` 都表示没有请求体。显式 `json` 会为字符串添加 JSON 引号；已有 JSON 文本应使用 `raw`
并指定 `Content-Type: application/json`。`json` 拒绝 NaN、Infinity 和不可序列化对象。

`request.body` 读取结果为编码后的 bytes 或 None。赋值支持以上输入，内部保留独立的数据快照；修改
`request.body_type` 会重新编码该快照。编码失败保留原来的 body、类型和请求头。HTTP method 不随 body 自动变化。

```python
form.body = {"page": 2}
derived = form.copy(body_type="json")
```

JSON 和 form 在没有手动指定 Content-Type 时自动添加请求头。切换类型或替换 body 时，自动生成的类型同步更新；
手动指定的类型保留，由调用方保证与编码一致。multipart 始终设置带有实际 boundary 的 Content-Type，覆盖旧值。
完成编码后仍可以直接修改 headers，调用方应保证 multipart boundary 与 body 一致。

替换 body 或切换类型会清除旧 Content-Length。`copy()` 隔离可变数据；没有覆盖 body、类型或 headers 时，
保留当前请求头和实际请求体字节，包括调用方已删除的自动 Content-Type。显式覆盖 headers 时重新应用构造规则，
multipart 仍保留与原请求体匹配的 boundary。UploadFile 接受内存中的字节或文本，不打开文件、不接管文件句柄，也不支持流式上传。
同时覆盖 body 和 headers 时，也会清除传入 headers 中的 Content-Length。

## 编辑与共享

Request 可以在请求前 Hook 中原地修改 headers、params、cookies 和 body；独立分支应先 copy。Headers 查询不区分大小写，
赋值替换所有同名字段，add 追加字段；params 同样支持赋值替换与 add 追加，但键名区分大小写。
运行时不会自动复制领域对象；下载节点在选择和调用下载器前复制请求，HTTPX 下载器记录实际发送的请求快照。

headers 和 params 的 `update()` 遵循普通映射语义；传入另一个多值容器时，只读取每个名称的最后一个值。
需要保留全部重复值时，使用 `source.raw` 逐项调用 `add(name, value)`，或直接赋值整个容器以构造独立副本。

请求 cookies 是受校验的可变映射，构造、整体赋值和逐项修改均校验名称和值。值使用未加引号的 HTTP cookie-octet
字符集合：允许空值，不接受空白、控制字符、非 ASCII 字符、双引号、逗号、分号和反斜杠；模型不自动转义或编码。
批量 `update()` 按普通映射语义逐项执行，后续项失败不会撤销此前成功的项。

## cURL 导入与导出

```python
request = Request.from_curl(
    "curl 'https://example.com/api' -H 'Content-Type: application/json' --data-raw '{\"page\":1}'"
)
command = request.to_curl()
```

导入只解析单条 POSIX cURL 命令，不执行 shell、不展开变量、不读取文件。支持 URL、`-X`、重复 `-H`、
`-b` 内联 Cookie、`-A`、`-e`、`-u user:password`、`-x`、`-m`、`-L`、`-G`、`-I`、
`-d`、`--data-raw`、`--data-binary`、`--data-urlencode`、`--json`、`-F` 和 `--form-string`。
可忽略的输出控制选项包括 `-s`、`-S`、`-i`、`-v`。不同请求体选项族混用、多个 URL 和未支持的选项会报错，
包括模型无法表达的 `--insecure` 和 `--compressed`。不支持 Bash 的 `$'...'` 转义字符串或 PowerShell 语法。

导入遵循 cURL 的默认设置：GET、不跟随重定向、不限时；存在 body 时默认 POST。已编码请求体保留为字节，
不重新解析 JSON。HTTP 方法限定为 GET、HEAD、POST、PUT、DELETE、CONNECT、OPTIONS、TRACE、PATCH。

```python
upload = Request.from_curl(
    "curl https://example.com/upload -F 'file=@report.txt;type=text/plain'",
    files={"report.txt": b"contents"},
)
command = upload.to_curl(body_file="request-body.bin")
```

`files` 显式提供文件引用的内容，包括 `--data-binary @文件` 和 multipart 上传。导出默认将 UTF-8 请求体放入
`--data-raw`；二进制或含 NUL 的请求体必须指定 `body_file`。该参数仅生成文件引用，不写文件，调用方需要
将完整 `request.body` 保存到该路径。导出包含认证信息与 Cookie，不做脱敏。

## Response 与 Cookie

Response 保存服务器响应或未获得正常响应时的内部错误。`status_code` 为 100–599 时表示服务器返回了 HTTP
状态，`error` 必须为 None；`status_code=-1` 时必须提供 Exception 对象，`reason` 缺省时取异常消息或类名。
异常身份及原始异常链会保留，方便按类型处理。引擎控制异常和取消信号继续传播，不转换为内部失败响应。
`-1` 不保证服务器没有收到请求，不能据此判断重复发送是否安全。

```python
from bricks import Response

failed = Response(status_code=-1, error=ConnectionError("proxy unavailable"))
response = Response(b"text", url="https://example.com", headers={"Set-Cookie": "session=abc; Path=/"})
response.encoding = "utf-8"
response.content = b"updated by hook"
cookies = response.cookies.to_string()
assert cookies == str(response.cookies)
```

Response 以读取为主，允许直接修改 content 和 encoding；其他字段通过 `copy()` 创建变体，状态与异常始终一起
校验。text 和 json() 每次读取当前内容，不缓存旧结果。`ok` 表示状态码在 200–399，不能代表业务成功。
关联 Request 与 history 在构造和复制时保存独立副本。未提供响应 URL 时取关联 Request 的实际地址。

`response.size()` 返回当前 content 的字节数，结果为整数。例如 UTF-8 的“中文”，size() 为 6。
修改编码不改变字节数；修改 content 后立即更新。不读取 Content-Length，也不包含响应头、重定向历史或对象内存
开销。字节数统计当前存储的响应体，不代表压缩前后的网络传输流量。需要文本字符数时使用 `len(response.text)`。

Cookies 保存标准库 Cookie 记录，保留域、路径、Secure、有效期及扩展属性。省略 cookies 参数时，从响应头的
Set-Cookie 自动提取，需要来源 URL；显式提供 Cookies、CookieJar、Cookie 序列或简写字典时使用提供的数据。
解析时使用标准库 CookieJar 的来源和有效期策略；原始 Set-Cookie 始终保留在 headers 中。
因此 `Max-Age=0` 等删除指令不会出现在 response.cookies 中。后续会话组件必须使用原始 Set-Cookie 和来源 URL
更新已有会话 CookieJar，不能只合并 response.cookies，否则会遗漏删除操作。

`cookies.get(name)` 返回单个值，同名记录有歧义时要求指定 domain/path。`to_string()` 和 `str()` 提取全部记录，
不筛选有效期；`to_string(url=目标地址)` 根据域、路径、Secure 和有效期筛选。不带来源域的简写记录只用于提取，
不会自动匹配任意主机。容器迭代返回完整 Cookie 副本。该实现不提供浏览器的 SameSite 请求上下文或公共后缀列表策略。

没有增加 cookie_str、follow 或页面解析接口；文件传输和持久化格式仍由后续适配器定义，异常对象目前仅用于进程内。

## Items 整理解析结果

Items 是字典记录的轻量集合，支持列表索引、切片、追加和删除。字段转换使用普通函数，不引入 Schema、校验规则
或持久化接口。

```python
from bricks import Items

items = Items([{"title": "News", "url": "/1"}, {"title": "Other", "url": "/1"}])
titles = items.values("title")
result = items.filter(lambda row: row.get("url")).unique("url").rename({"title": "name"})
records = result.to_list()
```

`append`、`insert`、`extend`、索引赋值与 `update_all` 修改原集合，加入的嵌套数据会复制。
`copy`、切片、`select`、`drop`、`filter`、`map`、`rename`、`unique` 返回嵌套数据独立的新 Items。
`to_list()` 和 `values()` 返回的值也与源记录隔离；直接取得 `items[0]` 则可以修改原记录。

`values(column, default=None)` 为每条记录取值，缺失字段使用 default，已有 None 值保持原样。
`filter(predicate)` 将副本交给回调，按真值保留原记录；`map(function)` 将副本交给回调并收集返回的记录映射。
两者都不因回调修改副本而改变源数据，回调异常直接传播。

`rename(mapping)` 同时执行重命名，允许字段互换，忽略不存在的源字段；一条记录中目标名称冲突时报错。
`unique(*fields)` 按指定字段联合去重，保留第一次出现的记录，缺少指定字段时报错；不传字段时比较整条记录。
去重支持嵌套列表和字典；不可哈希值使用逐项比较，大集合优先指定 URL 等可哈希字段。
可哈希性不改变相等语义，例如相等的 set 和 frozenset 字段也会合并，并保留首次出现的记录。

[上一章：架构与依赖](01-architecture.md) · [文档目录](README.md) · [下一章：爬虫下载](03-crawler-download.md)
