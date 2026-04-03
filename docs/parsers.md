# 解析器

Bricks 内置了四种数据提取引擎，并提供了统一的 `Response.extract()` 接口以及更便捷的快捷方法。无论 API 返回 JSON，还是页面返回 HTML，都可以用一套规则声明式地完成数据提取。

---

## 快速参考

| 引擎 | 字符串名称 | 适用场景 | 语法参考 |
|---|---|---|---|
| `JsonExtractor` | `"json"` | JSON 响应，使用 JMESPath | [jmespath.org](https://jmespath.org) |
| `XpathExtractor` | `"xpath"` | HTML/XML 响应 | XPath 1.0 |
| `JsonpathExtractor` | `"jsonpath"` | JSON 响应，使用 JSONPath | [jsonpath-rw](https://github.com/kennknowles/python-jsonpath-rw) |
| `RegexExtractor` | `"regex"` | 任意文本，正则匹配 | Python `re` 模块 |

---

## `Response.extract()` — 批量结构化提取

这是最强大的提取接口，通过嵌套字典声明提取规则，一次返回结构化数据列表。

```python
def parse(self, context: Context):
    return context.response.extract(
        engine="json",       # 引擎名称（字符串）或提取器类
        rules=dict_or_list,  # 规则字典或规则列表
    )
```

### `rules` 结构

`rules` 是一个嵌套字典，键为**输出字段名**，值为**提取表达式**。

```python
# 最简单的形式：直接提取 JSON 字段
rules = {
    "data.list": {        # 先定位到 data.list 数组（作为循环对象）
        "id": "id",       # 从每个元素提取 id 字段
        "name": "name",   # 从每个元素提取 name 字段
        "score": "score",
    }
}
```

框架会自动对数组结构做展开，每个元素生成一条记录，返回 `List[dict]`。

---

## JSON 引擎（JMESPath）

Bricks 的 JSON 引擎底层使用 [jmespath-community](https://github.com/jmespath-community/python-jmespath) 实现，支持完整的 JMESPath 语法。

### `Response.get()` — 单表达式提取

```python
# 返回匹配结果（自动解析 JSON）
value = response.get("data.total")

# 不成功返回默认值
total = response.get_first("data.total", default=0)
```

### `extract()` 使用 JSON 引擎

```python
items = response.extract(
    engine="json",
    rules={
        "data.list": {
            "userId": "userId",
            "roomId": "roomId",
            "score": ("score", lambda x: round(float(x), 2)),  # 元组：表达式 + 后置处理
        }
    }
)
# 返回 [{"userId": ..., "roomId": ..., "score": ...}, ...]
```

### JMESPath 常用语法

```python
# 提取字段
response.get("data.name")              # → data.name 的值

# 提取数组元素
response.get("data.list[0].id")        # → 第一个元素的 id

# 过滤
response.get("data.list[?status==`1`]")  # → status=1 的元素

# 函数
response.get("length(data.list)")       # → 列表长度
response.get("keys(data)")              # → 字典的所有键

# 多字段投影
response.get("data.list[*].{id: id, name: name}")
```

---

## XPath 引擎

适用于 HTML 和 XML 响应，底层使用 `lxml`。

### `Response.xpath()` — 直接 XPath 提取

```python
# 返回匹配的元素列表
links = response.xpath("//a/@href")

# 返回第一个匹配
title = response.xpath_first("//title/text()", default="")

# 提取文本
items = response.xpath("//div[@class='item']/text()")
```

### `extract()` 使用 XPath 引擎

```python
items = response.extract(
    engine="xpath",
    rules={
        "//div[@class='list']/div": {  # 定位列表容器
            "title": ".//h2/text()",   # 从每个 div 提取 h2 文本
            "url": ".//a/@href",       # 提取 a 标签的 href
            "desc": ".//p/text()",
        }
    }
)
```

### 常用 XPath 表达式

```python
# 选取所有 h2 文本
"//h2/text()"

# 选取特定属性
"//a/@href"

# 条件过滤
"//div[@class='item']"

# 子节点
"//ul/li"

# 包含文本
"//div[contains(text(), '关键词')]"

# 获取外部 HTML
"//div[@class='content']"
```

---

## JSONPath 引擎

JSONPath 是 XPath 在 JSON 上的类比，适用于需要通配符、递归查找等复杂场景。

### `Response.jsonpath()` — 直接 JSONPath 提取

```python
# 获取所有 id
ids = response.jsonpath("$.data.list[*].id")

# 递归查找
all_names = response.jsonpath("$..name")

# 返回第一个
first_id = response.jsonpath_first("$.data.list[*].id")
```

### 常用 JSONPath 表达式

```python
# 根节点访问
"$.data.total"

# 所有元素
"$.data.list[*]"

# 递归查找（任意层级的 name）
"$..name"

# 条件过滤
"$.data.list[?(@.status==1)]"

# 数组切片
"$.data.list[0:3]"
```

---

## 正则引擎

适用于不规则文本，底层使用 Python `re.findall`。

### `Response.re()` — 正则匹配

```python
# 返回所有匹配组
phones = response.re(r"1[3-9]\d{9}")

# 返回第一个匹配
price = response.re_first(r"价格：(\d+\.?\d*)", default="0")

# 带分组
pairs = response.re(r"(\w+)=(\w+)")  # 返回元组列表
```

### `extract()` 使用正则引擎

```python
items = response.extract(
    engine="regex",
    rules={
        r"href=\"(/detail/\d+)\"": {  # 定位 detail 链接
            "url": r"(/detail/\d+)",
        }
    }
)
```

---

## Rule 对象 — 高级提取控制

对于需要精细控制的字段，可以使用 `Rule` 对象代替字符串表达式：

```python
from bricks.lib.extractors import Rule

rules = {
    "data.list": {
        # 基础：字符串表达式
        "id": "id",

        # 元组简写：(表达式, 后置处理函数)
        "price": ("price", lambda x: float(x) if x else 0.0),

        # 元组简写：(表达式, 默认值)
        "name": ("name", "未知"),

        # Rule 对象：完整控制
        "score": Rule(
            exprs="score",
            default=0,
            post_script=lambda x: round(float(x), 2),
            is_array=False,     # False 强制返回单个值（不是列表）
            condition=lambda obj: "score" in obj,  # 条件：满足时才提取
        ),

        # 常量字段
        "source": Rule(const="bricks_spider"),

        # 内置特殊字段
        "@index": "@index",   # 当前元素的索引（从0开始）
        "@ts": "@ts",         # 提取时的时间戳
        "@date": "@date",     # 提取时的日期时间字符串

        # 展开字段（将 dict 的 k/v 合并到当前记录）
        "@unpack": "extra_info",
    }
}
```

---

## Rule.Group — OR 条件

当一个字段可能来自多个可能的路径时，使用 `Group`：

```python
from bricks.lib.extractors import Rule

# 先尝试 name，找不到再尝试 title，都找不到用 "未知"
field = Rule(exprs="name") | Rule(exprs="title", default="未知")

rules = {
    "data.list": {
        "name": field,
    }
}
```

---

## 多规则提取（`extract_all`）

当同一响应需要按多种规则提取时：

```python
# 传入规则列表，逐一提取并 yield
for result in response.extract_all(
    engine="json",
    rules=[
        {"data.users": {"id": "id", "name": "name"}},
        {"data.admins": {"id": "id", "role": "role"}},
    ]
):
    print(result)
```

---

## 在 `form.Spider` 中使用解析规则

`form.Spider` 的 `Parse` 节点可以直接引用引擎和规则：

```python
form.Parse(
    func="json",                        # 引擎名称
    kwargs={
        "rules": {
            "data.list": {
                "id": "id",
                "name": "name",
            }
        }
    },
)
```

---

## 响应对象速查

| 方法 | 说明 | 返回类型 |
|---|---|---|
| `response.text` | 响应文本（自动解码）| `str` |
| `response.json()` | 反序列化 JSON | `dict` / `list` |
| `response.html` | lxml HTML 对象 | `lxml._Element` |
| `response.get(rule)` | JMESPath 提取 | `Any` |
| `response.get_first(rule, default)` | JMESPath 第一个 | `Any` |
| `response.xpath(xpath)` | XPath 提取 | `list` |
| `response.xpath_first(xpath, default)` | XPath 第一个 | `Any` |
| `response.jsonpath(jpath)` | JSONPath 提取 | `list` |
| `response.jsonpath_first(jpath, default)` | JSONPath 第一个 | `Any` |
| `response.re(regex)` | 正则提取 | `list` |
| `response.re_first(regex, default)` | 正则第一个 | `str` |
| `response.extract(engine, rules)` | 结构化批量提取 | `List[dict]` |
| `response.extract_all(engine, rules)` | 多规则批量提取 | `Generator` |
| `response.ok` | 状态码 2xx/3xx | `bool` |
| `response.status_code` | HTTP 状态码 | `int` |
| `response.length` | 响应体字节长度 | `int` |
