# 第六章：独立解析与批量规则

`bricks.parsers` 直接处理内容，不依赖 Response、Items 或 Interlace 执行上下文。
既可执行单个表达式，也可用 `match` 将多组字段规则转换成普通 `list[dict]`。

```bash
uv run python -m examples.crawler_parse
```

完整代码见[独立解析示例](../examples/crawler_parse.py)。

## 单表达式提取

```python
import json

from bricks.parsers import CssParser, JmesPathParser, JsonPathParser, RegexParser, XPathParser

html = '<article><h2>笔记本</h2><a href="/1">详情</a></article>'
css = CssParser()
document = css.prepare(html)
elements = css.extract(document, "article")
titles = css.extract(document, "h2", output="text")
href = css.extract_first(document, "a", attribute="href", default=None)

count = XPathParser().extract(document, "count(//article)")
prices = RegexParser().extract("price=12 price=3", r"price=(\d+)", group=1)

data = json.loads('{"products": [{"id": 1}]}')
ids = JmesPathParser().extract(data, "products[*].id")
matches = JsonPathParser().extract(data, "$.products[*].id")
```

`prepare()` 可将文档文本准备为可复用对象；它必须接受自身的准备结果。
内置 HTML/XML 查询也接受原始文本或字节，重复查询时建议显式准备一次。
JSON 解析器直接接收已解码值；字符串始终是字符串值，`extract("hello", "$")`
在 JSONPath 中得到 `["hello"]`，不会尝试 JSON 解码。
JSONPath 的 `prepare()`、单表达式及批量入口只接收 dict、list、str、int、float、bool 或 None 根值，
文件对象、提供 `read` 接口的对象和字节输入抛出 TypeError，拒绝时不会读取或移动文件游标。
容器内容不递归校验，也不复制；文件和字节形式的 JSON 应由调用方先显式解码。

| 解析器 | 输入与结果 | 选项 |
|---|---|---|
| `CssParser` | HTML/XML 文本、字节或 lxml 元素，结果为列表 | `output="element"/"text"/"html"`、`attribute`、`namespaces` |
| `XPathParser` | 同上，保留列表、文本、数字、布尔结果 | `namespaces`、`variables` |
| `JmesPathParser` | 已解码 JSON 值，保留查询结果 | `options` 接收原生 `jmespath.Options` |
| `JsonPathParser` | 已解码 JSON 值，始终返回命中值列表 | 无；固定严格模式 |
| `RegexParser` | 已解码文本，始终返回列表 | `flags`、`group` |

`extract_first()` 只接受列表结果，空列表返回 `default`，默认 None。
它不会将标量包装成列表，也不会将字符串当字符序列。直接读取 XPath 标量或 JMESPath
投影结果时用 `extract()`。所有表达式必须是非空字符串，未知选项报错。

CSS 默认返回元素。`output="text"` 读取元素及后代文本，保留空白；`output="html"`
序列化元素自身，不包括其后面的 tail 文本。`attribute="href"` 按命中顺序读取属性，
缺失属性以 `MISSING` 保持原位置；它不能与非默认 output 同用。
不支持 `::text`、`::attr()`，选择器范围遵循 [cssselect/lxml 文档](https://lxml.de/cssselect.html)，
不承诺浏览器所有 CSS 选择器。需要 HTML 容错修复时使用默认 `format="html"`；空文档报错。

XML 显式使用 `XPathParser(format="xml")` 或 `CssParser(format="xml")`，不根据内容猜测格式。
命名空间通过前缀映射传入。XML 禁用外部 DTD 加载，并在返回文档或执行查询前拒绝所有 DOCTYPE，
抛出 ValueError，避免内部实体在 XPath 文本查询或属性读取时展开。
传入现成元素时也检查其所属文档；调用方在外部解析、复制或修改文档前的行为由调用方负责。
标准实体（如 `&amp;`）与数字字符引用仍正常解析，格式错误直接抛出原生异常。
字节输入默认遵循文档声明，需要覆盖时在构造器传 `encoding`；已解码 XML 字符串中的编码声明
受 lxml 原生限制，带编码声明的原文建议传 bytes。输入字符串始终作为内容，不读取文件或 URL。

正则默认 `group=0` 返回完整匹配，即使表达式存在捕获组也不改变形状。
`group=1` 或名称读取指定组，`group=None` 返回命名组字典；未参与匹配的可选组为 None。
不存在的组即使没有匹配也报错。字节、字典和 HTML 元素需要调用方显式转换成文本。

## match：批量记录提取

`match(source, rules, parser=parser)` 与内置解析器的 `parser.match(source, rules)` 等价。

- 字段映射生成一条记录；裸字符串表示当前解析器的表达式。
- `Rows(select, fields)` 在 select 明确选中的每个对象上执行 fields 记录规则。
- `Product(branches)` 将各分支的记录做笛卡尔积，并按字段合并。
- 多组规则序列在根、Rows 内和 Product 分支内都按声明顺序拼接结果，不做候选回退。
- 空序列返回 `[]`，空字段映射返回 `[{}]`。

```python
from bricks.parsers import CssParser, Rows, Rule, XPathParser

records = CssParser().match(
    html,
    [
        {"total": Rule("count(//article)", parser=XPathParser())},
        Rows("article", {
            "title": Rule("h2", mode="first", options={"output": "text"}),
            "url": Rule("a", mode="first", default=None, options={"attribute": "href"}),
        }),
    ],
)
```

这里第一组生成总数记录，第二组生成商品记录。数组字段不会因为只有一个元素变成标量。
需要首项时明确使用 `Rule(..., mode="first")`，否则默认 `mode="value"` 保留原始结果。

JSON 批量提取：

```python
from bricks.parsers import JmesPathParser, JsonPathParser, Rows, Rule

records = JmesPathParser().match(
    data,
    Rows("products", {"id": "id"}),
)

records = JsonPathParser().match(
    data,
    Rows("$.products[*]", {"id": Rule("$.id", mode="first")}),
)
```

JSONPath 的 `$.products` 命中的是数组自身，结果为 `[products]`；`$.products[*]` 才是
每个数组成员组成的列表。也可用 `Rows(Rule("$.products", mode="first"), ...)`
显式取出该数组。`Rows` 对标量或 None 报错，不隐式包装；空列表或 MISSING 生成零条记录。

## 字段规则与缺失

`Rule` 的顺序是：`when` 检查 → `before` 转换当前源 → 提取表达式 → `mode` 数量选择
→ `missing` 判定 → 必填/default → `transform` 结果转换。

| 配置 | 契约 |
|---|---|
| `parser` | 当前规则使用的解析器实例，默认继承当前解析器，不传字符串名称 |
| `when` | 返回假时省略该字段，不执行默认值、必填检查或转换 |
| `before` | 显式转换源，可用于切换到另一种解析器接受的输入 |
| `mode` | `value` 保留结果；`first` 要求列表，空列表转为 MISSING |
| `missing` | 可选谓词，将已有值显式判为缺失 |
| `default` | 缺失时复制默认值，不覆盖已有的 None、空集合或假值 |
| `required` | 缺失抛出 MissingValueError，不能同时指定 default |
| `transform` | 对取值或默认值转换，异常原样传播 |
| `options` | 传给该解析器 extract 的选项映射 |

`MISSING` 是缺失标记，字段最终为 MISSING 时省略。与之不同，None、`0`、False、`""`、
`[]` 都是合法值。`first` 空列表的缺失与 `first` 取得的 None 不同。

[JMESPath 语言](https://jmespath.org/tutorial.html)将字段不存在和显式 null 都表示为 None，
适配器不会虚构两者的区别。默认保留 None；需要回退时显式声明
`missing=lambda value: value is None`。需要区分两者可使用 JSONPath 的 `[]` 与 `[None]`。
JSONPath 使用 [python-jsonpath 严格模式](https://jg-rp.github.io/python-jsonpath/syntax/)，
不沿用 dev 分支旧 jsonpath 包的扩展语法或 eval 选项。

```python
from bricks.parsers import Constant, Group, JmesPathParser, Rule

records = JmesPathParser().match(
    {"price": "12.50"},
    {
        "price": Rule("price", transform=str.strip),
        "name": Group((
            Rule("display_name", missing=lambda value: value is None),
            Rule("name", missing=lambda value: value is None),
        ), default="未命名"),
        "source": Constant("catalog"),
    },
)
```

`Group` 按序返回第一个非缺失候选，接受显式值规则：Rule、Constant、Group、Pipeline 和 Collect；
候选自己的 default 也可成为结果，各候选都读取同一当前源。
全部缺失时使用 Group 的 default 或 required 策略。候选的语法错误、必填失败、回调异常和
取消均继续传播，不能触发隐式回退。不接受 `(表达式, 函数或默认值)` 元组简写。

## Pipeline：连续提取

`Pipeline(steps)` 将上一步的结果直接传给下一步，适合跨格式的连续提取：

```python
import json

from bricks.parsers import CssParser, JmesPathParser, Pipeline, Rule

html = '<script type="application/json">{"product": {"name": "笔记本"}}</script>'
name = Pipeline((
    Rule('script[type="application/json"]', mode="first", options={"output": "text"}),
    json.loads,
    Rule("product.name", parser=JmesPathParser()),
    str.strip,
))
records = CssParser().match(html, {"name": name})
assert records == [{"name": "笔记本"}]
```

步骤可以是 Rule、Constant、Group、Pipeline、Collect，或接收一个值的同步转换函数。
空链报错，不接受裸表达式、字段映射、Rows 或 Product 作为步骤。JSON 解码、文本清理、类型转换
均由函数显式表达，不按内容猜测格式，也不会自动对列表逐项执行下一步。

`match` 仍先使用调用处解析器准备输入，链的第一步接收当前准备好的文档或行对象。
链中的每条 Rule 都只覆盖自己的解析器；未指定 parser 的后续规则仍使用调用处默认解析器，
不会沿用前一步的覆盖配置。Pipeline 不保存当前源，也不复制输入或中间结果。

每一步正常完成后才进入下一步；结果为 MISSING 时终止并省略该字段，后续函数不再调用。
None、空列表和其他假值仍传给下一步。单条 Rule 的 default 先执行，得到有效默认值后链仍继续。
需要对整条链设置默认值或必填约束时，用 `Group((name,), default="未命名")`
或 `Group((name,), required=True)`；必填错误、解码错误和回调异常不触发回退。

## Collect：多规则收集

`Collect(rules, mode="values")` 在同一当前源上按声明顺序执行所有成员，每个成员执行一次。
成员接受显式值规则，支持嵌套 Pipeline、Group 和 Collect；函数步骤需放入 Pipeline。

```python
import json

from bricks.parsers import Collect, CssParser, JmesPathParser, Pipeline, Rule, XPathParser

html = '''<main>
  <a data-phone="13800000000">联系</a>
  <meta name="phone" content="010-12345678">
  <script>{"phones": ["400-1234567", "13800000000"]}</script>
</main>'''
phones = Collect((
    Rule("a", options={"attribute": "data-phone"}),
    Rule('//meta[@name="phone"]/@content', parser=XPathParser()),
    Pipeline((
        Rule("script", mode="first", options={"output": "text"}),
        json.loads,
        Rule("phones", parser=JmesPathParser()),
    )),
), mode="concat")
records = CssParser().match(html, {"phones": phones})
assert records == [{
    "phones": ["13800000000", "010-12345678", "400-1234567", "13800000000"],
}]
```

| 模式或边界 | 行为 |
|---|---|
| `mode="values"`，默认 | 各成员结果作为一个元素；`[1, 2]` 与 `[3]` 收集成 `[[1, 2], [3]]` |
| `mode="concat"` | 有效结果必须为 list，以上结果变为 `[1, 2, 3]`；仅拼接一层 |
| 整个成员返回 MISSING | 跳过该成员，继续后续规则 |
| None、False、0、空字符串 | values 保留；concat 因非列表报 TypeError |
| 成员返回空列表 | values 保留为一个 `[]` 元素；concat 不增加元素 |
| 列表内部的 MISSING | 保留原位置，不过滤；例如 CSS 缺失属性的占位 |
| 零成员或全部成员缺失 | 返回 `[]`，这是有效值，不触发外层 Group 回退 |
| 重复值、嵌套列表 | 不去重、不递归展开 |
| 任一成员失败 | 立即传播异常，后续成员不执行，不返回部分结果 |

需要去重、过滤或汇总后的转换时，在外层 Pipeline 显式添加函数步骤。
需要空收集结果触发 Group 回退时，也可在外层 Pipeline 用函数将 `[]` 显式转为 MISSING。
Collect 新建外层列表，但不深复制各成员提取出的文档节点和 JSON 容器；这些值仍归调用方管理。
各成员共享当前源，修改输入的回调会影响后续成员，不提供隔离副本或并行执行。

## 值规则与记录规则组合

Pipeline 中的 Collect 读取前一步的结果；Collect 中的每条 Pipeline 独立从同一当前源开始。
Group 可对整条链做候选回退，也可作为链中的一步。三者职责不同：链传递值，候选取首个有效值，
收集保留所有有效结果；Product 则组合各分支的记录，普通规则序列仍表示记录拼接。

`Rows.select` 接受表达式或值规则，因此可以先解码脚本，再展开其中的商品：

```python
import json

from bricks.parsers import CssParser, JmesPathParser, Pipeline, Rows, Rule

html = '<script>{"products": [{"id": "A"}, {"id": "B"}]}</script>'
products = Pipeline((
    Rule("script", parser=CssParser(), mode="first", options={"output": "text"}),
    json.loads,
    Rule("products"),
))
records = CssParser().match(
    html,
    Rows(products, {"id": "id"}, parser=JmesPathParser()),
)
assert records == [{"id": "A"}, {"id": "B"}]
```

这里 Rows 的默认解析器为 JMESPath，只有选取脚本的那一步使用 CSS。
最终选择仍必须是 list，MISSING 产生零条记录，标量报错；之后的 fields 可继续使用嵌套映射、
Rows 和 Product。在字段位置使用 Pipeline 或 Collect 时，结果结构照常保留，不自动展开成记录。

## 嵌套记录

字段中的映射在同一当前源上生成嵌套字典；字段中的 Rows 或 Product 则生成嵌套记录列表：

```python
records = JmesPathParser().match(
    {"orders": [{"id": 1, "lines": [{"sku": "A"}, {"sku": "B"}]}]},
    Rows("orders", {
        "id": "id",
        "summary": {"line_count": "length(lines)"},
        "lines": Rows("lines", {"sku": "sku"}),
    }),
)
```

`Rows(parser=...)` 设置该层行选择及字段的默认解析器；select 本身的 `Rule(parser=...)`
只影响选择，不改变字段默认解析器。跨格式输入必须显式转换，不能把节点或字典隐式转成文本。
普通字段中的多个嵌套 Rows 各自生成列表。需要扁平记录、笛卡尔组合或继承父字段时，
在记录规则位置显式使用 Product；不引入时间、索引或解包魔法字段。

## 笛卡尔组合与父子展开

`Product` 的分支都在**同一当前源**上提取，每个分支可以是字段映射、Rows、另一个 Product
或规则拼接序列。各分支分别产生记录列表，再枚举组合并合并字段。最右分支变化最快，
保留源中的重复记录，不去重；两个分支分别产生 2 条和 3 条记录时，结果为 6 条。

```python
from bricks.parsers import JmesPathParser, Product, Rows

data = {"colors": ["red", "blue"], "sizes": ["S", "M", "L"]}
records = JmesPathParser().match(data, Product((
    Rows("colors", {"color": "@"}),
    Rows("sizes", {"size": "@"}),
)))
```

结果依次为 red/S、red/M、red/L、blue/S、blue/M、blue/L 六条记录。
将上述两个 Rows 放在普通规则列表中则是拼接，得到 2 + 3 条，而非 2 × 3 条。

`Rows.fields` 支持完整的记录规则，因此可在每个父对象的作用域内组合父字段和子记录：

```python
orders = {"orders": [
    {"id": "o1", "lines": [{"sku": "A"}, {"sku": "B"}]},
    {"id": "o2", "lines": []},
]}
records = JmesPathParser().match(orders, Rows("orders", Product((
    {"order_id": "id"},
    Rows("lines", {"sku": "sku"}),
), keep_empty=True)))
```

输出是 `[{"order_id": "o1", "sku": "A"}, {"order_id": "o1", "sku": "B"}, {"order_id": "o2"}]`。
父字段与子 Rows 都在当前订单上求值，下一张订单不会与上一张订单的子记录交叉。
更多层级直接继续嵌套 `Rows(..., Product(...))`；每层先完成自己的子记录，再与外层父字段合并。
父字段不依靠字典遍历、索引字符串前缀或隐式上下文继承。

| 配置或边界 | 行为 |
|---|---|
| `keep_empty=False`，默认 | 任一分支没有记录时，整个乘积为 `[]` |
| `keep_empty=True` | 空分支作为 `[{}]` 参与组合，保留其他字段，不为缺失列补 None |
| 全部分支为空且 keep_empty=True | 产生 `[{}]` |
| `Product(())` | 零个分支的乘积为 `[{}]`；与空拼接序列 `[]` 不同 |
| `on_conflict="raise"`，默认 | 同名字段报 ValueError，即使值相同也报错，错误包含字段名和分支位置 |
| `on_conflict="first"` | 保留靠前分支的同名字段 |
| `on_conflict="last"` | 使用靠后分支的同名字段 |

冲突仅按记录顶层字段名判断；同名嵌套字典作为整个字段保留或替换，不递归合并。
已省略的 MISSING 字段不参与冲突，已有的 None、False、0 和空集合仍是有效字段。
需要子记录覆盖父字段时显式设置 `on_conflict="last"`。

空分支是零条记录 `[]`，不等同于存在一条无字段记录 `[{}]`。Rows 选中 None 等非列表值时仍报错，
`keep_empty=True` 不会掩盖选择类型错误。JMESPath 需要将缺失的可选子列表视为空时，可显式写
``Rows("lines || `[]`", ...)``，或通过 Rule 的 missing/default 策略处理。
不同 Product 的策略互不继承：仅让某个分支可选时，可以把它包在单分支
`Product((optional_rows,), keep_empty=True)` 中，外层仍保留默认的空分支行为。

各分支按声明顺序执行一次，然后使用标准库 `itertools.product` 枚举最终组合，不重复执行字段转换，
也不累计逐层增长的中间乘积。空分支不会使其他分支被跳过，已执行的语法、转换、必填及取消异常
继续原样传播。每条合并结果单独深复制，避免复制到多条记录的父字段、默认值或常量相互污染；
不能深复制的自定义值会直接报错，不退回共享引用。

match 仍返回完整内存列表，结果规模是分支记录数之积，没有自动截断或磁盘溢写。

## 扩展解析器

公共 `Parser` 是结构化 Protocol，只有两个方法：

```python
from typing import Any

from bricks.parsers import MISSING, Parser, match


class MappingParser:
    """演示一个无需继承的字典键解析器。"""

    def prepare(self, source: Any) -> Any:
        """直接使用调用方提供的映射。

        Args:
            source: 待查询映射。

        Returns:
            原映射。
        """
        return source

    def extract(self, source: Any, expression: str, **options: Any) -> Any:
        """按键查询，缺失时返回公共标记。

        Args:
            source: 待查询映射。
            expression: 非空字段名。
            **options: 不支持附加选项。

        Returns:
            字段值或 MISSING。

        Raises:
            TypeError: 传入附加选项。
            ValueError: 字段名为空。
        """
        if options:
            raise TypeError("unsupported options")
        if not expression.strip():
            raise ValueError("expression must not be empty")
        return source.get(expression, MISSING)


parser: Parser = MappingParser()
records = match({"id": 1}, {"id": "id"}, parser=parser)
```

第三方实现不必继承内置解析器；需要 `.match()`、`.extract_first()` 便捷方法时可选择继承
`BaseParser`。所有内置解析器都通过相同协议进入公共规则执行器，不存在按具体引擎分支的规则代码。

解析器实例不保存当前文档。字段映射在执行前验证，Rows/Product 在构造时递归固定规则结构；
Group、Pipeline 和 Collect 在构造时固定成员序列，Rule 不会在调用中绑定引擎。
options、default、Constant 的构造输入被复制，默认值及常量在每条记录中独立复制。
冻结对象不意味着其中任意用户对象都递归只读：不要在并发执行中修改规则保存的嵌套配置、文档或回调状态。
单表达式或普通字段映射返回的节点及 JSON 容器可能引用输入对象，调用方决定是否复制；
Product 则对每条合并结果深复制。

本模块提供内存文档查询，不提供流式解析、网络抓取、异步 CPU 隔离或自动数据存储。
要得到 Items，可由调用方显式构造；解析器本身保持独立。

[上一章：完整爬取示例](05-crawler-pipeline.md) · [文档目录](README.md)
