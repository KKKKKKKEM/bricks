"""独立运行 CSS 和 JSON 批量解析示例，不下载页面或创建运行时。"""

import json

from bricks.parsers import (
    Collect,
    CssParser,
    JmesPathParser,
    Pipeline,
    Product,
    Rows,
    Rule,
)


def main() -> None:
    """提取内存商品记录，演示规格组合、父字段继承、连续提取和多源收集。"""
    html = """<main>
      <article><h2>笔记本</h2><a href="/products/1">详情</a><span>12.50</span></article>
      <article><h2>签字笔</h2><span>3.00</span></article>
    </main>"""
    records = CssParser().match(
        html,
        Rows(
            "article",
            {
                "name": Rule("h2", mode="first", options={"output": "text"}),
                "url": Rule(
                    "a", mode="first", default=None, options={"attribute": "href"}
                ),
                "price": Rule("span", mode="first", options={"output": "text"}),
            },
        ),
    )
    print(json.dumps(records, ensure_ascii=False))

    data = json.loads('{"products": [{"id": 1, "name": "笔记本"}]}')
    records = JmesPathParser().match(
        data, Rows("products", {"id": "id", "name": "name"})
    )
    print(json.dumps(records, ensure_ascii=False))

    catalog = {
        "products": [
            {"sku": "A", "colors": ["red", "blue"], "sizes": ["S", "M", "L"]},
            {"sku": "B", "colors": [], "sizes": ["XL"]},
        ]
    }
    variants = JmesPathParser().match(
        catalog,
        Rows(
            "products",
            Product(
                (
                    {"sku": "sku"},
                    Rows("colors", {"color": "@"}),
                    Rows("sizes", {"size": "@"}),
                ),
                keep_empty=True,
            ),
        ),
    )
    print(json.dumps(variants, ensure_ascii=False))

    page = """<main>
      <a data-phone="13800000000">联系</a>
      <script>{"phones": ["010-12345678", "400-1234567"]}</script>
    </main>"""
    phones = Collect(
        (
            Rule("a", options={"attribute": "data-phone"}),
            Pipeline(
                (
                    Rule("script", mode="first", options={"output": "text"}),
                    json.loads,
                    Rule("phones", parser=JmesPathParser()),
                )
            ),
        ),
        mode="concat",
    )
    contacts = CssParser().match(page, {"phones": phones})
    assert contacts == [{"phones": ["13800000000", "010-12345678", "400-1234567"]}]
    print(json.dumps(contacts, ensure_ascii=False))


if __name__ == "__main__":
    main()
