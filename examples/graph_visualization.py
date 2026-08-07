"""演示 Graph 的 JSON 描述、Mermaid 和 DOT 输出。"""

import json

from bricks import GraphBuilder


builder = GraphBuilder("visualization", initial="start")
builder.action("start")
builder.terminal("done")
builder.transition("start", "finish", "done")
graph = builder.build()

description = graph.describe()
assert json.loads(json.dumps(description))["id"] == "visualization"
assert "graph TD" in graph.to_mermaid()
assert 'digraph "visualization"' in graph.to_dot()

print(json.dumps(description, ensure_ascii=False, indent=2))
print(graph.to_mermaid())
print(graph.to_dot())
