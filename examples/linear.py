"""线性 Graph：清理文本，然后转换为大写。"""

from __future__ import annotations

from bricks import Graph, Node, Output, Ports, Runtime


class Strip(Node):
    input_ports = Ports(text=str)
    output_ports = Ports(text=str)

    def execute(self, inputs, context):
        del context
        return Output(inputs["text"].strip(), "text")


class Upper(Node):
    input_ports = Ports(text=str)
    output_ports = Ports(result=str)

    def execute(self, inputs, context):
        del context
        return Output(inputs["text"].upper(), "result")


def run(text: str = "  hello, bricks  ") -> str:
    """执行一条两节点的数据流，并返回未连接的终端输出。"""

    graph = (
        Graph(entrypoint="strip")
        .add("strip", Strip())
        .add("upper", Upper())
        .connect("strip", "upper", source_port="text", target_port="text")
    )
    with Runtime() as runtime:
        runtime.register("linear", graph)
        return runtime.run("linear", text)[0].value


if __name__ == "__main__":
    print(run())
