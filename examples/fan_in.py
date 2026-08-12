"""分支与汇聚：一个值产生两个输出，再由 ALL Node 合并。"""

from __future__ import annotations

from bricks import Graph, InputPolicy, Node, Output, Ports, Runtime


class Split(Node):
    input_ports = Ports(value=int)
    output_ports = Ports(left=int, right=int)

    def execute(self, inputs, context):
        del context
        value = inputs["value"]
        return Output(value, "left"), Output(value + 1, "right")


class Add(Node):
    input_ports = Ports(left=int, right=int)
    output_ports = Ports(total=int)
    input_policy = InputPolicy.ALL

    def execute(self, inputs, context):
        del context
        return Output(inputs["left"] + inputs["right"], "total")


def run(value: int = 2) -> int:
    """执行 Split → Add；Add 只在两个端口均到达后运行。"""

    graph = (
        Graph(entrypoint="split")
        .add(split=Split(), add=Add())
        .connect("split", "add", source_port="left", target_port="left")
        .connect("split", "add", source_port="right", target_port="right")
    )
    with Runtime() as runtime:
        runtime.register("fan-in", graph)
        return runtime.run("fan-in", value)[0].value


if __name__ == "__main__":
    print(run())
