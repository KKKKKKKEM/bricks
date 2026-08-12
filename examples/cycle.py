"""循环 Graph：普通 Edge 回到上游，Node 停止反馈后自然结束。"""

from __future__ import annotations

from bricks import Graph, Node, Output, Ports, Runtime


class Counter(Node):
    input_ports = Ports(value=int)
    output_ports = Ports(again=int, done=int)

    def execute(self, inputs, context):
        del context
        value = inputs["value"]
        if value < 3:
            return Output(value + 1, "again")
        return Output(value, "done")


def run(value: int = 0) -> int:
    """沿自环递增，直到 Counter 从 done 端口退出。"""

    graph = (
        Graph(entrypoint="counter")
        .add(counter=Counter())
        .connect("counter", "counter", source_port="again", target_port="value")
    )
    with Runtime() as runtime:
        runtime.register("cycle", graph)
        return runtime.run("cycle", value)[0].value


if __name__ == "__main__":
    print(run())
