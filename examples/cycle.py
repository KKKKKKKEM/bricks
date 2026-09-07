"""循环 Graph：普通 Edge 回到上游，Node 停止反馈后自然结束。"""

from __future__ import annotations

from bricks import Graph, Node, Output, Ports, Runtime


class Counter(Node):
    """通过回边递增计数并在达到边界时结束的示例节点。

    Attributes:
        input_ports: 节点声明的输入端口及其类型。
        output_ports: 节点声明的输出端口及其类型。
    """

    input_ports = Ports(value=int)
    output_ports = Ports(again=int, done=int)

    def execute(self, inputs, context):
        """根据当前输入执行节点行为，并返回声明端口上的输出。

        Args:
            inputs: 入口数据或按端口名称组织的输入映射。
            context: 当前调用的执行或插件上下文。

        Returns:
            未达到边界时返回继续循环的 Output，否则返回终端结果 Output。
        """

        del context
        value = inputs["value"]
        if value < 3:
            return Output(value + 1, "again")
        return Output(value, "done")


def run(value: int = 0) -> int:
    """沿自环递增，直到 Counter 从 done 端口退出。

    Args:
        value: 当前操作处理的输入值。

    Returns:
        当前记录携带的数据值。
    """

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
