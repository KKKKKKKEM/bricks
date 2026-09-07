"""分支与汇聚：一个值产生两个输出，再由 ALL Node 合并。"""

from __future__ import annotations

from bricks import Graph, InputPolicy, Node, Output, Ports, Runtime


class Split(Node):
    """将同一输入分发到两条计算分支的示例节点。

    Attributes:
        input_ports: 节点声明的输入端口及其类型。
        output_ports: 节点声明的输出端口及其类型。
    """

    input_ports = Ports(value=int)
    output_ports = Ports(left=int, right=int)

    def execute(self, inputs, context):
        """根据当前输入执行节点行为，并返回声明端口上的输出。

        Args:
            inputs: 入口数据或按端口名称组织的输入映射。
            context: 当前调用的执行或插件上下文。

        Returns:
            分别从 left 和 right 端口发送的两项 Output。
        """

        del context
        value = inputs["value"]
        return Output(value, "left"), Output(value + 1, "right")


class Add(Node):
    """等待两路输入并输出相加结果的示例节点。

    Attributes:
        input_ports: 节点声明的输入端口及其类型。
        output_ports: 节点声明的输出端口及其类型。
        input_policy: 仅依据端口和 token 数量生效的输入策略。
    """

    input_ports = Ports(left=int, right=int)
    output_ports = Ports(total=int)
    input_policy = InputPolicy.ALL

    def execute(self, inputs, context):
        """根据当前输入执行节点行为，并返回声明端口上的输出。

        Args:
            inputs: 入口数据或按端口名称组织的输入映射。
            context: 当前调用的执行或插件上下文。

        Returns:
            total 端口上左右输入之和的 Output。
        """

        del context
        return Output(inputs["left"] + inputs["right"], "total")


def run(value: int = 2) -> int:
    """执行 Split → Add；Add 只在两个端口均到达后运行。

    Args:
        value: 当前操作处理的输入值。

    Returns:
        当前记录携带的数据值。
    """

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
