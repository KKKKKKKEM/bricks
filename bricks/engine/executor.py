"""只负责执行单张 Graph 的内核。"""

from __future__ import annotations

from collections import deque
from collections.abc import Callable, Iterable, Iterator, Mapping, MutableMapping
from types import MappingProxyType
from typing import Any

from .core import (
    Node,
    Output,
    require_non_empty_string,
)
from .errors import (
    ExecutionControlError,
    ExecutionError,
    HookExecutionError,
    IncompleteInputsError,
    InvalidOutputError,
    PortValueTypeError,
)
from .events import Context, Event
from .execution import Execution, ExecutionStatus
from .graph import ExecutionPlan, Graph
from .hooks import (
    HookHandle,
    HookPhase,
    HookRegistry,
    NodeCall,
    NodeHook,
    Outputs,
    ShortCircuit,
    StopGraph,
)
from .observation import ObservationHub, RuntimeEvent, RuntimeEventKind
from .runner import LocalRunner
from .slots import Slot

Emit = Callable[[Event], None]


class Engine:
    """执行 Graph 内的 Node、InputPolicy、Output 和 Edge。

    Attributes:
        hooks: 节点 Hook 的注册与快照能力。
        _runner: 解析同步值和异步结果的运行器。
        _observations: 当前组件的只读生命周期事件分发中心。
    """

    def __init__(
        self,
        *,
        hooks: HookRegistry | None = None,
        runner: LocalRunner | None = None,
        observations: ObservationHub | None = None,
    ) -> None:
        """组装动态 Hook 注册表和同步/异步调用 Runner。

        Args:
            hooks: 待装配的节点 Hook 集合。
            runner: 负责解析同步值和异步结果的调用运行器。
            observations: 负责分发生命周期事件的观察中心。
        """

        self.hooks = HookRegistry() if hooks is None else hooks
        self._runner = LocalRunner() if runner is None else runner
        self._observations = ObservationHub() if observations is None else observations

    def execute(
        self,
        name: str,
        graph: Graph,
        inputs: Any,
        emit: Emit,
        plan: ExecutionPlan | None = None,
        *,
        slot: Slot | None = None,
        execution: Execution,
    ) -> None:
        """执行一张 Graph，通过统一 Execution 输出接口交付终端 Output。

        Args:
            name: 注册或查找使用的名称。
            graph: 目标 Graph 定义或其注册名称，以类型声明为准。
            inputs: 入口数据或按端口名称组织的输入映射。
            emit: 发布跨图事件的回调。
            plan: 限定本次执行范围的计划，None 使用完整 Graph。
            slot: 当前逻辑执行链使用的本地执行槽。
            execution: 记录当前执行状态、控制限制及输出的句柄。

        Raises:
            RuntimeError: 当前生命周期状态或操作顺序不允许此操作。
            TypeError: 参数类型或接口实现不符合当前契约。
            ValueError: 参数值或字段组合不合法。
        """

        name = require_non_empty_string(name, "graph name")
        if not isinstance(graph, Graph):
            raise TypeError("graph must be a Graph")
        if not callable(emit):
            raise TypeError("emit must be callable")
        if slot is not None and not isinstance(slot, Slot):
            raise TypeError("slot must be a Slot or None")
        if not isinstance(execution, Execution):
            raise TypeError("execution must be an Execution")
        elif execution.graph != name:
            raise ValueError("execution belongs to a different registered Graph")
        if not graph.frozen:
            raise RuntimeError("Engine requires a frozen Graph")
        if plan is not None:
            if not isinstance(plan, ExecutionPlan):
                raise TypeError("plan must be an ExecutionPlan")
            if plan.graph is not graph:
                raise ValueError("execution plan belongs to a different Graph")
        if execution.status is not ExecutionStatus.RUNNING:
            raise RuntimeError("execution must be running")
        prepared = self._coerce_inputs(graph, inputs)
        snapshot = self.hooks.snapshot(name)
        self._run(name, graph, prepared, emit, snapshot, plan, slot, execution)

    def close(self) -> None:
        """关闭 Hook 注册表和后台异步 Runner。"""

        self.hooks.close()
        self._runner.close()

    def attach(
        self,
        hook: NodeHook | Callable[..., object],
        *,
        phase: HookPhase | str | None = None,
        graph: str | None = None,
        node: str | None = None,
    ) -> HookHandle:
        """声明并实现 GraphExecutor 的可选动态 Hook 能力。

        Args:
            hook: 节点 Hook 对象或单阶段回调。
            phase: 函数 Hook 对应的执行阶段。
            graph: 目标 Graph 定义或其注册名称，以类型声明为准。
            node: 节点实例或作用域中的节点 ID，以接口类型为准。

        Returns:
            用于卸载本次注册的句柄。
        """

        return self.hooks.attach(hook, phase=phase, graph=graph, node=node)

    def _run(
        self,
        graph_name: str,
        graph: Graph,
        initial_inputs: Mapping[str, Any],
        emit: Emit,
        hook_snapshot: tuple[Any, ...],
        plan: ExecutionPlan | None,
        slot: Slot | None,
        execution: Execution,
    ) -> None:
        """推进 Graph 的可执行节点，直至数据流静止或执行终止。

        Args:
            graph_name: 执行或观测记录中的 Graph 注册名称。
            graph: 目标 Graph 定义或其注册名称，以类型声明为准。
            initial_inputs: 执行开始时注入入口节点的数据。
            emit: 发布跨图事件的回调。
            hook_snapshot: 本次 Graph 执行固定使用的 Hook 注册快照。
            plan: 限定本次执行范围的计划，None 使用完整 Graph。
            slot: 当前逻辑执行链使用的本地执行槽。
            execution: 记录当前执行状态、控制限制及输出的句柄。

        Raises:
            HookExecutionError: Hook 的调用或返回结果违反约束。
            IncompleteInputsError: 数据流静止时仍有无法组合的输入。
            InvalidOutputError: 节点输出不符合 Output 契约。
            PortValueTypeError: 实际输入或输出值不符合端口类型。
        """

        nodes = graph.nodes
        active_nodes = set(nodes) if plan is None else plan.nodes
        specs = {
            node_id: graph.spec_for(node_id)
            for node_id in nodes
            if node_id in active_nodes
        }
        input_ports = {
            node_id: tuple(spec.input_ports) for node_id, spec in specs.items()
        }
        hooks_by_node = {
            node_id: self.hooks.for_node(hook_snapshot, node_id)
            for node_id in active_nodes
        }
        queues: dict[str, dict[str, deque[Any]]] = {
            node_id: {port: deque() for port in specs[node_id].input_ports}
            for node_id in nodes
            if node_id in active_nodes
        }
        outgoing_for = graph.outgoing_for if plan is None else plan.outgoing_for
        for port, value in initial_inputs.items():
            queues[graph.entrypoint][port].append(value)
        started: set[str] = set()
        ready: deque[str] = deque((graph.entrypoint,))
        scheduled = {graph.entrypoint}
        local_state: dict[tuple[str, str], MutableMapping[str, Any]] = {}
        finalizers: list[Callable[[], None]] = []

        def schedule_if_ready(node_id: str) -> None:
            """根据已绑定输入策略，将具备可消费数据的节点加入就绪队列。

            Args:
                node_id: Graph 内绑定的节点 ID。
            """

            if node_id in scheduled:
                return
            spec = specs[node_id]
            if spec.input_policy.on_start:
                runnable = node_id == graph.entrypoint and node_id not in started
            else:
                runnable = (
                    spec.input_policy.select(input_ports[node_id], queues[node_id])
                    is not None
                )
            if runnable:
                ready.append(node_id)
                scheduled.add(node_id)

        while ready:
            execution.checkpoint()
            node_id = ready.popleft()
            scheduled.remove(node_id)
            node = nodes[node_id]
            spec = specs[node_id]
            policy = spec.input_policy
            if policy.on_start:
                started.add(node_id)
                consumed: dict[str, Any] = {}
            else:
                selection = policy.select(input_ports[node_id], queues[node_id])
                if selection is None:
                    continue
                consumed = {port: queues[node_id][port].popleft() for port in selection}

            context = Context(
                emit,
                slot,
                checkpoint=execution.checkpoint,
                is_cancelled=lambda: execution.cancel_requested,
                local=local_state,
                finalizers=finalizers,
                scope=node_id,
            )
            self._observations.publish(
                RuntimeEvent(
                    RuntimeEventKind.NODE_STARTED,
                    graph=graph_name,
                    execution_id=execution.id,
                    node=node_id,
                    attributes={"step": execution.steps + 1},
                )
            )
            node_error: BaseException | None = None
            try:
                with execution.step(node_id):
                    try:
                        outputs = self._call_node(
                            graph_name,
                            node_id,
                            node,
                            MappingProxyType(consumed),
                            context,
                            hooks_by_node[node_id],
                            spec.input_ports,
                            execution,
                        )
                    except ShortCircuit as exc:
                        raise HookExecutionError(
                            "ShortCircuit is only valid during hook enter",
                            graph=graph_name,
                            node=node_id,
                        ) from exc
            except StopGraph as signal:
                outputs = self._coerce_outputs(
                    signal.outputs, graph_name, None, "StopGraph"
                )
                for output in outputs:
                    execution.publish_output(output)
                return
            except BaseException as exc:
                node_error = exc
                raise
            finally:
                self._observations.publish(
                    RuntimeEvent(
                        RuntimeEventKind.NODE_FINISHED,
                        graph=graph_name,
                        execution_id=execution.id,
                        node=node_id,
                        status="failed" if node_error is not None else "succeeded",
                        error_type=(
                            None if node_error is None else type(node_error).__name__
                        ),
                        attributes={"step": execution.steps},
                    )
                )
            for output in outputs:
                declared = spec.output_ports
                if output.port not in declared:
                    raise InvalidOutputError(
                        f"node {node_id!r} produced unknown port {output.port!r}",
                        graph=graph_name,
                        node=node_id,
                    )
                expected = declared[output.port]
                if not isinstance(output.value, expected):
                    raise PortValueTypeError(
                        f"node {node_id!r} output {output.port!r} expected "
                        f"{expected.__name__}, got {type(output.value).__name__}",
                        graph=graph_name,
                        node=node_id,
                    )
                edges = outgoing_for(node_id, output.port)
                if not edges:
                    execution.publish_output(output)
                    continue
                for edge in edges:
                    target_type = specs[edge.target].input_ports[edge.target_port]
                    if not isinstance(output.value, target_type):
                        raise PortValueTypeError(
                            f"edge target {edge.target}.{edge.target_port} "
                            f"expected {target_type.__name__}",
                            graph=graph_name,
                            node=node_id,
                        )
                    queues[edge.target][edge.target_port].append(output.value)
                    schedule_if_ready(edge.target)

            # 每轮只消费一组输入，避免活跃循环使其他节点长期无法执行。
            schedule_if_ready(node_id)

        execution.checkpoint()
        leftovers = {
            f"{node_id}.{port}": len(values)
            for node_id, ports in queues.items()
            for port, values in ports.items()
            if values
        }
        if leftovers:
            raise IncompleteInputsError(
                f"graph stopped with incomplete inputs: {leftovers!r}",
                graph=graph_name,
            )
        for finalize in tuple(finalizers):
            finalize()

    def _call_node(
        self,
        graph_name: str,
        node_id: str,
        node: Node,
        inputs: Mapping[str, Any],
        context: Context,
        hooks: tuple[NodeHook, ...],
        input_ports: Mapping[str, type[Any]],
        execution: Execution,
    ) -> tuple[Output, ...]:
        """在 Hook 生命周期内调用 Node，并保留原始异常供 error 处理。

        Args:
            graph_name: 执行或观测记录中的 Graph 注册名称。
            node_id: Graph 内绑定的节点 ID。
            node: 节点实例或作用域中的节点 ID，以接口类型为准。
            inputs: 入口数据或按端口名称组织的输入映射。
            context: 当前调用的执行或插件上下文。
            hooks: 待装配的节点 Hook 集合。
            input_ports: 节点声明的输入端口集合。
            execution: 记录当前执行状态、控制限制及输出的句柄。

        Returns:
            符合声明端口契约的 Output 集合。

        Raises:
            ExecutionError: Graph 或节点执行未能完成。
        """

        original = NodeCall(graph_name, node_id, node, inputs, context)

        def invoke(index: int, call: NodeCall) -> Outputs:
            """在输入与控制约束下调用当前节点的执行方法。

            Args:
                index: 条目的索引或切片。
                call: Hook 当前处理的节点调用记录。

            Returns:
                符合声明端口契约的 Output。

            Raises:
                HookExecutionError: Hook 的调用或返回结果违反约束。
            """

            if index == len(hooks):
                try:
                    result = self._resolve(
                        node.execute(call.inputs, context),
                        execution,
                    )
                except (ShortCircuit, StopGraph) as signal:
                    raise HookExecutionError(
                        f"{type(signal).__name__} can only be raised by a hook",
                        graph=graph_name,
                        node=node_id,
                    ) from signal
                return self._coerce_outputs(result, graph_name, node_id, "Node")

            hook = hooks[index]
            try:
                entered = self._resolve(hook.enter(call), execution)
            except ShortCircuit as signal:
                outputs = self._coerce_outputs(
                    signal.outputs, graph_name, node_id, "ShortCircuit"
                )
                return self._resolve_hook_outputs(
                    hook.exit(call, outputs), graph_name, node_id, execution
                )

            entered = self._validate_hook_call(
                original, entered, input_ports, graph_name, node_id
            )
            try:
                outputs = invoke(index + 1, entered)
            except ExecutionControlError:
                raise
            except Exception as error:  # noqa: BLE001
                outputs = self._resolve_hook_outputs(
                    hook.error(entered, error), graph_name, node_id, execution
                )
            return self._resolve_hook_outputs(
                hook.exit(entered, outputs), graph_name, node_id, execution
            )

        try:
            return invoke(0, original)
        except ExecutionError:
            raise
        except (ShortCircuit, StopGraph):
            raise
        except Exception as exc:
            raise ExecutionError(
                f"node {node_id!r} failed: {exc}",
                graph=graph_name,
                node=node_id,
            ) from exc

    def _resolve_hook_outputs(
        self,
        value: object,
        graph_name: str,
        node_id: str,
        execution: Execution,
    ) -> Outputs:
        """解析 Hook 输出并校验输出类型和端口契约。

        Args:
            value: 当前节点或 Hook 产生的待解析返回值。
            graph_name: 执行或观测记录中的 Graph 注册名称。
            node_id: Graph 内绑定的节点 ID。
            execution: 记录当前执行状态、控制限制及输出的句柄。

        Returns:
            符合声明端口契约的 Output。
        """

        resolved = self._resolve(value, execution)
        return self._coerce_outputs(resolved, graph_name, node_id, "Hook")

    def _resolve(self, value: object, execution: Execution) -> object:
        """通过调用运行器解析同步值或等待异步结果。

        Args:
            value: 当前节点或 Hook 产生的待解析返回值。
            execution: 记录当前执行状态、控制限制及输出的句柄。

        Returns:
            实现自行选择的组合结果，调用方不依赖其具体类型。
        """

        return self._runner.resolve(
            value,
            checkpoint=execution.checkpoint,
            wait_timeout=execution.wait_timeout,
        )

    @staticmethod
    def _coerce_outputs(
        result: object,
        graph_name: str,
        node_id: str | None,
        source: str,
    ) -> Outputs:
        """规范化节点返回值，拒绝不符合 Output 契约的内容。

        Args:
            result: 待解析或校验的节点执行结果。
            graph_name: 执行或观测记录中的 Graph 注册名称。
            node_id: Graph 内绑定的节点 ID。
            source: 源节点、原始对象或待转换数据。

        Returns:
            符合声明端口契约的 Output。

        Raises:
            ExecutionError: Graph 或节点执行未能完成。
            InvalidOutputError: 节点输出不符合 Output 契约。
        """

        try:
            return tuple(Engine._iter_outputs(result))
        except ExecutionError:
            raise
        except TypeError as exc:
            raise InvalidOutputError(
                f"{source} returned invalid output: {exc}",
                graph=graph_name,
                node=node_id,
            ) from exc
        except Exception as exc:
            raise ExecutionError(
                f"{source} failed while producing output: {exc}",
                graph=graph_name,
                node=node_id,
            ) from exc

    @staticmethod
    def _validate_hook_call(
        original: NodeCall,
        modified: object,
        input_ports: Mapping[str, type[Any]],
        graph_name: str,
        node_id: str,
    ) -> NodeCall:
        """确保 Hook 仅修改允许的数据，不改变节点调用身份。

        Args:
            original: 修改前的原始调用或定义。
            modified: 待校验的修改结果。
            input_ports: 节点声明的输入端口集合。
            graph_name: 执行或观测记录中的 Graph 注册名称。
            node_id: Graph 内绑定的节点 ID。

        Returns:
            身份约束通过校验的 NodeCall。

        Raises:
            HookExecutionError: Hook 的调用或返回结果违反约束。
            PortValueTypeError: 实际输入或输出值不符合端口类型。
        """

        if not isinstance(modified, NodeCall):
            raise HookExecutionError(
                "hook enter must return NodeCall",
                graph=graph_name,
                node=node_id,
            )
        if (
            modified.graph != original.graph
            or modified.node_id != original.node_id
            or modified.node is not original.node
            or modified.context is not original.context
        ):
            raise HookExecutionError(
                "hook cannot replace graph, node, or context",
                graph=graph_name,
                node=node_id,
            )
        if set(modified.inputs) != set(original.inputs):
            raise HookExecutionError(
                "hook cannot add or remove node input ports",
                graph=graph_name,
                node=node_id,
            )
        for port, value in modified.inputs.items():
            expected = input_ports[port]
            if not isinstance(value, expected):
                raise PortValueTypeError(
                    f"hook input {port!r} expected {expected.__name__}, "
                    f"got {type(value).__name__}",
                    graph=graph_name,
                    node=node_id,
                )
        return modified

    @staticmethod
    def _iter_outputs(result: object) -> Iterator[Output]:
        """把 Node 返回值规范化为按顺序校验的 Output 迭代器。

        Args:
            result: 待解析或校验的节点执行结果。

        Returns:
            按产生顺序交付终端 Output 的迭代入口。

        Raises:
            TypeError: 参数类型或接口实现不符合当前契约。
        """

        if result is None:
            return iter(())
        if isinstance(result, Output):
            return iter((result,))
        if not isinstance(result, Iterable) or isinstance(
            result, (str, bytes, Mapping)
        ):
            raise TypeError("Node must return Output, Iterable[Output], or None")

        def validated() -> Iterator[Output]:
            """逐项校验输出类型和端口后继续交付。

            Yields:
                按原始顺序通过 Output 类型校验的节点输出。

            Raises:
                TypeError: 参数类型或接口实现不符合当前契约。
            """

            for output in result:
                if not isinstance(output, Output):
                    raise TypeError("Node output iterable must contain only Output")
                yield output

        return validated()

    @staticmethod
    def _coerce_inputs(graph: Graph, value: Any) -> Mapping[str, Any]:
        """将入口输入规范化为符合端口声明的映射。

        Args:
            graph: 目标 Graph 定义或其注册名称，以类型声明为准。
            value: 当前节点或 Hook 产生的待解析返回值。

        Returns:
            按端口名称组织的已校验输入映射。

        Raises:
            PortValueTypeError: 实际输入或输出值不符合端口类型。
        """

        ports = graph.spec_for(graph.entrypoint).input_ports
        names = tuple(ports)
        if not names:
            if value not in (None, {}):
                raise PortValueTypeError("graph entrypoint accepts no input")
            return MappingProxyType({})
        if len(names) == 1:
            prepared = {names[0]: value}
        elif isinstance(value, Mapping) and set(value) == set(names):
            prepared = dict(value)
        else:
            raise PortValueTypeError(f"graph entrypoint requires input ports {names!r}")
        for port, item in prepared.items():
            expected = ports[port]
            if not isinstance(item, expected):
                raise PortValueTypeError(
                    f"entry input {port!r} expected {expected.__name__}, "
                    f"got {type(item).__name__}"
                )
        return MappingProxyType(prepared)
