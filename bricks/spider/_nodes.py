"""将业务操作和 Hook 插槽分别装配为显式节点，signals 只在领域层转译。"""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Awaitable, Callable, Iterable, Mapping
from typing import Any

from interlace import Context as EngineContext
from interlace import Node, Output, Ports
from interlace.engine.errors import ExecutionControlError

from bricks import AsyncDownloadNode, DownloadNode, signals
from bricks.hooks import Task

from ._calls import invoke
from ._hooks import run_hooks
from .context import Context, InitContext, ResponseRejectedError, _Initialization, _Task


def _run(
    owner: Any,
    callback: Callable[..., Any],
    context: Context | InitContext,
    finish: Callable[[Any], Output],
    route: Callable[[signals.Signal], Output],
) -> Output | Awaitable[Output]:
    """在同步调用和异步等待两侧统一处理领域信号，保留普通错误与取消。

    Args:
        owner: 当前 Spider，仅调用领域错误报告入口。
        callback: 当前业务或插槽调用。
        context: 本次独立上下文。
        finish: 正常返回值转换。
        route: 当前节点支持的信号转换。

    Returns:
        类型化输出或等待结果。

    Raises:
        BaseException: 非领域信号继续按原始错误传播。
    """

    def report(error: Exception) -> None:
        """报告普通异常，不将取消或超时交给业务错误 Hook 恢复。

        Args:
            error: 原异常。
        """
        if not isinstance(error, ExecutionControlError):
            owner._report_error(context, error)

    def signal_output(signal: signals.Signal) -> Output:
        """将信号转换为本节点声明的端口。

        Args:
            signal: 明确的领域信号。

        Returns:
            图内输出。
        """
        try:
            return route(signal)
        except Exception as error:
            report(error)
            raise

    try:
        value = invoke(owner, callback, context, finish)
    except signals.Signal as signal:
        return signal_output(signal)
    except Exception as error:
        report(error)
        raise
    if not inspect.isawaitable(value):
        return value

    async def awaited() -> Output:
        """等待业务协程，在同一边界捕获协程实际抛出的领域信号。

        Returns:
            正常或信号产生的输出。
        """
        try:
            return await value
        except signals.Signal as signal:
            return signal_output(signal)
        except Exception as error:
            report(error)
            raise

    return awaited()


class HookNode(Node):
    """一个阶段一个节点，按固定顺序调用该插槽全部 Hooks。

    Attributes:
        input_ports: 初始化状态、入口种子或图内任务端口。
        output_ports: 正常、完成以及当前阶段允许的 retry/skip 端口。
        _owner: 提供错误报告和回调生命周期保护的 Spider。
        _form: 当前插槽名称。
        _tasks: 打开时固定、已按 index 排序的声明。
        _initial: 是否属于初始化图。
        _retry: 是否允许请求图内重试。
        _skip: 是否允许跳过对应业务操作。
    """

    def __init__(
        self,
        owner: Any,
        form: str,
        tasks: tuple[Task, ...],
        *,
        initial: bool = False,
        entry: bool = False,
        retry: bool = False,
        skip: bool = False,
    ) -> None:
        """固定插槽与合法路由，不保存本次执行状态。

        Args:
            owner: 当前 Spider。
            form: 阶段名称。
            tasks: 有序声明快照。
            initial: 初始化图标志。
            entry: 消费图入口，接收 JSON 种子。
            retry: 是否声明重试端口。
            skip: 是否声明跳过端口。
        """
        self._owner, self._form, self._tasks = owner, form, tasks
        self._initial, self._retry, self._skip = initial, retry, skip
        value_type = _Initialization if initial else _Task
        normal = "initial" if initial else "task"
        self.input_ports = (
            Ports(initial=_Initialization)
            if initial
            else Ports(seeds=dict)
            if entry
            else Ports(task=_Task)
        )
        ports = {normal: value_type, "complete": value_type}
        if retry:
            ports["retry"] = value_type
        if skip:
            ports["skip"] = value_type
        self.output_ports = Ports(**ports)

    def execute(
        self, inputs: Mapping[str, Any], context: EngineContext
    ) -> Output | Awaitable[Output]:
        """执行整组回调，并将第一个控制信号转换为图内路由。

        Args:
            inputs: 最终校验过的端口输入。
            context: 当前图提供的领域上下文。

        Returns:
            正常、提前完成、重试或跳过输出。

        Raises:
            TypeError: 上下文、返回值或信号在当前阶段不合法。
            ResponseRejectedError: 明确业务失败。
        """
        if self._initial:
            if not isinstance(context, InitContext):
                raise TypeError("initial HookNode requires InitContext")
            context._initial = inputs["initial"]
            payload: Any = context._initial
            normal = "initial"
        else:
            if not isinstance(context, Context):
                raise TypeError("crawl HookNode requires Context")
            context._bind(inputs)
            payload = context._task
            normal = "task"
        context.form = self._form
        context.checkpoint()

        def finish(value: Any) -> Output:
            """正常回调完成后输出当前数据载体。

            Args:
                value: 插槽结果，必须为 None。

            Returns:
                正常输出。
            """
            if value is not None:
                raise TypeError("hooks must return None")
            context.checkpoint()
            return Output(payload, normal)

        def route(signal: signals.Signal) -> Output:
            """按插槽类型处理控制信号。

            Args:
                signal: 插槽抛出的信号。

            Returns:
                已声明的端口输出。

            Raises:
                TypeError: 当前插槽不支持该信号或缺少替代数据。
                ResponseRejectedError: Failure 明确拒绝任务。
            """
            if isinstance(signal, signals.Pass):
                return Output(payload, normal)
            if isinstance(signal, signals.Success):
                return Output(payload, "complete")
            if isinstance(signal, signals.Failure):
                raise ResponseRejectedError(
                    str(signal) or "seed marked as failed"
                ) from signal
            if (
                isinstance(signal, signals.Retry)
                and self._retry
                and isinstance(context, Context)
            ):
                context.request
                payload.retry_delay = signal.delay
                return Output(payload, "retry")
            if isinstance(signal, signals.Skip) and self._skip:
                if isinstance(context, Context):
                    if self._form == "BEFORE_MAKE_REQUEST":
                        context.request
                    elif self._form == "BEFORE_REQUEST":
                        context.response
                    elif self._form == "BEFORE_PARSE":
                        context.items
                elif self._form == "BEFORE_MAKE_SEEDS":
                    context._initial.iterator = iter(())
                return Output(payload, "skip")
            raise TypeError(f"{type(signal).__name__} is not allowed in {self._form}")

        return _run(
            self._owner,
            lambda current: run_hooks(self._owner, self._tasks, current),
            context,
            finish,
            route,
        )


class CrawlNode(Node):
    """执行业务操作，Hook 已由前后独立节点执行。

    Attributes:
        input_ports: 图内任务输入。
        output_ports: 正常与成功收尾端口，下载阶段另有 retry。
        _owner: 当前 Spider。
        _stage: 构造时固定的操作名称。
        _callback: 业务方法或 None。
        _download: 已装配的领域下载节点。
        _max_retries: 额外重试次数上限。
        _retry_delay: 默认重试等待秒数。
    """

    input_ports = Ports(task=_Task)  # 同一执行的数据载体。

    def __init__(
        self,
        owner: Any,
        stage: str,
        callback: Callable[..., Any] | None = None,
        *,
        download: DownloadNode | AsyncDownloadNode | None = None,
        max_retries: int = 2,
        retry_delay: float = 0,
    ) -> None:
        """固定业务操作、资源与输出类型。

        Args:
            owner: 当前 Spider。
            stage: 业务操作名。
            callback: 显式接收 Context 的方法。
            download: 借用下载节点。
            max_retries: 额外重试次数。
            retry_delay: 默认等待秒数。
        """
        self._owner, self._stage, self._callback = owner, stage, callback
        self._download, self._max_retries, self._retry_delay = (
            download,
            max_retries,
            retry_delay,
        )
        ports = {"task": _Task, "complete": _Task}
        if stage == "download":
            ports["retry"] = _Task
        self.output_ports = (
            Ports(summary=dict) if stage == "complete" else Ports(**ports)
        )

    def execute(
        self, inputs: Mapping[str, Any], context: EngineContext
    ) -> Output | Awaitable[Output]:
        """运行一次业务步骤，成功信号只选择完成端口，不直接确认任务。

        Args:
            inputs: 当前任务输入。
            context: 当前领域上下文。

        Returns:
            类型化输出或 Awaitable。
        """
        if not isinstance(context, Context):
            raise TypeError("crawl graph requires Context")
        context._bind(inputs)
        context.form = "ON_" + {"download": "REQUEST", "retry_prepare": "RETRY"}.get(
            self._stage, self._stage.upper()
        )
        context.checkpoint()
        task = context._task

        def operation(current: Context) -> Any:
            """在当前执行中调用业务或执行内建步骤。

            Args:
                current: 当前上下文。

            Returns:
                业务结果或 Awaitable。
            """
            if self._stage == "complete":
                if self._callback is not None:
                    value = self._callback(current)
                    if inspect.iscoroutine(value):
                        value.close()
                    if value is not None:
                        raise TypeError(
                            "completion collector must return None synchronously"
                        )
                return {
                    "items": 0 if task.items is None else len(task.items),
                    "attempts": task.attempt,
                    "retries": task.retries,
                }
            if self._stage == "retry":
                if task.retries >= self._max_retries:
                    raise ResponseRejectedError("download retries exhausted")
                task.retries += 1
                return None
            if self._stage == "retry_wait":

                async def wait() -> None:
                    """等待本次重试并清理上一响应，期间仍持有当前 Slot。"""
                    delay = (
                        self._retry_delay
                        if task.retry_delay is None
                        else task.retry_delay
                    )
                    if delay:
                        await asyncio.sleep(delay)
                    current.checkpoint()
                    current.request = current.request.copy()
                    task.response = None
                    task.retry_delay = None

                return wait()
            if self._stage == "download":
                assert self._download is not None
                task.attempt += 1
                return self._download.execute({"request": current.request}, current)
            assert self._callback is not None
            return self._callback(current)

        def finish(value: Any) -> Output:
            """校验业务结果后沿正常边传递任务。

            Args:
                value: 业务返回值。

            Returns:
                当前步骤输出。
            """
            context.checkpoint()
            if self._stage == "complete":
                return Output(value, "summary")
            if self._stage == "make_request":
                context.request = value
            elif self._stage == "download":
                if not isinstance(value, Output):
                    raise TypeError("download node must return Output")
                context.response = value.value
            elif self._stage == "parse":
                context.items = value
            elif value is not None:
                raise TypeError(f"{self._stage} must return None")
            return Output(task, "task")

        def route(signal: signals.Signal) -> Output:
            """转换业务方法抛出的有限控制信号。

            Args:
                signal: 明确信号。

            Returns:
                完成或重试端口。

            Raises:
                ResponseRejectedError: 业务失败。
                TypeError: 该业务阶段不支持此信号。
            """
            if isinstance(signal, signals.Success):
                return Output(task, "complete")
            if isinstance(signal, signals.Failure):
                raise ResponseRejectedError(
                    str(signal) or "seed marked as failed"
                ) from signal
            if isinstance(signal, signals.Retry) and self._stage == "download":
                task.retry_delay = signal.delay
                return Output(task, "retry")
            raise TypeError(f"{type(signal).__name__} is not allowed in {context.form}")

        return _run(self._owner, operation, context, finish, route)


class InitNode(Node):
    """推进种子源、提交与检查点，前后 Hook 由独立节点执行。

    Attributes:
        input_ports: 当前初始化状态。
        output_ports: next_seed 分支为 seed/complete，其他操作为 initial/complete。
        _owner: 当前 Spider。
        _stage: 操作名称。
        _callback: 种子工厂或提交函数。
    """

    input_ports = Ports(initial=_Initialization)  # 不跨队列传输的生产状态。

    def __init__(
        self, owner: Any, stage: str, callback: Callable[..., Any] | None = None
    ) -> None:
        """固定初始化步骤。

        Args:
            owner: 当前 Spider。
            stage: make_seeds、next_seed、submit、checkpoint 或 complete。
            callback: 对应业务函数。
        """
        self._owner, self._stage, self._callback = owner, stage, callback
        self.output_ports = (
            Ports(summary=dict)
            if stage == "complete"
            else Ports(seed=_Initialization, complete=_Initialization)
            if stage == "next_seed"
            else Ports(initial=_Initialization, complete=_Initialization)
        )

    def execute(
        self, inputs: Mapping[str, Any], context: EngineContext
    ) -> Output | Awaitable[Output]:
        """执行一个初始化操作或响应明确的完成/失败信号。

        Args:
            inputs: 本次初始化状态。
            context: 图内初始化上下文。

        Returns:
            当前操作输出或 Awaitable。
        """
        if not isinstance(context, InitContext):
            raise TypeError("initialization graph requires InitContext")
        state = inputs["initial"]
        context._initial = state
        context.form = "ON_" + self._stage.upper()
        context.checkpoint()

        def operation(current: InitContext) -> Any:
            """推进本次生产操作。

            Args:
                current: 初始化上下文。

            Returns:
                操作结果。
            """
            if self._stage == "complete":
                return Output({"submitted": state.produced}, "summary")
            if self._stage == "next_seed":
                assert state.iterator is not None
                try:
                    current.seeds = next(state.iterator)
                except StopIteration:
                    return Output(state, "complete")
                return Output(state, "seed")
            if self._stage == "checkpoint":
                current.task_queue.save_init(state.lease, state.progress)
                return None
            assert self._callback is not None
            return self._callback(current)

        def finish(value: Any) -> Output:
            """绑定迭代器或确认初始化步骤结果。

            Args:
                value: 操作结果。

            Returns:
                当前图输出。
            """
            context.checkpoint()
            if self._stage in {"next_seed", "complete"}:
                return value
            if self._stage == "make_seeds":
                if value is None:
                    value = ()
                elif type(value) is dict:
                    value = (value,)
                if not isinstance(value, Iterable) or isinstance(value, (str, bytes)):
                    raise TypeError(
                        "make_seeds must return a dict, list or synchronous iterable"
                    )
                state.iterator = iter(value)
            elif value is not None:
                raise TypeError(f"{self._stage} must return None")
            return Output(state, "initial")

        def route(signal: signals.Signal) -> Output:
            """结束初始化或明确报告非法阶段信号。

            Args:
                signal: 初始化业务信号。

            Returns:
                完成端口。
            """
            if isinstance(signal, signals.Success):
                return Output(state, "complete")
            if isinstance(signal, signals.Failure):
                raise ResponseRejectedError(
                    str(signal) or "initialization marked as failed"
                ) from signal
            raise TypeError(f"{type(signal).__name__} is not allowed in {context.form}")

        return _run(self._owner, operation, context, finish, route)
