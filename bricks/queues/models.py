"""任务队列的交付凭据、状态快照与严格 JSON 编码。"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from interlace import Event
from interlace.spi import ExecutionLimits, Work


class LeaseLostError(RuntimeError):
    """租约已到期、被接管或不属于当前队列。"""


class TaskConflictError(ValueError):
    """同一任务 ID 已对应不同的任务内容。"""


def text(value: str, name: str) -> str:
    """校验不含首尾空白的非空名称。

    Args:
        value: 待校验名称。
        name: 错误中的字段名。

    Returns:
        原始名称。

    Raises:
        ValueError: 名称不是规范的非空字符串。
    """
    if not isinstance(value, str) or not value or value.strip() != value:
        raise ValueError(f"{name} must be a non-empty trimmed string")
    return value


def seconds(value: float, *, positive: bool = True) -> float:
    """校验有限的秒数。

    Args:
        value: 秒数，拒绝布尔值。
        positive: 是否必须严格大于零，默认 True。

    Returns:
        浮点秒数。

    Raises:
        ValueError: 秒数非法。
    """
    if type(value) not in (int, float) or not math.isfinite(value):
        raise ValueError("seconds must be finite")
    if value < 0 or (positive and value == 0):
        raise ValueError("seconds must be positive" if positive else "negative seconds")
    return float(value)


def encode(value: Any) -> str:
    """严格编码 JSON 值，拒绝隐式键转换、非有限数和运行时资源。

    Args:
        value: 仅由 JSON 标量、列表和字符串键字典组成的数据。

    Returns:
        排序后可用于内容比较的 JSON 文本。

    Raises:
        TypeError: 数据不属于明确的 JSON 类型。
        ValueError: 数据包含非有限数或循环引用。
    """

    def validate(item: Any, ancestors: set[int]) -> None:
        """递归检查类型与当前祖先中的循环。

        Args:
            item: 当前值。
            ancestors: 当前递归路径的容器身份。

        Raises:
            TypeError: 含非 JSON 类型或非字符串键。
            ValueError: 含循环引用。
        """
        if item is None or type(item) in (str, int, float, bool):
            return
        if type(item) not in (list, dict):
            if type(item).__name__ == "_Missing" and type(item).__module__.startswith(
                "bricks.parsers"
            ):
                raise TypeError("queue data cannot contain parser MISSING")
            raise TypeError("queue data must contain only JSON values")
        if id(item) in ancestors:
            raise ValueError("circular queue data")
        ancestors.add(id(item))
        try:
            if isinstance(item, dict):
                if any(type(key) is not str for key in item):
                    raise TypeError("queue object keys must be strings")
                values = item.values()
            else:
                values = item
            for child in values:
                validate(child, ancestors)
        finally:
            ancestors.remove(id(item))

    validate(value, set())
    return json.dumps(
        value, ensure_ascii=True, allow_nan=False, sort_keys=True, separators=(",", ":")
    )


def encode_work(work: Work) -> str:
    """保存公开 Work 字段，任务输入与事件 payload 必须是 JSON 数据。

    Args:
        work: 待传输的执行请求。

    Returns:
        版本化的任务信封。

    Raises:
        TypeError: 输入不是 Work 或携带不可序列化的数据。
        ValueError: 数据不符合 JSON 契约。
    """
    if not isinstance(work, Work):
        raise TypeError("queue task must be Work")
    text(work.id, "work ID")
    text(work.graph, "work graph")
    return encode(
        {
            "version": 1,
            "id": work.id,
            "graph": work.graph,
            "inputs": work.inputs,
            "options": dict(work.options),
            "limits": {
                "max_steps": work.limits.max_steps,
                "timeout": work.limits.timeout,
            },
            "trigger": None
            if work.trigger is None
            else {"type": work.trigger.type, "payload": work.trigger.payload},
        }
    )


def decode_work(value: str) -> Work:
    """按严格字段集合重建独立 Work，不执行动态反序列化。

    Args:
        value: 当前版本的任务信封。

    Returns:
        当前进程拥有的新 Work。

    Raises:
        ValueError: 信封版本不受支持或内容非法。
        TypeError: Work 字段不符合公开契约。
    """
    data = json.loads(value)
    if type(data) is not dict or set(data) != {
        "version",
        "id",
        "graph",
        "inputs",
        "options",
        "limits",
        "trigger",
    }:
        raise ValueError("invalid queue envelope keys")
    if type(data["version"]) is not int or data["version"] != 1:
        raise ValueError("unsupported queue envelope version")
    if type(data["id"]) is not str or type(data["graph"]) is not str:
        raise TypeError("queue envelope id and graph must be strings")
    text(data["id"], "work ID")
    text(data["graph"], "work graph")
    if type(data["options"]) is not dict or type(data["limits"]) is not dict:
        raise TypeError("queue envelope options and limits must be objects")
    if set(data["limits"]) != {"max_steps", "timeout"}:
        raise ValueError("invalid queue execution limits")
    trigger = data["trigger"]
    if trigger is not None and (
        type(trigger) is not dict or set(trigger) != {"type", "payload"}
    ):
        raise ValueError("invalid queue trigger")
    return Work(
        graph=data["graph"],
        inputs=data["inputs"],
        id=data["id"],
        options=data["options"],
        limits=ExecutionLimits(**data["limits"]),
        trigger=None if trigger is None else Event(**trigger),
    )


def make_work(
    graph: str,
    inputs: Any = None,
    *,
    id: str | None = None,
    options: Mapping[str, Any] | None = None,
    limits: ExecutionLimits | None = None,
    trigger: Event | None = None,
) -> Work:
    """构造独立 JSON 任务，缺省 ID 按规范化任务内容生成 SHA-256 指纹。

    Args:
        graph: 已注册的图名称，建议包含逻辑版本。
        inputs: JSON 任务输入，构造时建立独立副本。
        id: 显式指定的任务 ID；None 按内容计算，空字符串不视为缺省。
        options: 本次执行的 JSON 配置，缺省为空映射。
        limits: 执行限制，缺省使用 Interlace 默认限制。
        trigger: 可选来源事件，其类型和 JSON payload 参与内容指纹。

    Returns:
        可直接交给 TaskQueue.put 或 QueueBackend.submit 的 Interlace Work。
        缺省 ID 为 sha256: 加十六进制摘要；内容包括信封版本、graph、inputs、
        options、limits 和 trigger，不包括 ID、租约或交付次数。

    Raises:
        TypeError: 字段违反 Work 或严格 JSON 契约。
        ValueError: 名称、ID 或内容非法。
    """
    work = Work(
        graph,
        inputs,
        trigger,
        options={} if options is None else options,
        limits=ExecutionLimits() if limits is None else limits,
        **({} if id is None else {"id": id}),
    )
    data = json.loads(encode_work(work))
    if id is None:
        del data["id"]
        data["id"] = (
            "sha256:" + hashlib.sha256(encode(data).encode("utf-8")).hexdigest()
        )
    return decode_work(encode(data))


@dataclass(frozen=True)
class LocalSubmission:
    """仅在当前进程 Event 中携带完整持久 Work 的内部信封。

    Attributes:
        work: 已包含稳定 ID、options 和 limits 的目标 Work。
    """

    work: Work

    def __post_init__(self) -> None:
        """校验本地信封只包装 Interlace Work。

        Raises:
            TypeError: 内容不是 Work。
        """
        if not isinstance(self.work, Work):
            raise TypeError("local submission requires a Work")


@dataclass(frozen=True)
class TaskLease:
    """一次交付的凭据；续租不改变 token，重新领取必须生成新 token。

    Attributes:
        queue: 所属命名队列。
        work: 此次领取独立解码的任务，修改不影响存储。
        token: 本次领取独有的凭据，不是业务身份。
        owner: 调用方声明的 Worker 身份。
        attempt: 从 1 开始的交付次数。
        deadline: 服务端或内存时钟上的过期时间，单位秒。
    """

    queue: str
    work: Work
    token: str
    owner: str
    attempt: int
    deadline: float


@dataclass(frozen=True)
class TaskRecord:
    """调用方拥有的任务状态快照。

    Attributes:
        work: 独立解码的执行请求。
        status: pending、leased、acked 或 dead。
        attempt: 已经开始的交付次数。
        error: 最后一次重投或拒绝原因。
        result: 显式业务结果 success/failure；未登记时为 None，ACK 不推断成功。
    """

    work: Work
    status: str
    attempt: int
    error: str
    result: str | None


@dataclass(frozen=True)
class InitLease:
    """初始化生产者的有期限写入凭据。

    Attributes:
        queue: 所属命名队列。
        owner: 初始化生产者身份。
        token: 每次申请独有的凭据。
        fence: 当前队列单调递增的接管序号。
        deadline: 存储时钟上的过期时间，单位秒。
    """

    queue: str
    owner: str
    token: str
    fence: int
    deadline: float


@dataclass(frozen=True)
class InitRecord:
    """初始化进度快照，生产结束与消费结束分别记录。

    Attributes:
        status: running、succeeded 或 failed；过期 running 允许重新申请。
        lease: 最后生产者的凭据，读取快照不保证它仍有效。
        progress: 调用方拥有的 JSON 进度副本。
        error: 显式记录的初始化失败原因。
    """

    status: str
    lease: InitLease
    progress: Any
    error: str
