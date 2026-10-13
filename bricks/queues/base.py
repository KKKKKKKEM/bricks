"""种子任务队列的显式操作契约和共享参数校验。"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, Protocol
from uuid import uuid4

from interlace.spi import Work

from .models import (
    InitLease,
    InitRecord,
    LeaseLostError,
    TaskConflictError,
    TaskLease,
    TaskRecord,
    decode_work,
    encode,
    encode_work,
    seconds,
    text,
)

if TYPE_CHECKING:
    from .scope import TaskScope


class TaskQueue(Protocol):
    """可替换的具名任务存储；不执行 Graph，不拥有注入资源。

    Attributes:
        name: 当前队列的固定名称，也是任务 ID 的去重范围。
    """

    @property
    def name(self) -> str:
        """返回固定队列名称。

        Returns:
            当前任务 ID 的去重范围。
        """
        ...

    def put(self, work: Work, *, init: InitLease | None = None) -> bool:
        """原子保存任务及待领取索引。

        Args:
            work: JSON 可序列化的公开 Work，可用 make_work 自动生成稳定 ID。
            init: 可选初始化租约，与入队在同一原子操作中校验。

        Returns:
            新增返回 True，相同 ID 与内容已存在返回 False。

        Raises:
            TaskConflictError: 相同 ID 对应不同内容。
            LeaseLostError: 初始化租约无效。
        """
        ...

    def claim(self, owner: str, *, lease_seconds: float = 30) -> TaskLease | None:
        """非阻塞领取一项任务，并有界回收已过期的交付。

        Args:
            owner: Worker 身份。
            lease_seconds: 此次租约时长，默认 30 秒。

        Returns:
            交付凭据；当前没有可领取任务时返回 None。
        """
        ...

    def put_claim(
        self, work: Work, owner: str, *, lease_seconds: float = 30
    ) -> TaskLease | None:
        """原子保存并按 ID 领取一个就绪任务。

        Args:
            work: JSON 可序列化的目标 Work。
            owner: 本地继续执行它的 Worker 身份。
            lease_seconds: 此次租约时长，默认 30 秒。

        Returns:
            新建或可领取任务的凭据；相同任务已在处理、延迟或终态时返回 None。

        Raises:
            TaskConflictError: 相同 ID 对应不同内容。
        """
        ...

    def renew(self, lease: TaskLease, *, lease_seconds: float = 30) -> TaskLease:
        """续租并返回新截止时间。

        Args:
            lease: 当前交付凭据。
            lease_seconds: 从当前存储时间开始计算的时长。

        Returns:
            token 不变的新凭据。

        Raises:
            LeaseLostError: 租约失效。
        """
        ...

    def ack(self, lease: TaskLease, *, result: str | None = None) -> None:
        """确认交付，可同时记录显式的业务结果。

        Args:
            lease: 当前交付凭据。
            result: success、failure 或 None；不根据 ACK 猜测业务成功。

        Raises:
            LeaseLostError: 租约失效。
        """
        ...

    def retry(self, lease: TaskLease, *, delay: float = 0, error: str = "") -> None:
        """有界重投，耗尽次数时进入 dead。

        Args:
            lease: 当前交付凭据。
            delay: 再次可领取之前的秒数。
            error: 当前失败的文本说明。
        """
        ...

    def reject(self, lease: TaskLease, *, error: str = "") -> None:
        """结束交付并进入 dead，保留内容供检查。

        Args:
            lease: 当前交付凭据。
            error: 拒绝原因。
        """
        ...

    def get(self, task_id: str) -> TaskRecord | None:
        """读取独立任务快照。

        Args:
            task_id: 队列内任务 ID。

        Returns:
            任务快照或 None。
        """
        ...

    def counts(self) -> dict[str, int]:
        """读取 pending、leased、acked、dead 数量。

        Returns:
            独立计数字典，过期但尚未回收的任务仍计入 leased。
        """
        ...

    def acquire_init(
        self, owner: str, *, lease_seconds: float = 30
    ) -> InitLease | None:
        """申请初始化执行权，成功状态不重新开始。

        Args:
            owner: 初始化生产者身份。
            lease_seconds: 租约有效秒数。

        Returns:
            新租约；已成功或已有有效生产者时返回 None。
        """
        ...

    def renew_init(self, lease: InitLease, *, lease_seconds: float = 30) -> InitLease:
        """续租初始化生产者。

        Args:
            lease: 当前初始化凭据。
            lease_seconds: 从存储当前时间计算的有效秒数。

        Returns:
            新截止时间的租约。
        """
        ...

    def save_init(self, lease: InitLease, progress: Any) -> None:
        """在租约保护下保存 JSON 断点。

        Args:
            lease: 当前初始化凭据。
            progress: 调用方定义的 JSON 进度。
        """
        ...

    def finish_init(self, lease: InitLease, *, success: bool, error: str = "") -> None:
        """记录初始化结束，不推断消费者已经排空。

        Args:
            lease: 当前初始化凭据。
            success: 是否完成生产，失败后允许显式重新申请。
            error: 失败原因。
        """
        ...

    def initialization(self) -> InitRecord | None:
        """读取初始化状态。

        Returns:
            独立的进度快照或 None。
        """
        ...

    def forget(self, task_id: str) -> bool:
        """显式移除终态任务及去重记录。

        Args:
            task_id: 要清理的任务 ID。

        Returns:
            是否存在且已删除。

        Raises:
            ValueError: 任务尚未进入终态。
        """
        ...


class _Queue:
    """为本地和 Redis 存储提供相同的校验与快照边界。

    Attributes:
        _name: 构造后固定的队列名称，通过只读属性访问。
        _max_attempts: 每个任务的最大交付次数，包含首次交付。
        _renewals: 此队列实例的惰性共享续租器，无活跃上下文时没有线程。
    """

    def __init__(self, name: str, max_attempts: int) -> None:
        """校验固定配置。

        Args:
            name: 队列名称。
            max_attempts: 最大交付次数。

        Raises:
            ValueError: 名称或次数非法。
        """
        self._name = text(name, "queue name")
        if type(max_attempts) is not int or max_attempts < 1:
            raise ValueError("max_attempts must be a positive integer")
        self._max_attempts = max_attempts
        from .scope import _Renewals

        self._renewals = _Renewals()

    @property
    def name(self) -> str:
        """返回构造时固定的队列名称。

        Returns:
            当前存储的名称。
        """
        return self._name

    def reserve(self, owner: str, *, lease_seconds: float = 30) -> TaskScope:
        """创建进入时领取、退出时结束占用的单次上下文。

        Args:
            owner: Worker 身份。
            lease_seconds: 领取和续租秒数，默认 30。

        Returns:
            TaskScope；进入时队列为空，其 as 值为 None。
        """
        from .scope import TaskScope

        return TaskScope(self, self._renewals, owner=owner, lease_seconds=lease_seconds)

    def hold(self, lease: TaskLease, *, lease_seconds: float = 30) -> TaskScope:
        """创建接管已有凭据并自动续租的上下文。

        Args:
            lease: 当前队列已领取的凭据，进入时立即续租校验。
            lease_seconds: 续租秒数，默认 30。

        Returns:
            TaskScope；进入后由它管理该凭据的续租和退出决定。
        """
        from .scope import TaskScope

        return TaskScope(self, self._renewals, lease=lease, lease_seconds=lease_seconds)

    def _call(self, operation: str, **args: Any) -> Any:
        """通过具体存储执行一项原子操作。

        Args:
            operation: 内部操作名。
            args: 已校验的原始 JSON 参数。

        Returns:
            操作结果。

        Raises:
            NotImplementedError: 子类未提供存储。
        """
        raise NotImplementedError

    def _execute(self, operation: str, **args: Any) -> Any:
        """统一翻译存储返回的领域错误。

        Args:
            operation: 内部操作名。
            args: 操作参数。

        Returns:
            存储结果。

        Raises:
            LeaseLostError: 凭据失效。
            TaskConflictError: ID 内容冲突。
            ValueError: 操作不符合当前状态。
        """
        result = self._call(operation, **args)
        if isinstance(result, dict) and "failure" in result:
            reason = result["failure"]
            if reason == "lease":
                raise LeaseLostError("queue lease expired or replaced")
            if reason == "conflict":
                raise TaskConflictError("task ID already has different content")
            raise ValueError(reason)
        return result

    def _credentials(self, lease: TaskLease | InitLease) -> dict[str, Any]:
        """提取当前队列可接受的凭据。

        Args:
            lease: 交付或初始化凭据。

        Returns:
            可序列化的凭据字段。

        Raises:
            LeaseLostError: 凭据属于其他队列。
        """
        if lease.queue != self.name:
            raise LeaseLostError("lease belongs to another queue")
        data: dict[str, Any] = {"token": lease.token, "owner": lease.owner}
        if isinstance(lease, TaskLease):
            data["id"] = lease.work.id
        else:
            data["fence"] = lease.fence
        return data

    def put(self, work: Work, *, init: InitLease | None = None) -> bool:
        """原子入队并校验可选初始化凭据，契约见 TaskQueue.put。

        Args:
            work: 待保存任务。
            init: 可选初始化凭据。

        Returns:
            是否新增任务。
        """
        body = encode_work(work)
        return bool(
            self._execute(
                "put",
                id=work.id,
                body=body,
                maximum=self._max_attempts,
                init=None if init is None else self._credentials(init),
            )
        )

    def claim(self, owner: str, *, lease_seconds: float = 30) -> TaskLease | None:
        """领取一项就绪任务。

        Args:
            owner: Worker 身份。
            lease_seconds: 租约秒数。

        Returns:
            交付凭据或 None。
        """
        data = self._execute(
            "claim",
            owner=text(owner, "owner"),
            ttl=seconds(lease_seconds),
            token=uuid4().hex,
        )
        return None if data is None else self._task_lease(data)

    def put_claim(
        self, work: Work, owner: str, *, lease_seconds: float = 30
    ) -> TaskLease | None:
        """原子保存并按内容 ID 领取当前任务。

        Args:
            work: 待保存和领取的严格 JSON Work。
            owner: 本地 Worker 身份。
            lease_seconds: 租约秒数。

        Returns:
            本次取得的交付凭据；重复任务不可领取时返回 None。
        """
        data = self._execute(
            "put_claim",
            id=work.id,
            body=encode_work(work),
            maximum=self._max_attempts,
            owner=text(owner, "owner"),
            ttl=seconds(lease_seconds),
            token=uuid4().hex,
        )
        return None if data is None else self._task_lease(data)

    def _task_lease(self, data: dict[str, Any]) -> TaskLease:
        """从内部快照重建交付凭据。

        Args:
            data: 存储返回的数据。

        Returns:
            调用方独立拥有的凭据与 Work。
        """
        return TaskLease(
            self.name,
            decode_work(data["body"]),
            data["token"],
            data["owner"],
            data["attempt"],
            data["deadline"],
        )

    def renew(self, lease: TaskLease, *, lease_seconds: float = 30) -> TaskLease:
        """续租当前交付。

        Args:
            lease: 当前凭据。
            lease_seconds: 新的有效秒数。

        Returns:
            新截止时间的凭据。
        """
        return self._task_lease(
            self._execute(
                "renew", **self._credentials(lease), ttl=seconds(lease_seconds)
            )
        )

    def ack(self, lease: TaskLease, *, result: str | None = None) -> None:
        """确认交付，并可原子登记业务结果。

        Args:
            lease: 当前凭据。
            result: success、failure 或 None。

        Raises:
            ValueError: 业务结果非法。
        """
        if result not in (None, "success", "failure"):
            raise ValueError("result must be success, failure or None")
        self._execute("ack", **self._credentials(lease), result=result)

    def retry(self, lease: TaskLease, *, delay: float = 0, error: str = "") -> None:
        """结束当前租约并按最大次数重新入队。

        Args:
            lease: 当前凭据。
            delay: 延迟秒数。
            error: 原因文本。
        """
        self._execute(
            "retry",
            **self._credentials(lease),
            delay=seconds(delay, positive=False),
            error=self._error(error),
        )

    def reject(self, lease: TaskLease, *, error: str = "") -> None:
        """保留交付为 dead 状态。

        Args:
            lease: 当前凭据。
            error: 原因文本。
        """
        self._execute("reject", **self._credentials(lease), error=self._error(error))

    @staticmethod
    def _error(error: str) -> str:
        """校验错误文本，不隐式序列化异常对象。

        Args:
            error: 错误说明。

        Returns:
            原始文本。

        Raises:
            TypeError: 不是字符串。
        """
        if not isinstance(error, str):
            raise TypeError("error must be a string")
        return error

    def get(self, task_id: str) -> TaskRecord | None:
        """读取独立快照。

        Args:
            task_id: 任务 ID。

        Returns:
            快照或 None。
        """
        data = self._execute("get", id=text(task_id, "task ID"))
        return (
            None
            if data is None
            else TaskRecord(
                decode_work(data["body"]),
                data["status"],
                data["attempt"],
                data["error"],
                data["result"],
            )
        )

    def counts(self) -> dict[str, int]:
        """返回各交付状态的计数快照。

        Returns:
            pending、leased、acked 和 dead 数量。
        """
        return self._execute("counts")

    def forget(self, task_id: str) -> bool:
        """移除终态任务和去重身份。

        Args:
            task_id: 待清理任务 ID。

        Returns:
            是否删除。
        """
        return bool(self._execute("forget", id=text(task_id, "task ID")))

    def acquire_init(
        self, owner: str, *, lease_seconds: float = 30
    ) -> InitLease | None:
        """申请初始化租约。

        Args:
            owner: 生产者身份。
            lease_seconds: 有效秒数。

        Returns:
            新凭据或 None。
        """
        data = self._execute(
            "acquire_init",
            owner=text(owner, "owner"),
            ttl=seconds(lease_seconds),
            token=uuid4().hex,
        )
        return None if data is None else self._init_lease(data)

    def _init_lease(self, data: dict[str, Any]) -> InitLease:
        """重建初始化凭据。

        Args:
            data: 存储快照。

        Returns:
            初始化凭据。
        """
        return InitLease(
            self.name, data["owner"], data["token"], data["fence"], data["deadline"]
        )

    def renew_init(self, lease: InitLease, *, lease_seconds: float = 30) -> InitLease:
        """延长初始化租约。

        Args:
            lease: 当前凭据。
            lease_seconds: 新有效秒数。

        Returns:
            新截止时间的凭据。
        """
        return self._init_lease(
            self._execute(
                "renew_init", **self._credentials(lease), ttl=seconds(lease_seconds)
            )
        )

    def save_init(self, lease: InitLease, progress: Any) -> None:
        """保存与当前租约绑定的 JSON 断点。

        Args:
            lease: 当前凭据。
            progress: JSON 进度值。
        """
        self._execute(
            "save_init", **self._credentials(lease), progress=encode(progress)
        )

    def finish_init(self, lease: InitLease, *, success: bool, error: str = "") -> None:
        """标记生产完成或失败，保留最终断点。

        Args:
            lease: 当前凭据。
            success: 是否成功。
            error: 失败原因。

        Raises:
            TypeError: success 不是布尔值。
        """
        if type(success) is not bool:
            raise TypeError("success must be a boolean")
        self._execute(
            "finish_init",
            **self._credentials(lease),
            success=success,
            error=self._error(error),
        )

    def initialization(self) -> InitRecord | None:
        """返回初始化状态和独立进度副本。

        Returns:
            初始化记录或 None。
        """
        data = self._execute("initialization")
        return (
            None
            if data is None
            else InitRecord(
                data["status"],
                self._init_lease(data),
                json.loads(data["progress"]),
                data["error"],
            )
        )
