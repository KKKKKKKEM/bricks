"""Air 的显式流程信号，由领域节点或领取边界捕获，不交给核心解释。"""

from bricks.models._validation import duration


class Signal(BaseException):
    """领域流程中断基类，避免被业务代码的 except Exception 吞掉。"""


class Retry(Signal):
    """结束当前回调并请求图内有界重试。

    Attributes:
        delay: 本次额外重试等待秒数；None 沿用 Spider 配置。
    """

    def __init__(self, *, delay: float | None = None) -> None:
        """校验本次重试的等待时间。

        Args:
            delay: 非负秒数；None 沿用默认值。
        """
        super().__init__()
        self.delay = duration(delay, "delay", allow_zero=True)


class Success(Signal):
    """结束当前任务并走成功收尾，收尾完成前不确认交付。"""


class Failure(Signal):
    """结束当前任务并报告业务失败，可通过异常参数提供原因。"""


class Skip(Signal):
    """跳过当前 before 阶段对应的业务操作，仍执行匹配的 after 阶段。"""


class Pass(Signal):
    """停止当前插槽剩余回调，继续该插槽的正常后继节点。"""


class Wait(Signal):
    """在领取前插槽延后下次轮询，不阻塞在途任务的续租。

    Attributes:
        duration: 延迟秒数，默认一秒。
    """

    def __init__(self, duration: float = 1) -> None:
        """保存经过校验的等待秒数。

        Args:
            duration: 非负秒数，默认一秒。
        """
        super().__init__()
        from bricks.models._validation import duration as validate_duration

        validate_duration(duration, "duration", allow_zero=True)
        self.duration = duration
