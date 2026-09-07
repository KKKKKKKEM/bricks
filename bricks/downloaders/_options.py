"""下载器共享的传输配置校验。"""

from dataclasses import dataclass


@dataclass(frozen=True)
class TransportOptions:
    """不可变的 TLS、环境代理和重定向配置。"""

    verify: bool = True  # 是否校验 TLS 证书，默认启用。
    trust_env: bool = False  # 是否读取环境代理，默认禁用；证书行为由具体库决定。
    max_redirects: int = 20  # 单次下载允许跟随的最大重定向次数。

    def __post_init__(self) -> None:
        """在网络调用前校验传输配置。

        Raises:
            TypeError: verify 或 trust_env 不是布尔值。
            ValueError: max_redirects 不是非负整数。
        """

        if type(self.verify) is not bool or type(self.trust_env) is not bool:
            raise TypeError("verify and trust_env must be booleans")
        if type(self.max_redirects) is not int or self.max_redirects < 0:
            raise ValueError("max_redirects must be a non-negative integer")
