"""统一、受控的 Runtime 插件宿主。"""

from __future__ import annotations

import re
from collections import defaultdict
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any, Protocol

from ..engine.core import require_non_empty_string
from ..engine.hooks import HookPhase, NodeHook
from ..engine.observation import RuntimeObserver
from ..engine.policies import InputSelector

_VERSION = re.compile(r"^[0-9]+(?:\.[0-9]+){0,2}(?:[-+][A-Za-z0-9.-]+)?$")


@dataclass(frozen=True, slots=True)
class PluginDescriptor:
    """插件身份、依赖以及声明提供的能力。

    Attributes:
        id: 当前对象的唯一标识。
        version: 插件版本。
        requires: 必须先装配的插件标识集合。
        provides: 插件声明提供的单例能力集合。
        api_version: 插件声明兼容的 SPI 主版本。
        requires_capabilities: 插件启动前必须存在的能力集合。
        contributes: 插件声明提供的聚合贡献类别。
    """

    id: str
    version: str
    requires: tuple[str, ...] = ()
    provides: tuple[str, ...] = ()
    api_version: str = "1"
    requires_capabilities: tuple[str, ...] = ()
    contributes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        """校验构造字段并固定需要保持不变的数据。

        Raises:
            TypeError: 参数类型或接口实现不符合当前契约。
            ValueError: 参数值或字段组合不合法。
        """

        plugin_id = require_non_empty_string(self.id, "plugin id")
        if "/" not in plugin_id:
            raise ValueError("plugin id must be namespaced")
        version = require_non_empty_string(self.version, "plugin version")
        if not _VERSION.match(version):
            raise ValueError("plugin version must use a simple semantic version")
        api_version = require_non_empty_string(self.api_version, "plugin API version")
        if not api_version.isdigit() or int(api_version) < 1:
            raise ValueError("plugin API version must be a positive integer")
        for label, values in (
            ("plugin requirement", self.requires),
            ("required capability", self.requires_capabilities),
            ("provided capability", self.provides),
            ("contributed capability", self.contributes),
        ):
            if not isinstance(values, tuple):
                raise TypeError(f"{label}s must be a tuple")
            normalized = tuple(
                require_non_empty_string(value, label) for value in values
            )
            if len(set(normalized)) != len(normalized):
                raise ValueError(f"duplicate {label}")
        if plugin_id in self.requires:
            raise ValueError("plugin must not require itself")
        if set(self.provides) & set(self.contributes):
            raise ValueError("a capability cannot be both singleton and aggregate")
        overlap = set(self.requires_capabilities) & set(self.provides)
        if overlap:
            raise ValueError(
                f"plugin must not require capabilities it provides: {sorted(overlap)!r}"
            )


class Plugin(Protocol):
    """一个可以向宿主贡献能力并参与生命周期的插件。

    Attributes:
        descriptor: 插件身份、依赖和能力声明。
    """

    descriptor: PluginDescriptor

    def setup(self, context: PluginContext) -> None:
        """注册能力；此阶段不能使用尚未启动的外部资源。

        Args:
            context: 当前调用的执行或插件上下文。
        """

    def start(self, context: PluginContext) -> None:
        """在全部插件完成 setup 后启动。

        Args:
            context: 当前调用的执行或插件上下文。
        """

    def stop(self, context: PluginContext) -> None:
        """释放资源；宿主按照启动顺序的逆序调用。

        Args:
            context: 当前调用的执行或插件上下文。
        """


@dataclass(frozen=True, slots=True)
class Contribution:
    """一个插件登记的具名聚合能力贡献。

    Attributes:
        plugin: 贡献所属的插件标识。
        capability: 当前贡献所属的能力类别。
        name: 当前注册项或具名策略的名称。
        value: 当前记录携带的数据值。
    """

    plugin: str
    capability: str
    name: str
    value: Any = field(compare=False, repr=False)


@dataclass(frozen=True, slots=True)
class NodeHookContribution:
    """一个带作用域的动态 Node Hook 贡献。

    Attributes:
        hook: 注册的节点 Hook 实例。
        phase: 函数 Hook 对应的执行阶段。
        graph: 关联的 Graph 定义或注册名称。
        node: 关联的节点实例或 Graph 内节点 ID。
    """

    hook: NodeHook | Callable[..., object] = field(compare=False, repr=False)
    phase: HookPhase | str | None = None
    graph: str | None = None
    node: str | None = None

    def __post_init__(self) -> None:
        """校验构造字段并固定需要保持不变的数据。

        Raises:
            TypeError: 参数类型或接口实现不符合当前契约。
            ValueError: 参数值或字段组合不合法。
        """

        if not isinstance(self.hook, NodeHook) and not callable(self.hook):
            raise TypeError("hook must be a NodeHook or callable")
        if isinstance(self.hook, NodeHook) and self.phase is not None:
            raise TypeError("phase is only valid for a function hook")
        if self.phase is not None:
            HookPhase(self.phase)
        if self.node is not None and self.graph is None:
            raise ValueError("node-scoped hook contribution requires graph")
        if self.graph is not None:
            require_non_empty_string(self.graph, "hook graph")
        if self.node is not None:
            require_non_empty_string(self.node, "hook node")


class RegistrationHandle:
    """控制一项尚未冻结的能力注册。

    Attributes:
        __slots__: 实例允许保存的字段名称，限制动态增加属性。
        _detach: 解除注册关系的回调。
        _detached: 是否已完成卸载，避免重复释放。
    """

    __slots__ = ("_detach", "_detached")

    def __init__(self, detach: Callable[[], None]) -> None:
        """保存可幂等调用的注册卸载回调。

        Args:
            detach: 执行注册项卸载的回调。
        """

        self._detach = detach
        self._detached = False

    def detach(self) -> None:
        """解除当前句柄对应的注册关系。"""

        if not self._detached:
            self._detach()
            self._detached = True

    def __enter__(self) -> RegistrationHandle:  # noqa: PYI034
        """进入资源作用域并返回当前句柄。

        Returns:
            当前资源管理对象。
        """

        return self

    def __exit__(self, *args: object) -> None:
        """退出资源作用域，执行对应的关闭或卸载操作。

        Args:
            *args: 调用协议传入的位置参数。
        """

        del args
        self.detach()


class PluginContext:
    """插件可见的最小宿主接口。

    Attributes:
        __slots__: 实例允许保存的字段名称，限制动态增加属性。
        _host: 当前上下文所属的插件宿主。
        _plugin: 当前上下文绑定的插件声明。
    """

    __slots__ = ("_host", "_plugin")

    def __init__(self, host: PluginHost, plugin: str) -> None:
        """将插件身份绑定到宿主公开的受控装配入口。

        Args:
            host: 装配插件和管理能力的宿主。
            plugin: 当前插件实例或其声明身份。
        """

        self._host = host
        self._plugin = plugin

    @property
    def plugin(self) -> str:
        """返回当前插件上下文绑定的插件声明。

        Returns:
            当前上下文绑定的插件声明。
        """

        return self._plugin

    def provide(self, capability: str, value: Any) -> RegistrationHandle:
        """提供一个排他的单例能力。

        Args:
            capability: 需要提供、取得或移除的能力名称。
            value: 插件提供的单例实现或具名贡献对象。

        Returns:
            用于卸载本次注册的句柄。
        """

        return self._host._provide(self._plugin, capability, value)

    def contribute(
        self,
        capability: str,
        name: str,
        value: Any,
    ) -> RegistrationHandle:
        """向可聚合能力提供一个具名贡献。

        Args:
            capability: 需要提供、取得或移除的能力名称。
            name: 注册或查找使用的名称。
            value: 插件提供的单例实现或具名贡献对象。

        Returns:
            用于卸载本次注册的句柄。
        """

        return self._host._contribute(self._plugin, capability, name, value)

    def require(self, capability: str) -> Any:
        """取得已经完成 setup 的单例能力。

        Args:
            capability: 需要提供、取得或移除的能力名称。

        Returns:
            指定能力的已装配实现。
        """

        return self._host.require(capability)

    def contributions(self, capability: str) -> tuple[Contribution, ...]:
        """取得当前上下文可见的聚合能力贡献。

        Args:
            capability: 需要提供、取得或移除的能力名称。

        Returns:
            当前能力或插件上下文可见的贡献快照。
        """

        return self._host.contributions(capability)


class PluginHost:
    """解析插件依赖，并管理统一能力注册和生命周期。

    Attributes:
        API_VERSION: 当前宿主支持的 SPI 主版本。
        _plugins: 按依赖顺序排列的插件实例。
        _capabilities: 已经注册的单例能力及其提供者。
        _contributions: 按能力类别组织的具名贡献。
        _contexts: 按插件身份保存的受控装配上下文。
        _started: 已接管生命周期、需要逆序关闭的插件集合。
        _frozen: 是否已完成冻结，冻结后不再接受定义修改。
        _closed: 当前组件是否已停止接受新工作。
    """

    API_VERSION = "1"

    def __init__(self, plugins: Iterable[Plugin]) -> None:
        """校验插件依赖并初始化能力、贡献和生命周期记录。

        Args:
            plugins: 待装配的插件实例集合。
        """

        self._plugins = self._normalize(plugins)
        self._capabilities: dict[str, tuple[str, Any]] = {}
        self._contributions: dict[str, dict[str, Contribution]] = defaultdict(dict)
        self._contexts = {
            descriptor.id: PluginContext(self, descriptor.id)
            for descriptor, _ in self._plugins
        }
        self._started: list[tuple[PluginDescriptor, Plugin]] = []
        self._frozen = False
        self._closed = False

    @property
    def descriptors(self) -> tuple[PluginDescriptor, ...]:
        """返回按装配顺序排列的插件声明快照。

        Returns:
            按装配顺序排列的插件声明。
        """

        return tuple(descriptor for descriptor, _ in self._plugins)

    def start(self) -> PluginHost:
        """按依赖顺序装配和启动插件，失败时执行逆序回滚。

        Returns:
            本次操作得到的 PluginHost 实例。

        Raises:
            RuntimeError: 当前生命周期状态或操作顺序不允许此操作。
        """

        if self._closed:
            raise RuntimeError("plugin host is closed")
        if self._frozen:
            return self
        try:
            for descriptor, plugin in self._plugins:
                # setup 即使中途失败也可能已经创建资源，因此先纳入回滚栈。
                self._started.append((descriptor, plugin))
                plugin.setup(self._contexts[descriptor.id])
                actual = {
                    capability
                    for capability, (owner, _) in self._capabilities.items()
                    if owner == descriptor.id
                }
                contributed = {
                    capability
                    for capability, entries in self._contributions.items()
                    if any(item.plugin == descriptor.id for item in entries.values())
                }
                missing = (set(descriptor.provides) - actual) | (
                    set(descriptor.contributes) - contributed
                )
                if missing:
                    raise RuntimeError(
                        f"plugin {descriptor.id!r} did not provide declared capabilities: "
                        f"{sorted(missing)!r}"
                    )
            self._frozen = True
            for descriptor, plugin in self._started:
                plugin.start(self._contexts[descriptor.id])
        except BaseException:
            self._frozen = True
            self._stop_started()
            self._closed = True
            raise
        return self

    def require(self, capability: str) -> Any:
        """按能力名称取得已经装配的单例实现。

        Args:
            capability: 需要提供、取得或移除的能力名称。

        Returns:
            指定能力的已装配实现。

        Raises:
            LookupError: 指定编码不存在或不是文本编码。
        """

        capability = require_non_empty_string(capability, "capability")
        try:
            return self._capabilities[capability][1]
        except KeyError as exc:
            raise LookupError(
                f"required capability {capability!r} is unavailable"
            ) from exc

    def contributions(self, capability: str) -> tuple[Contribution, ...]:
        """返回当前可见的具名能力贡献快照。

        Args:
            capability: 需要提供、取得或移除的能力名称。

        Returns:
            当前能力或插件上下文可见的贡献快照。
        """

        capability = require_non_empty_string(capability, "capability")
        return tuple(self._contributions.get(capability, {}).values())

    def close(self) -> None:
        """逆序关闭已接管的插件，单个关闭失败不跳过后续插件。"""

        if self._closed:
            return
        self._closed = True
        failure = self._stop_started()
        if failure is not None:
            raise failure

    def _provide(self, plugin: str, capability: str, value: Any) -> RegistrationHandle:
        """登记插件提供的单例能力并检查声明与冲突。

        Args:
            plugin: 当前插件实例或其声明身份。
            capability: 需要提供、取得或移除的能力名称。
            value: 插件提供的单例实现或具名贡献对象。

        Returns:
            用于卸载本次注册的句柄。

        Raises:
            ValueError: 参数值或字段组合不合法。
        """

        self._ensure_registering()
        capability = require_non_empty_string(capability, "capability")
        self._ensure_declared(plugin, capability, aggregate=False)
        existing = self._capabilities.get(capability)
        if existing is not None:
            raise ValueError(
                f"capability {capability!r} is already provided by {existing[0]!r}"
            )
        self._capabilities[capability] = (plugin, value)
        return RegistrationHandle(lambda: self._remove_capability(plugin, capability))

    def _contribute(
        self, plugin: str, capability: str, name: str, value: Any
    ) -> RegistrationHandle:
        """向宿主登记一项具名的聚合能力贡献。

        Args:
            plugin: 当前插件实例或其声明身份。
            capability: 需要提供、取得或移除的能力名称。
            name: 注册或查找使用的名称。
            value: 插件提供的单例实现或具名贡献对象。

        Returns:
            用于卸载本次注册的句柄。

        Raises:
            ValueError: 参数值或字段组合不合法。
        """

        self._ensure_registering()
        capability = require_non_empty_string(capability, "capability")
        self._ensure_declared(plugin, capability, aggregate=True)
        name = require_non_empty_string(name, "contribution name")
        if "/" not in name:
            raise ValueError("contribution name must be namespaced")
        entries = self._contributions[capability]
        if name in entries:
            raise ValueError(f"contribution {capability!r}/{name!r} already exists")
        entries[name] = Contribution(plugin, capability, name, value)
        return RegistrationHandle(
            lambda: self._remove_contribution(plugin, capability, name)
        )

    def _ensure_declared(
        self, plugin: str, capability: str, *, aggregate: bool
    ) -> None:
        """检查插件是否兑现全部声明的单例和聚合能力。

        Args:
            plugin: 当前插件实例或其声明身份。
            capability: 需要提供、取得或移除的能力名称。
            aggregate: 是否读取同一能力的多个具名贡献。

        Raises:
            ValueError: 参数值或字段组合不合法。
        """

        descriptor = next(item for item, _ in self._plugins if item.id == plugin)
        declared = descriptor.contributes if aggregate else descriptor.provides
        if capability not in declared:
            raise ValueError(
                f"plugin {plugin!r} did not declare capability {capability!r}"
            )

    def _remove_capability(self, plugin: str, capability: str) -> None:
        """回滚插件登记的单例能力。

        Args:
            plugin: 当前插件实例或其声明身份。
            capability: 需要提供、取得或移除的能力名称。
        """

        self._ensure_registering()
        if self._capabilities.get(capability, (None,))[0] == plugin:
            del self._capabilities[capability]

    def _remove_contribution(self, plugin: str, capability: str, name: str) -> None:
        """回滚插件登记的具名贡献。

        Args:
            plugin: 当前插件实例或其声明身份。
            capability: 需要提供、取得或移除的能力名称。
            name: 注册或查找使用的名称。
        """

        self._ensure_registering()
        entry = self._contributions.get(capability, {}).get(name)
        if entry is not None and entry.plugin == plugin:
            del self._contributions[capability][name]

    def _ensure_registering(self) -> None:
        """确保插件宿主仍处于允许注册能力的阶段。

        Raises:
            RuntimeError: 当前生命周期状态或操作顺序不允许此操作。
        """

        if self._frozen or self._closed:
            raise RuntimeError("plugin registrations are frozen")

    def _stop_started(self) -> BaseException | None:
        """按启动顺序的逆序停止插件，并收集关闭失败。

        Returns:
            关闭过程中最先出现的异常，全部成功时为 None。
        """

        failure: BaseException | None = None
        for descriptor, plugin in reversed(self._started):
            try:
                plugin.stop(self._contexts[descriptor.id])
            except BaseException as exc:  # noqa: BLE001
                if failure is None:
                    failure = exc
        self._started.clear()
        return failure

    @staticmethod
    def _normalize(
        plugins: Iterable[Plugin],
    ) -> tuple[tuple[PluginDescriptor, Plugin], ...]:
        """校验插件声明并按依赖关系确定装配顺序。

        Args:
            plugins: 待装配的插件实例集合。

        Returns:
            按依赖顺序排列的插件声明与实例对。

        Raises:
            TypeError: 参数类型或接口实现不符合当前契约。
            ValueError: 参数值或字段组合不合法。
        """

        candidates: dict[str, tuple[PluginDescriptor, Plugin]] = {}
        try:
            values = tuple(plugins)
        except TypeError as exc:
            raise TypeError("plugins must be iterable") from exc
        for plugin in values:
            descriptor = getattr(plugin, "descriptor", None)
            if not isinstance(descriptor, PluginDescriptor):
                raise TypeError("plugin must define a PluginDescriptor")
            for method in ("setup", "start", "stop"):
                if not callable(getattr(plugin, method, None)):
                    raise TypeError(f"plugin must define {method}()")
            if descriptor.id in candidates:
                raise ValueError(f"duplicate plugin {descriptor.id!r}")
            if descriptor.api_version != PluginHost.API_VERSION:
                raise ValueError(
                    f"plugin {descriptor.id!r} requires API {descriptor.api_version}, "
                    f"host provides {PluginHost.API_VERSION}"
                )
            candidates[descriptor.id] = (descriptor, plugin)

        providers: dict[str, str] = {}
        aggregates = {
            capability
            for descriptor, _ in candidates.values()
            for capability in descriptor.contributes
        }
        for descriptor, _ in candidates.values():
            for capability in descriptor.provides:
                if capability in aggregates:
                    raise ValueError(
                        f"capability {capability!r} cannot be both singleton and aggregate"
                    )
                existing = providers.get(capability)
                if existing is not None:
                    raise ValueError(
                        f"capability {capability!r} is already provided by "
                        f"{existing!r}; {descriptor.id!r} also declares it"
                    )
                providers[capability] = descriptor.id

        for descriptor, _ in candidates.values():
            for capability in descriptor.requires_capabilities:
                if capability not in providers:
                    raise ValueError(
                        f"missing required capability {capability!r} "
                        f"for plugin {descriptor.id!r}"
                    )

        ordered: list[tuple[PluginDescriptor, Plugin]] = []
        visiting: set[str] = set()
        visited: set[str] = set()

        def visit(plugin_id: str) -> None:
            """递归访问插件依赖，并检测循环依赖。

            Args:
                plugin_id: 带命名空间的插件标识。

            Raises:
                ValueError: 参数值或字段组合不合法。
            """

            if plugin_id in visiting:
                raise ValueError(f"cyclic plugin dependency involving {plugin_id!r}")
            if plugin_id in visited:
                return
            try:
                descriptor, plugin = candidates[plugin_id]
            except KeyError as exc:
                raise ValueError(f"missing required plugin {plugin_id!r}") from exc
            visiting.add(plugin_id)
            for required in descriptor.requires:
                visit(required)
            for capability in descriptor.requires_capabilities:
                visit(providers[capability])
            visiting.remove(plugin_id)
            visited.add(plugin_id)
            ordered.append((descriptor, plugin))

        for plugin_id in candidates:
            visit(plugin_id)
        return tuple(ordered)


CAP_EVENT_BUS = "bricks.runtime/event-bus"
CAP_TASK_BACKEND = "bricks.runtime/task-backend"
CAP_GRAPH_EXECUTOR = "bricks.runtime/graph-executor"
CAP_EXECUTION_FACTORY = "bricks.runtime/execution-factory"
CAP_EVENT_ROUTER = "bricks.runtime/event-router"
CAP_GRAPH_WORKER = "bricks.runtime/graph-worker"
CAP_INPUT_SELECTOR = "bricks.contribution/input-selector"
CAP_NODE_HOOK = "bricks.contribution/node-hook"
CAP_RUNTIME_OBSERVER = "bricks.contribution/runtime-observer"


class ContributionPlugin:
    """把常见 Policy、Hook 和 Observer 组合成一个声明式插件。

    Attributes:
        _selectors: 带命名空间的输入策略实现映射。
        _hooks: 准备贡献的具名节点 Hook。
        _observers: 按注册顺序排列的运行时观察者快照。
        descriptor: 插件身份、依赖和能力声明。
    """

    def __init__(
        self,
        plugin_id: str,
        *,
        version: str = "1.0.0",
        requires: tuple[str, ...] = (),
        selectors: Mapping[str, InputSelector] | None = None,
        hooks: Mapping[str, NodeHookContribution | NodeHook | Callable[..., object]]
        | None = None,
        observers: Mapping[str, RuntimeObserver] | None = None,
    ) -> None:
        """固定待注册的策略、Hook 和观察者贡献。

        Args:
            plugin_id: 带命名空间的插件标识。
            version: 等待开始前观察到的通知版本。
            requires: 当前插件依赖的插件标识集合。
            selectors: 待注册的具名输入选择器。
            hooks: 待装配的节点 Hook 集合。
            observers: 待装配的运行时观察者集合。
        """

        self._selectors = {} if selectors is None else dict(selectors)
        self._hooks = {} if hooks is None else dict(hooks)
        self._observers = {} if observers is None else dict(observers)
        contributes = tuple(
            capability
            for capability, entries in (
                (CAP_INPUT_SELECTOR, self._selectors),
                (CAP_NODE_HOOK, self._hooks),
                (CAP_RUNTIME_OBSERVER, self._observers),
            )
            if entries
        )
        self.descriptor = PluginDescriptor(
            plugin_id,
            version,
            requires=requires,
            contributes=contributes,
        )

    def setup(self, context: PluginContext) -> None:
        """在插件装配阶段登记声明的能力或贡献。

        Args:
            context: 当前调用的执行或插件上下文。
        """

        for name, selector in self._selectors.items():
            context.contribute(CAP_INPUT_SELECTOR, name, selector)
        for name, hook in self._hooks.items():
            context.contribute(CAP_NODE_HOOK, name, hook)
        for name, observer in self._observers.items():
            context.contribute(CAP_RUNTIME_OBSERVER, name, observer)

    def start(self, context: PluginContext) -> None:
        """完成纯贡献插件的启动阶段，无额外外部资源需要启动。

        Args:
            context: 当前调用的执行或插件上下文。
        """

        del context

    def stop(self, context: PluginContext) -> None:
        """完成纯贡献插件的停止阶段，无额外外部资源需要释放。

        Args:
            context: 当前调用的执行或插件上下文。
        """

        del context
