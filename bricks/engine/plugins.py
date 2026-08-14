"""统一、受控的 Runtime 插件宿主。"""

from __future__ import annotations

import re
from collections import defaultdict
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any, Protocol

from .core import require_non_empty_string

_VERSION = re.compile(r"^[0-9]+(?:\.[0-9]+){0,2}(?:[-+][A-Za-z0-9.-]+)?$")


@dataclass(frozen=True, slots=True)
class PluginDescriptor:
    """插件身份、依赖以及声明提供的能力。"""

    id: str
    version: str
    requires: tuple[str, ...] = ()
    provides: tuple[str, ...] = ()
    api_version: str = "1"

    def __post_init__(self) -> None:
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
            ("provided capability", self.provides),
        ):
            if not isinstance(values, tuple):
                raise TypeError(f"{label}s must be a tuple")
            normalized = tuple(require_non_empty_string(value, label) for value in values)
            if len(set(normalized)) != len(normalized):
                raise ValueError(f"duplicate {label}")
        if plugin_id in self.requires:
            raise ValueError("plugin must not require itself")


class Plugin(Protocol):
    """一个可以向宿主贡献能力并参与生命周期的插件。"""

    descriptor: PluginDescriptor

    def setup(self, context: PluginContext) -> None:
        """注册能力；此阶段不能使用尚未启动的外部资源。"""

    def start(self, context: PluginContext) -> None:
        """在全部插件完成 setup 后启动。"""

    def stop(self, context: PluginContext) -> None:
        """释放资源；宿主按照启动顺序的逆序调用。"""


@dataclass(frozen=True, slots=True)
class Contribution:
    plugin: str
    capability: str
    name: str
    value: Any = field(compare=False, repr=False)


@dataclass(frozen=True, slots=True)
class NodeHookContribution:
    """一个带作用域的动态 Node Hook 贡献。"""

    hook: Any = field(compare=False, repr=False)
    phase: Any = None
    graph: str | None = None
    node: str | None = None

    def __post_init__(self) -> None:
        if self.node is not None and self.graph is None:
            raise ValueError("node-scoped hook contribution requires graph")
        if self.graph is not None:
            require_non_empty_string(self.graph, "hook graph")
        if self.node is not None:
            require_non_empty_string(self.node, "hook node")


class RegistrationHandle:
    """控制一项尚未冻结的能力注册。"""

    __slots__ = ("_detach", "_detached")

    def __init__(self, detach: Callable[[], None]) -> None:
        self._detach = detach
        self._detached = False

    def detach(self) -> None:
        if not self._detached:
            self._detach()
            self._detached = True

    def __enter__(self) -> RegistrationHandle:
        return self

    def __exit__(self, *args: object) -> None:
        del args
        self.detach()


class PluginContext:
    """插件可见的最小宿主接口。"""

    __slots__ = ("_host", "_plugin")

    def __init__(self, host: PluginHost, plugin: str) -> None:
        self._host = host
        self._plugin = plugin

    @property
    def plugin(self) -> str:
        return self._plugin

    def provide(self, capability: str, value: Any) -> RegistrationHandle:
        """提供一个排他的单例能力。"""

        return self._host._provide(self._plugin, capability, value)

    def contribute(
        self,
        capability: str,
        name: str,
        value: Any,
    ) -> RegistrationHandle:
        """向可聚合能力提供一个具名贡献。"""

        return self._host._contribute(self._plugin, capability, name, value)

    def require(self, capability: str) -> Any:
        """取得已经完成 setup 的单例能力。"""

        return self._host.require(capability)

    def contributions(self, capability: str) -> tuple[Contribution, ...]:
        return self._host.contributions(capability)


class PluginHost:
    """解析插件依赖，并管理统一能力注册和生命周期。"""

    API_VERSION = "1"

    def __init__(self, plugins: Iterable[Plugin]) -> None:
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
        return tuple(descriptor for descriptor, _ in self._plugins)

    def start(self) -> PluginHost:
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
                actual.update(
                    capability
                    for capability, entries in self._contributions.items()
                    if any(item.plugin == descriptor.id for item in entries.values())
                )
                missing = set(descriptor.provides) - actual
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
        capability = require_non_empty_string(capability, "capability")
        try:
            return self._capabilities[capability][1]
        except KeyError as exc:
            raise LookupError(f"required capability {capability!r} is unavailable") from exc

    def contributions(self, capability: str) -> tuple[Contribution, ...]:
        capability = require_non_empty_string(capability, "capability")
        return tuple(self._contributions.get(capability, {}).values())

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        failure = self._stop_started()
        if failure is not None:
            raise failure

    def _provide(self, plugin: str, capability: str, value: Any) -> RegistrationHandle:
        self._ensure_registering()
        capability = require_non_empty_string(capability, "capability")
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
        self._ensure_registering()
        capability = require_non_empty_string(capability, "capability")
        name = require_non_empty_string(name, "contribution name")
        if "/" not in name:
            raise ValueError("contribution name must be namespaced")
        entries = self._contributions[capability]
        if name in entries:
            raise ValueError(f"contribution {capability!r}/{name!r} already exists")
        entries[name] = Contribution(plugin, capability, name, value)
        return RegistrationHandle(lambda: self._remove_contribution(plugin, capability, name))

    def _remove_capability(self, plugin: str, capability: str) -> None:
        self._ensure_registering()
        if self._capabilities.get(capability, (None,))[0] == plugin:
            del self._capabilities[capability]

    def _remove_contribution(self, plugin: str, capability: str, name: str) -> None:
        self._ensure_registering()
        entry = self._contributions.get(capability, {}).get(name)
        if entry is not None and entry.plugin == plugin:
            del self._contributions[capability][name]

    def _ensure_registering(self) -> None:
        if self._frozen or self._closed:
            raise RuntimeError("plugin registrations are frozen")

    def _stop_started(self) -> BaseException | None:
        failure: BaseException | None = None
        for descriptor, plugin in reversed(self._started):
            try:
                plugin.stop(self._contexts[descriptor.id])
            except BaseException as exc:
                if failure is None:
                    failure = exc
        self._started.clear()
        return failure

    @staticmethod
    def _normalize(plugins: Iterable[Plugin]) -> tuple[tuple[PluginDescriptor, Plugin], ...]:
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

        ordered: list[tuple[PluginDescriptor, Plugin]] = []
        visiting: set[str] = set()
        visited: set[str] = set()

        def visit(plugin_id: str) -> None:
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
            visiting.remove(plugin_id)
            visited.add(plugin_id)
            ordered.append((descriptor, plugin))

        for plugin_id in candidates:
            visit(plugin_id)
        return tuple(ordered)


CAP_EVENT_BUS = "bricks.runtime/event-bus"
CAP_TASK_BACKEND = "bricks.runtime/task-backend"
CAP_GRAPH_EXECUTOR = "bricks.runtime/graph-executor"
CAP_EVENT_ROUTER = "bricks.runtime/event-router"
CAP_GRAPH_WORKER = "bricks.runtime/graph-worker"
CAP_INPUT_SELECTOR = "bricks.extension/input-selector"
CAP_NODE_HOOK = "bricks.extension/node-hook"
CAP_RUNTIME_OBSERVER = "bricks.extension/runtime-observer"


class ExtensionPlugin:
    """把常见 Policy、Hook 和 Observer 组合成一个声明式插件。"""

    def __init__(
        self,
        plugin_id: str,
        *,
        version: str = "1.0.0",
        requires: tuple[str, ...] = (),
        selectors: Mapping[str, Any] | None = None,
        hooks: Mapping[str, Any] | None = None,
        observers: Mapping[str, Any] | None = None,
    ) -> None:
        self._selectors = {} if selectors is None else dict(selectors)
        self._hooks = {} if hooks is None else dict(hooks)
        self._observers = {} if observers is None else dict(observers)
        provides = tuple(
            capability
            for capability, entries in (
                (CAP_INPUT_SELECTOR, self._selectors),
                (CAP_NODE_HOOK, self._hooks),
                (CAP_RUNTIME_OBSERVER, self._observers),
            )
            if entries
        )
        self.descriptor = PluginDescriptor(plugin_id, version, requires, provides)

    def setup(self, context: PluginContext) -> None:
        for name, selector in self._selectors.items():
            context.contribute(CAP_INPUT_SELECTOR, name, selector)
        for name, hook in self._hooks.items():
            context.contribute(CAP_NODE_HOOK, name, hook)
        for name, observer in self._observers.items():
            context.contribute(CAP_RUNTIME_OBSERVER, name, observer)

    def start(self, context: PluginContext) -> None:
        del context

    def stop(self, context: PluginContext) -> None:
        del context
