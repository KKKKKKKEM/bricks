# -*- coding: utf-8 -*-
"""
验证 declaration-based hook registration 设计
"""
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from bricks.core.events import EventManager, REGISTERED_EVENTS, EventSpec, Task, on, _get_event_spec, _get_event_specs, _has_event_spec
from bricks.core.context import Context, Flow
from bricks.core import genesis

# 跟踪调用结果
calls = []


def reset():
    calls.clear()


# ============================================================
# 1. 模块级函数：立即全局注册（向后兼容）
# ============================================================
@on("module_level_event")
def module_handler(context):
    calls.append(("module", context.form))


# ============================================================
# 2. Pangu 子类：实例方法 / staticmethod / classmethod
# ============================================================
class MyPangu(genesis.Pangu):
    _test_flows = {}

    @property
    def flows(self):
        return self._test_flows

    # 实例方法
    @on("instance_event")
    def my_instance_method(self, context):
        calls.append(("instance", self.__class__.__name__))

    # @staticmethod 作为外层, @events.on 作为内层（推荐顺序）
    @staticmethod
    @on("static_event")
    def my_static_method(context):
        calls.append(("static", "ok"))

    # @classmethod 作为外层, @events.on 作为内层（推荐顺序）
    @classmethod
    @on("classmethod_event")
    def my_classmethod(cls, context):
        calls.append(("classmethod", cls.__name__))

    # @events.on 作为外层, @staticmethod 作为内层（也应支持）
    @on("static_event_outer")
    @staticmethod
    def my_static_outer(context):
        calls.append(("static_outer", "ok"))

    # @events.on 作为外层, @classmethod 作为内层（也应支持）
    @on("classmethod_event_outer")
    @classmethod
    def my_classmethod_outer(cls, context):
        calls.append(("classmethod_outer", cls.__name__))

    # 带 index 和 kwargs 的装饰
    @on("indexed_event", index=5, kwargs={"extra": "data"})
    def my_indexed(self, context):
        calls.append(("indexed", self.__class__.__name__))

    # 多 spec 装饰：一个方法声明两个 @on()，两个 form 均应触发
    @on("multi_event_a")
    @on("multi_event_b")
    def my_multi(self, context):
        calls.append(("multi", context.form))


# ============================================================
# 3. Chaos 子类：有装饰方法但没有 use()，实例化不应报错
# ============================================================
class MyChaos(genesis.Chaos):
    @on("chaos_event")
    def chaos_method(self, context):
        calls.append(("chaos", "should_not_run"))


# ============================================================
# 4. 继承测试
# ============================================================
class ChildPangu(MyPangu):
    # 重写实例方法
    @on("instance_event")
    def my_instance_method(self, context):
        calls.append(("child_instance", self.__class__.__name__))

    # 未重写的方法（如 my_static_method）应继承父类的装饰


# ============================================================
# 运行测试
# ============================================================
def test_module_level():
    """模块级函数应已立即全局注册"""
    reset()
    ctx = Context(form="module_level_event", target=None)
    list(EventManager.trigger(ctx))
    assert len(calls) == 1, f"Expected 1 module call, got {len(calls)}: {calls}"
    assert calls[0] == ("module", "module_level_event"), f"Got {calls[0]}"
    print("[PASS] module-level immediate registration")


def test_pangu_instance_method():
    """实例方法绑定后应正确接收 self"""
    reset()
    p = MyPangu()
    ctx = p.make_context(form="instance_event")
    list(EventManager.trigger(ctx))
    assert ("instance", "MyPangu") in calls, f"Missing instance call: {calls}"
    print("[PASS] Pangu instance method (self bound correctly)")


def test_pangu_static_method():
    """staticmethod 装饰的方法应正常触发"""
    reset()
    p = MyPangu()
    ctx = p.make_context(form="static_event")
    list(EventManager.trigger(ctx))
    assert ("static", "ok") in calls, f"Missing static call: {calls}"
    print("[PASS] Pangu @staticmethod (inner @events.on)")


def test_pangu_classmethod():
    """classmethod 装饰的方法应正常触发"""
    reset()
    p = MyPangu()
    ctx = p.make_context(form="classmethod_event")
    list(EventManager.trigger(ctx))
    assert ("classmethod", "MyPangu") in calls, f"Missing classmethod call: {calls}"
    print("[PASS] Pangu @classmethod (inner @events.on)")


def test_pangu_static_outer():
    """@events.on 外层 + @staticmethod 内层应正常工作"""
    reset()
    p = MyPangu()
    ctx = p.make_context(form="static_event_outer")
    list(EventManager.trigger(ctx))
    assert ("static_outer", "ok") in calls, f"Missing static_outer call: {calls}"
    print("[PASS] Pangu @events.on outer + @staticmethod inner")


def test_pangu_classmethod_outer():
    """@events.on 外层 + @classmethod 内层应正常工作"""
    reset()
    p = MyPangu()
    ctx = p.make_context(form="classmethod_event_outer")
    list(EventManager.trigger(ctx))
    assert ("classmethod_outer", "MyPangu") in calls, f"Missing classmethod_outer call: {calls}"
    print("[PASS] Pangu @events.on outer + @classmethod inner")


def test_indexed_kwargs():
    """带 index/kwargs 的装饰应正确注册"""
    reset()
    p = MyPangu()
    ctx = p.make_context(form="indexed_event")
    list(EventManager.trigger(ctx))
    assert ("indexed", "MyPangu") in calls, f"Missing indexed call: {calls}"
    print("[PASS] Pangu indexed event with kwargs")


def test_multi_spec():
    """单个方法声明两个 @on() 应为两个 form 分别注册并触发"""
    reset()
    p = MyPangu()

    # 触发 multi_event_a
    ctx_a = p.make_context(form="multi_event_a")
    list(EventManager.trigger(ctx_a))
    assert ("multi", "multi_event_a") in calls, f"Missing multi_event_a call: {calls}"

    # 触发 multi_event_b
    ctx_b = p.make_context(form="multi_event_b")
    list(EventManager.trigger(ctx_b))
    assert ("multi", "multi_event_b") in calls, f"Missing multi_event_b call: {calls}"

    print("[PASS] Pangu multi-spec method fires for both forms")


def test_multi_spec_metadata():
    """验证 _get_event_specs 返回全部 spec 元数据"""
    raw = MyPangu.__dict__["my_multi"]
    specs = _get_event_specs(raw)
    assert len(specs) == 2, f"Expected 2 specs, got {len(specs)}"
    forms = {s.form for s in specs}
    assert forms == {"multi_event_a", "multi_event_b"}, f"Unexpected forms: {forms}"

    # 向后兼容：_get_event_spec 仍返回最后一个声明（外层装饰器）
    single = _get_event_spec(raw)
    assert single is not None, "_get_event_spec should return the last spec"
    assert single.form == "multi_event_a"
    print("[PASS] _get_event_specs returns all specs; _get_event_spec returns last")


def test_chaos_no_crash():
    """Chaos 子类有装饰方法但无 use()，实例化不应报错"""
    reset()
    try:
        c = MyChaos()
        print("[PASS] Chaos instantiation with @events.on method (no crash)")
    except Exception as e:
        print(f"[FAIL] Chaos instantiation crashed: {e}")
        raise


def test_chaos_hook_not_registered():
    """Chaos 子类的装饰方法不应被注册"""
    reset()
    c = MyChaos()
    ctx = Context(form="chaos_event", target=None)
    list(EventManager.trigger(ctx))
    assert len(calls) == 0, f"Chaos hook should not be registered, but got: {calls}"
    print("[PASS] Chaos @events.on hook NOT registered (no use())")


def test_inheritance():
    """子类重写的方法应使用子类版本，未重写的应继承"""
    reset()
    p = ChildPangu()

    # 子类重写的实例方法
    ctx = p.make_context(form="instance_event")
    list(EventManager.trigger(ctx))
    assert ("child_instance", "ChildPangu") in calls, f"Missing child override: {calls}"
    # 父类版本不应出现
    assert ("instance", "MyPangu") not in calls, f"Parent version should be overridden: {calls}"

    # 继承的 staticmethod
    reset()
    ctx = p.make_context(form="static_event")
    list(EventManager.trigger(ctx))
    assert ("static", "ok") in calls, f"Missing inherited static: {calls}"

    print("[PASS] Inheritance: override works, inherited hooks preserved")


def test_event_spec_metadata():
    """验证 __event_spec__ 元数据被正确附加"""
    spec = _get_event_spec(MyPangu.__dict__["my_instance_method"])
    assert spec is not None, "my_instance_method should have EventSpec"
    assert spec.form == "instance_event"
    assert spec.index is None

    spec_static = _get_event_spec(MyPangu.__dict__["my_static_method"])
    assert spec_static is not None, "my_static_method should have EventSpec"
    assert spec_static.form == "static_event"

    spec_cls = _get_event_spec(MyPangu.__dict__["my_classmethod"])
    assert spec_cls is not None, "my_classmethod should have EventSpec"
    assert spec_cls.form == "classmethod_event"

    print("[PASS] EventSpec metadata correctly attached")


if __name__ == "__main__":
    test_module_level()
    test_event_spec_metadata()
    test_chaos_no_crash()
    test_chaos_hook_not_registered()
    test_pangu_instance_method()
    test_pangu_static_method()
    test_pangu_classmethod()
    test_pangu_static_outer()
    test_pangu_classmethod_outer()
    test_indexed_kwargs()
    test_multi_spec()
    test_multi_spec_metadata()
    test_inheritance()
    print("\n" + "=" * 50)
    print("ALL TESTS PASSED")
    print("=" * 50)
