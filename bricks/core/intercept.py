# -*- coding: utf-8 -*-
# @Time    : 2026-07-11
# @Author  : Kem
# @Desc    : 显式拦截器装饰器

import functools
from typing import Callable


def intercept(target_method: str):
    """
    显式声明拦截器装饰器。

    用法::

        class MySpider(Spider):
            @intercept("on_request")
            def wrap_on_request(self, raw_method):
                def wrapper(context, *args, **kwargs):
                    # 前置逻辑
                    result = raw_method(context, *args, **kwargs)
                    # 后置逻辑
                    return result
                return wrapper

    :param target_method: 要拦截的目标方法名
    """
    def decorator(wrapper_func: Callable):
        @functools.wraps(wrapper_func)
        def inner(*args, **kwargs):
            return wrapper_func(*args, **kwargs)
        # 在 functools.wraps 之后设置，避免被覆盖
        inner.__intercept_target__ = target_method
        return inner
    return decorator


def collect_interceptors(instance) -> dict:
    """
    从实例上收集所有 @intercept 装饰的拦截器。

    沿 MRO 遍历类字典，直接读取描述符元数据，
    不对实例做 getattr，避免触发 @property 副作用。

    返回: {target_method_name: wrapper_method_name}
    """
    interceptors = {}
    seen = set()
    for cls in type(instance).__mro__:
        for name, obj in cls.__dict__.items():
            if name in seen:
                continue
            seen.add(name)
            # 支持 staticmethod / classmethod 描述符
            if isinstance(obj, (staticmethod, classmethod)):
                obj = obj.__func__
            if hasattr(obj, "__intercept_target__"):
                interceptors.setdefault(obj.__intercept_target__, name)
    return interceptors
