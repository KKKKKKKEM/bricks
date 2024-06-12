# -*- coding: utf-8 -*-
# @Time    : 2023-11-15 14:04
# @Author  : Kem
# @Desc    :
from typing import Literal


class Signal(BaseException):
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

    def __str__(self):
        return f"<{self.__class__.__name__} {self.__dict__}>"


# 中断信号, 中断当前 flow 流程
# 如当前 flow.next 为 函数 A, 在函数 A 内 raise Break, 则会中断函数 A 的执行, 并将 flow.next 设置为 None
# 接下来不会再处理这个 flow
class Break(Signal):
    ...


# 切换信号, 切换流程
# 如当前 flow.next 为函数 A, 在函数 A 内使用 flow.switch("next": 函数 B), 然后 raise Switch
# 那么接下来会调用 函数 B
# 如果函数 B 内没有手动 flow 流转, 那么接下来就会执行函数 A
class Switch(Signal):
    def __init__(self, by: Literal['func', 'block'] = "func"):
        self.by = by


# 退出信号, 退出当前任务
class Exit(Signal):
    ...


class Wait(Signal):
    def __init__(self, duration=1):
        self.duration = duration


class Empty(Signal):
    ...


class Retry(Signal):
    ...


class Success(Signal):
    ...


class Failure(Signal):
    ...


class Pass(Signal):
    ...
