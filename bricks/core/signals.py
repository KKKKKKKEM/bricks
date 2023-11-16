# -*- coding: utf-8 -*-
# @Time    : 2023-11-15 14:04
# @Author  : Kem
# @Desc    :
class Signal(Exception):
    def __init__(self, **kwargs):
        self.form = self.__class__
        for k, v in kwargs.items():
            setattr(self, k, v)


# 中断信号, 中断后续流程
class Break(Signal):
    ...


# 退出信号, 退出当前任务
class Exit(Signal):
    ...


class Wait(Signal):
    ...


class Empty(Signal):
    ...

class Retry(Signal):
    ...
class End(Signal):
    ...


class Success(Signal):
    ...


class Failure(Signal):
    ...
