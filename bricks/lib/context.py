# -*- coding: utf-8 -*-
# @Time    : 2023-11-15 12:19
# @Author  : Kem
# @Desc    :
from typing import Optional

from bricks.lib.request import Request
from bricks.lib.response import Response


class Context:

    def __init__(self, form: str, target=None, **kwargs) -> None:
        self.form = form
        self.target = target
        for k, v in kwargs.items():
            setattr(self, k, v)


class ErrorContext(Context):
    def __init__(self, form: str, error: Exception, target=None) -> None:
        super().__init__(form, target)
        self.error = error


class SpiderContext(Context):

    def __init__(
            self,
            form: str,
            target,
            request: Optional[Request] = None,
            response: Optional[Response] = None,
            seeds: Optional[dict] = None,
            proxy: Optional[str] = None,
    ) -> None:
        super().__init__(form, target)
        self.request = request
        self.response = response
        self.seeds = seeds
        self.proxy = proxy
