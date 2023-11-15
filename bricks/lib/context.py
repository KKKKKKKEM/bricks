# -*- coding: utf-8 -*-
# @Time    : 2023-11-15 12:19
# @Author  : Kem
# @Desc    :
class Context:

    def __init__(self, form: str, target=None) -> None:
        self.form = form
        self.target = target


class ExceptionContext(Context):
    def __init__(self, form: str, error: Exception, target=None) -> None:
        super().__init__(form, target)
        self.error = error
