# -*- coding: utf-8 -*-
# @Time    : 2023-11-14 20:44
# @Author  : Kem
# @Desc    : Header Model
from collections import UserDict
from typing import Mapping


class Header(UserDict):

    def __init__(self, data=None, **kwargs):
        self.caches = {}
        if data is None:
            data = {}
        super().__init__({**data, **kwargs})

    def __setitem__(self, key, value):
        if key.lower() in self.caches:
            return super().__setitem__(self.caches[key.lower()], value)
        else:
            self.caches[key.lower()] = key
            return super().__setitem__(key, value)

    def __getitem__(self, key):

        if key.lower() in self.caches:
            return super().__getitem__(self.caches[key.lower()])
        else:
            return super().__getitem__(key)

    def __delitem__(self, key):
        if key.lower() in self.caches:
            return super().__delitem__(self.caches[key.lower()])
        else:
            return super().__delitem__(key)

    def __eq__(self, other):
        if isinstance(other, (Mapping, dict)):
            other = self.__class__(other)
        else:
            return NotImplemented
        # Compare insensitively
        return self.items() == other.items()

    def __contains__(self, __o: object) -> bool:
        if isinstance(__o, str):
            key = __o.lower()
        else:
            key = __o

        return key in self.caches
