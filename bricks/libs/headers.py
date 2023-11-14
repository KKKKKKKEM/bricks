# -*- coding: utf-8 -*-
# @Time    : 2023-11-14 20:44
# @Author  : Kem
# @Desc    : Header Model
from typing import Callable, Mapping


class Header(dict):

    def __init__(self, data=None, _dtype: Callable = None, _ktype: Callable = None, **kwargs):
        self.dtype = _dtype or str
        self.ktype = _ktype or str
        self.comps = [
            lambda x: str(x).lower(),
            lambda x: str(x).upper(),
            lambda x: str(x).title(),
        ]
        if data is None:
            data = {}
        super().__init__({**data, **kwargs})

    def __setitem__(self, key, value):
        key: str = self.ktype(key)
        keys = self.keys()

        for comp in self.comps:
            nkey = comp(key)
            if nkey in keys:
                return super().__setitem__(nkey, self.dtype(value))

        else:
            return super().__setitem__(key, self.dtype(value))

    def __getitem__(self, key):

        key: str = self.ktype(key)
        keys = self.keys()

        for comp in self.comps:
            nkey = comp(key)
            if nkey in keys:
                return super().__getitem__(nkey)
        else:
            return super().__getitem__(key)

    def __delitem__(self, key):
        key: str = self.ktype(key)
        keys = self.keys()

        for comp in self.comps:
            nkey = comp(key)
            if nkey in keys:
                return super().__delitem__(nkey)
        else:
            return super().__delitem__(key)

    def __eq__(self, other):
        if isinstance(other, (Mapping, dict)):
            other = self.__class__(other, dtype=self.dtype, ktype=self.ktype)
        else:
            return NotImplemented
        # Compare insensitively
        return self.items() == other.items()

    def __contains__(self, __o: object) -> bool:
        key: str = self.ktype(__o)
        keys = self.keys()

        for comp in self.comps:
            nkey = comp(key)
            if nkey in keys:
                return True
        else:
            return False

    def setdefault(self, __key, __default):  # noqa
        key: str = self.ktype(__key)
        keys = self.keys()

        for comp in self.comps:
            nkey = comp(key)
            if nkey in keys:
                return super().setdefault(nkey, self.dtype(__default))
        else:
            return super().setdefault(key, self.dtype(__default))
