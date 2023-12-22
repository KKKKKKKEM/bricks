# -*- coding: utf-8 -*-
# @Time    : 2023-12-10 00:19
# @Author  : Kem
# @Desc    : 时间工具类
import calendar
import datetime
import math
from typing import Callable, Tuple, Union


class _Range:
    def __init__(
            self,
            years: Tuple['Arrow', 'Arrow'],
            quarters: Tuple['Arrow', 'Arrow'],
            months: Tuple['Arrow', 'Arrow'],
            days: Tuple['Arrow', 'Arrow'],
            hours: Tuple['Arrow', 'Arrow'],
            minutes: Tuple['Arrow', 'Arrow'],
            seconds: Tuple['Arrow', 'Arrow'],
    ):
        self.seconds: Tuple[Arrow, Arrow] = seconds
        self.minutes: Tuple[Arrow, Arrow] = minutes
        self.hours: Tuple[Arrow, Arrow] = hours
        self.days: Tuple[Arrow, Arrow] = days
        self.years: Tuple[Arrow, Arrow] = years
        self.quarters: Tuple[Arrow, Arrow] = quarters
        self.months: Tuple[Arrow, Arrow] = months


class _Offset:

    def __init__(
            self,
            years: Callable,
            quarters: Callable,
            months: Callable,
            days: Callable,
            hours: Callable,
            minutes: Callable,
            seconds: Callable,
    ):
        self.seconds: Arrow = seconds  # noqa
        self.minutes: Arrow = minutes  # noqa
        self.hours: Arrow = hours  # noqa
        self.days: Arrow = days  # noqa
        self.years: Arrow = years  # noqa
        self.quarters: Arrow = quarters  # noqa
        self.months: Arrow = months  # noqa

    def __getattribute__(self, item):
        if item in [
            "seconds",
            "minutes",
            "hours",
            "days",
            "years",
            "quarters",
            "months",
        ]:
            method = object.__getattribute__(self, item)
            return method()
        else:
            return object.__getattribute__(self, item)


class _Statistics:
    def __init__(
            self,
            number_of_days_for_this_year: int,
            number_of_days_for_this_month: int,
            number_of_days_for_this_quarter: int,
            days_for_this_year: int,
            days_for_this_month: int,
            is_leap_year: bool,
    ):
        self.number_of_days_for_this_year = number_of_days_for_this_year
        self.number_of_days_for_this_month = number_of_days_for_this_month
        self.number_of_days_for_this_quarter = number_of_days_for_this_quarter
        self.days_for_this_year = days_for_this_year
        self.days_for_this_month = days_for_this_month
        self.is_leap_year = is_leap_year


class Arrow(datetime.datetime):

    def __new__(cls, year=None, month=None, day=None, hour=0, minute=0, second=0,
                microsecond=0, tzinfo=None, *, fold=0, date=None) -> 'Arrow':

        if not date:
            now = datetime.datetime.now()
            year = year or now.year
            day = day or now.day
            month = month or now.month
            return datetime.datetime.__new__(
                cls,
                year=year,
                month=month,
                day=day,
                hour=hour,
                minute=minute,
                second=second,
                microsecond=microsecond,
                tzinfo=tzinfo,
                fold=fold,
            )

        elif isinstance(date, Arrow):
            return date

        elif isinstance(date, datetime.datetime):
            return datetime.datetime.__new__(
                cls,
                year=date.year,
                month=date.month,
                day=date.day,
                hour=date.hour,
                minute=date.minute,
                second=date.second,
                microsecond=date.microsecond,
                tzinfo=date.tzinfo,
                fold=date.fold,
            )

        elif isinstance(date, datetime.date):
            return datetime.datetime.__new__(
                cls,
                year=date.year,
                month=date.month,
                day=date.day,
                hour=hour,
                minute=minute,
                second=second,
                microsecond=microsecond,
                tzinfo=tzinfo,
                fold=fold,
            )

    def shift(
            self,
            years: int = ...,
            quarters: int = ...,
            months: int = ...,
            weeks: float = ...,
            days: Union[float, str] = ...,
            hours: float = ...,
            minutes: float = ...,
            seconds: float = ...,
            microseconds: float = ...,
            milliseconds: float = ...,
            *,
            fold: int = ...,
    ) -> 'Arrow':
        """

        :param years:
        :param quarters:
        :param months:
        :param weeks:
        :param days: > 代表月底
        :param hours:
        :param minutes:
        :param seconds:
        :param microseconds:
        :param milliseconds:
        :param fold:
        :return:
        """
        if days == ">": days = calendar.monthrange(self.year, self.month)[-1] - self.day

        raw_parameter = {
            "years": years,
            "quarters": quarters,
            "months": months,
            "weeks": weeks,
            "days": days,
            "hours": hours,
            "minutes": minutes,
            "seconds": seconds,
            "microseconds": microseconds,
            "milliseconds": milliseconds,
            "fold": fold,
        }
        raw_parameter = {k: v for k, v in raw_parameter.items() if v is not ...}

        def cacl_days(count):
            y = self.year + math.ceil((self.month + count) / 12) - 1

            m = (self.month + count) % 12
            if m == 0: m = 12
            return (self.replace(year=y, month=m, day=min([self.day, calendar.monthrange(y, m)[-1]])) - self).days

        raw_parameter.setdefault('days', 0)

        if 'months' in raw_parameter: raw_parameter['days'] += cacl_days(raw_parameter.pop('months'))
        if 'years' in raw_parameter: raw_parameter['days'] += cacl_days(raw_parameter.pop('years') * 12)
        if 'quarters' in raw_parameter: raw_parameter['days'] += cacl_days(raw_parameter.pop('quarters') * 3)

        return self.__class__(date=self + datetime.timedelta(**raw_parameter))

    def to(self, action: str, unit="months") -> 'Arrow':
        """
        移动至

        :param action:
        :param unit:
        :return:
        """
        return getattr(getattr(self, action), unit)

    def format(self, fmt: str) -> str:
        return f'{self:{fmt}}'

    @classmethod
    def get(
            cls,
            __date: Union[str, datetime.datetime, datetime.date, int, float],
            fmt: str = "%Y-%m-%d %H:%M:%S"
    ) -> 'Arrow':
        if isinstance(__date, str):
            return cls.strptime(__date, fmt)

        elif isinstance(__date, (int, float)):
            if isinstance(__date, int) and len(str(__date)) > 10:
                __date = float(f'{str(__date)[:10]}.{str(__date)[10:]}')

            return cls.fromtimestamp(__date)

        else:
            return cls(date=__date)

    @property
    def next(self) -> _Offset:
        return _Offset(
            years=lambda: self.shift(years=1),
            quarters=lambda: self.shift(quarters=1),
            months=lambda: self.shift(months=1),
            days=lambda: self.shift(days=1),
            hours=lambda: self.shift(hours=1),
            minutes=lambda: self.shift(minutes=1),
            seconds=lambda: self.shift(seconds=1),
        )

    @property
    def prev(self) -> _Offset:
        return _Offset(
            years=lambda: self.shift(years=-1),
            quarters=lambda: self.shift(quarters=-1),
            months=lambda: self.shift(months=-1),
            days=lambda: self.shift(days=-1),
            hours=lambda: self.shift(hours=-1),
            minutes=lambda: self.shift(minutes=-1),
            seconds=lambda: self.shift(seconds=-1),
        )

    @property
    def end(self) -> _Offset:
        return _Offset(
            years=lambda: self.replace(
                month=12,
                day=31,
                hour=23,
                minute=59,
                second=59,
                microsecond=10 ** 6 - 1
            ),
            quarters=lambda: self.replace(
                month=(abs(self.month - 1) // 3 + 1 - 1) * 3 + 3,
                day=calendar.monthrange(self.year, (abs(self.month - 1) // 3 + 1 - 1) * 3 + 3)[-1],
                hour=23,
                minute=59,
                second=59,
                microsecond=10 ** 6 - 1
            ),
            months=lambda: self.replace(
                day=calendar.monthrange(self.year, self.month)[-1],
                hour=23,
                minute=59,
                second=59,
                microsecond=10 ** 6 - 1

            ),
            days=lambda: self.replace(
                hour=23,
                minute=59,
                second=59,
                microsecond=10 ** 6 - 1
            ),
            hours=lambda: self.replace(
                minute=59,
                second=59,
                microsecond=10 ** 6 - 1
            ),
            minutes=lambda: self.replace(
                second=59,
                microsecond=10 ** 6 - 1
            ),
            seconds=lambda: self.replace(
                microsecond=10 ** 6 - 1
            ),
        )

    @property
    def start(self) -> _Offset:
        return _Offset(
            years=lambda: self.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0),
            quarters=lambda: self.replace(day=1, month=(abs(self.month - 1) // 3 + 1 - 1) * 3 + 1),
            months=lambda: self.replace(day=1, hour=0, minute=0, second=0, microsecond=0),
            days=lambda: self.replace(hour=0, minute=0, second=0, microsecond=0),
            hours=lambda: self.replace(minute=0, second=0, microsecond=0),
            minutes=lambda: self.replace(second=0, microsecond=0),
            seconds=lambda: self.replace(microsecond=0),
        )

    @property
    def range(self) -> _Range:

        return _Range(
            years=(self.start.years, self.end.years),
            quarters=(self.start.quarters, self.end.quarters),
            months=(self.start.months, self.end.months),
            days=(self.start.days, self.end.days),
            hours=(self.start.hours, self.end.hours),
            minutes=(self.start.minutes, self.end.minutes),
            seconds=(self.start.seconds, self.end.seconds),
        )

    @property
    def statistics(self) -> _Statistics:

        ndm = calendar.monthrange(self.year, self.month)[-1]
        quarters_range = self.range.quarters
        ndq = sum([
            calendar.monthrange(self.year, i)[-1] for i in
            range(quarters_range[0].month, quarters_range[1].month + 1)
        ])
        is_leap_year = (self.year % 4 == 0 and self.year % 100 != 0) or (self.year % 400 == 0)
        ndy = 366 if is_leap_year else 365

        return _Statistics(
            number_of_days_for_this_month=ndm,
            number_of_days_for_this_year=ndy,
            number_of_days_for_this_quarter=ndq,
            days_for_this_year=(self - self.start.years).days + 1,
            days_for_this_month=(self - self.start.months).days + 1,
            is_leap_year=is_leap_year
        )

    def ts(self, length=10, fmt=int):
        """
        获取时间戳

        :param length: 长度为几位
        :param fmt: 返回什么类型
        :return:
        """
        ts = super().timestamp()
        return fmt(ts * 10 ** (length - 10))


if __name__ == '__main__':
    # 获取当前时间
    arrow = Arrow()

    # 获取当前时间的相关信息
    print(f'{arrow.year=}, {arrow.month=}, {arrow.day=}, {arrow.hour=}, {arrow.minute=}, {arrow.second=}')

    # 当前时间往后面偏移一年 + 一个月
    print(arrow.shift(years=1, months=1))

    # 获取当前时间当前月份有多少天
    print(f'{arrow.statistics.number_of_days_for_this_month=}')

    # 将当前时间转化为时间戳
    print(arrow.ts())

    # 将字符串转为 arrow 对象, 后面可以传格式
    print(arrow.get("2023-11-20 10:12:13"))
    # 将时间戳转为 arrow 对象, 可以是秒级 / 毫秒 啥的, 会兼容
    print(arrow.get(1703174400))

    # 获取当前时间月份的开始, 天的开始, 天的结尾, 月份的结尾
    print(arrow.start.months, arrow.start.days, arrow.end.days, arrow.end.months)
