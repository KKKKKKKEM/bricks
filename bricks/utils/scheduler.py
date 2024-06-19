# -*- coding: utf-8 -*-
# @Time    : 2022/6/7 15:36
# @Author  : Kem
# @File    : scheduler.py
# @Software: PyCharm
# @Desc    : 调度器

# trigger 内的参数说明
## type: 表示触发器的类型, 目前支持 cron / interval / date
# exprs: 触发器调度规则
# begin: 什么时候才开始调度, 日期字符串, 格式为 %Y-%m-%d %H:%M:%S
# until: 调度到什么时候为止, 日期字符串, 格式为 %Y-%m-%d %H:%M:%S
# raise_on_error: 程序发生异常时是否抛出异常, 默认不抛出
# name: 调度任务的名称
# mode: 调度模式, 整型,
# 模式 0: 忽略任务执行时间, 依据任务运行完成的时间 与 self._last_fire_time 来计算下一次调度时间 (每小时 1 次, 5 点开始爬, 任务跑了1 小时1 分钟, 6 点 01 的时候继续跑)
# 模式 1: 不忽略任务执行时间, 依据任务运行完成的时间来计算下一次调度时间 (每小时 1 次, 5 点开始爬, 任务跑了1 小时1 分钟, 7 点 的时候继续跑)
# 模式 2: 不忽略任务执行时间, 依据上一次调度的时间来计算下一次调度时间 (每小时 1 次, 5 点开始爬, 任务跑了1 小时1 分钟, 7 点 的时候继续跑)

# 使用 cron 表达式 定时调度, seconds minutes hours days months weekday
# 支持 大部分表达式
# L 在 days 位置代表月底, 可以用 L - 1 表示月底前一天
# 1-3 代表 1 到 3
# 1,2,3,4 代表 1,2,3,4
# 可以使用 1-3,8-9 混用, 代表 1 到 3 和 8 到 9
# */N 代表能整除 N 的结果, 如 seconds 为 */1 表示每秒, */2 表示每 2 秒 (0,2,4,6...), 10-50/2 代表 开始为 10,结束为 50, 每 2 秒(10,12,14,...,50)
# MySpider(trigger={"type": "cron", "exprs": "0 8 8 1,8,15,22,L-3"})
#
# # 使用间隔定时调度, 支持单位: seconds,minutes,hours,days,months,years,weeks, quarters
# # seconds=5 表示每 5 秒一次
# # 可以多单位同时偏移, seconds=5&hours=2 表示每 2 小时 5 秒 一次
# MySpider(trigger={"type": "interval", "exprs": "seconds=5"})
#
# # 在指定时间点执行
# # 2022-06-20 18:00:00 表示在 2022-06-20 18:00:00 才执行
# MySpider(trigger={"type": "date", "exprs": "2022-06-20 18:00:00"})

import enum
import itertools
import re
import threading
import time
import urllib.parse
from typing import List, Union, Callable, Optional, Literal

from loguru import logger

from bricks.utils import pandora, arrow


class TSTATE(enum.Enum):
    CANCEL = 0


class BaseTrigger:

    def __init__(
            self,
            exprs: str,
            ref: Union[str, Callable] = None,
            args: Optional[list] = None,
            kwargs: Optional[dict] = None,
            begin: Optional[Union[str, int, arrow.Arrow]] = None,
            until: Optional[Union[str, int, arrow.Arrow]] = None,
            raise_on_error: bool = False,
            name: Optional[str] = None,
            mode: int = 0,
    ):
        self.exprs = exprs.strip()
        self.ref = pandora.load_objects(ref)
        self.name = name
        self.args = args or []
        self.kwargs = kwargs or {}
        self.begin: Optional[arrow.Arrow] = begin and arrow.Arrow.get(begin)
        self.until: Optional[arrow.Arrow] = until and arrow.Arrow.get(begin)
        self._last_fire_time: Optional[arrow.Arrow] = None
        self._next_fire_time: Optional[arrow.Arrow] = None
        self._result = None
        self.mode = mode
        self.raise_on_error = raise_on_error

    def get_next_fire_time(self, now: arrow.Arrow):
        raise NotImplementedError

    @classmethod
    def get_fire_times(cls, exprs: str, times: int = 10, mode=2, fmt=None):
        j = cls(exprs, mode=mode).do(lambda: None)
        ret = []
        fmt = fmt or (lambda x: x)
        for i in range(times):
            ret.append(fmt(j.next_fire_time))
            j.run()
        else:
            return ret

    def run(self):
        self._last_fire_time = self.next_fire_time
        if self.begin and self.begin > arrow.Arrow.now():
            return

        def main():

            try:
                if isinstance(self.ref, str): self.ref = pandora.load_objects(self.ref)
                self._result = self.ref(*self.args, **self.kwargs)
            except Exception as e:
                logger.exception(e)
                if self.raise_on_error: raise e
                self._result = e

        if self.mode == 3:
            threading.Thread(target=main, daemon=True).start()
        else:
            main()

        # 模式 0: 忽略任务执行时间, 依据任务运行完成的时间 与 self._last_fire_time 来计算下一次调度时间
        if self.mode == 0:
            now = min([arrow.Arrow.now(), self._last_fire_time])

        # 模式 1: 不忽略任务执行时间, 依据任务运行完成的时间来计算下一次调度时间
        elif self.mode == 1:
            now = arrow.Arrow.now()

        # 模式 2+3: 不忽略任务执行时间, 依据上一次调度的时间来计算下一次调度时间
        else:
            now = self._last_fire_time

        self._next_fire_time = self.get_next_fire_time(now)
        if self.until and self.until > arrow.Arrow.now():
            return TSTATE.CANCEL
        else:
            return self._result

    def do(
            self,
            ref: Union[str, Callable],
            *args,
            **kwargs
    ):
        self.next_fire_time  # noqa
        self.ref = pandora.load_objects(ref)
        self.args = args or []
        self.kwargs = kwargs or {}
        return self

    @property
    def should_run(self):
        """
        判断是否应该运行

        :return:
        """
        return max([arrow.Arrow.now(), self.begin]) if self.begin else arrow.Arrow.now() >= self.next_fire_time

    @property
    def next_fire_time(self):
        if not self._next_fire_time:
            self._next_fire_time = self.get_next_fire_time(
                max([arrow.Arrow.now(), self.begin]) if self.begin else arrow.Arrow.now()
            )

        return self._next_fire_time

    def __lt__(self, other) -> bool:
        return self.next_fire_time < other.next_fire_time

    def __gt__(self, other) -> bool:
        return self.next_fire_time > other.next_fire_time

    def __str__(self):

        name = self.name or self.ref.__doc__ or self.ref.__name__
        next_fire_time = self.next_fire_time
        last_fire_time = self._last_fire_time
        return f'[{name}] (last run: {last_fire_time or "[never]"}, next run: {next_fire_time})'


class CronTrigger(BaseTrigger):
    """
    *  -- seconds
    *  -- minutes
    *  -- hours
    *  -- days
    *  -- months
    *  -- weekday

    """

    def get_next_fire_time(self, now: arrow.Arrow):
        now = now.replace(microsecond=0)
        _keys = [
            "seconds",
            "minutes",
            "hours",
            "days",
            "months",
            "weekdays",
        ]

        _units = ["years", *_keys[::-1][1:]]

        _ranges = [
            range(60),
            range(60),
            range(24),
            range(32),
            range(1, 13),
            range(1, 8),
        ]

        def _r2range(x: str, _range):
            ret = []
            for rule in re.findall(
                    "[*]/\d+|"  # */N
                    "\d+#\d+|"  # N#A

                    "L[+-]\d+-L[+-]\d+/\d+|"  # L-N - L-N / N
                    "L[+-]\d+-L[+-]\d+|"  # L-N - L-N

                    "L[+-]\d+-L/\d+|"  # L-N - L / N
                    "L[+-]\d+-L|"  # L-N - L

                    "\d+-L[+-]\d+/\d+|"  # N - L-N / N
                    "\d+-L[+-]\d+|"  # N - L-N

                    "L[+-]\d+/\d+|"  # L-N / N
                    "L[+-]\d+|"  # L-N

                    "L|"  # L

                    "\d+-\d+/\d+|"  # N-N/N
                    "\d+/\d+|"  # N/N
                    "\d+-\d+|"  # N-N
                    "\d+",  # N
                    x
            ):
                if "L" in rule or "#" in rule:
                    ret.append(rule)

                # */N
                elif re.match('[*]/\d+', rule):
                    ret.extend(eval(re.sub('[*]/(\d+)', f'range({_range[0]}, {_range[-1] + 1}, \\1)', rule)))
                # N-N/N
                elif re.match('\d+-\d+/\d+', rule):
                    ret.extend(eval(re.sub('(\d+)-(\d+)/(\d+)', f'range(\\1, \\2+1, \\3)', rule)))
                # N/N
                elif re.match('\d+/\d+', rule):
                    ret.extend(eval(re.sub('(\d+)/(\d+)', f'range(\\1, {_range[-1] + 1}, \\2)', rule)))

                # N-N
                elif re.match('\d+-\d+', rule):
                    ret.extend(eval(re.sub('(\d+)-(\d+)', f'range(\\1, \\2+1)', rule)))

                # N
                else:
                    ret.append(int(rule))

            return list(sorted(set(ret), key=lambda y: str(y)))

        def parsing_rules():
            _rules = {}

            for k, v in dict(zip(_keys, self.exprs.split(" "))).items():
                index = _keys.index(k)

                if v in ["*", "?"]:
                    continue

                else:
                    _range = _r2range(v, _ranges[index])

                _rules[k] = _range

            _weekdays = _rules.pop("weekdays", range(1, 8))

            return _rules, _weekdays

        def _fmt_days(x: str, length: int):
            x = str(x)
            # L-N - L-N / N
            if re.match("L[+-]\d+-L[+-]\d+/\d+", x):
                return eval(re.sub('L([+-]\d+)-L([+-]\d+)/(\d+)', f"range({length}\\1, {length}\\2+1, \\3)", x))

            # L-N - L-N
            elif re.match("L[+-]\d+-L[+-]\d+", x):
                return eval(re.sub('L([+-]\d+)-L([+-]\d+)', f"range({length}\\1, {length}\\2+1)", x))

            # L-N - L / N
            elif re.match("L[+-]\d+-L/\d+", x):
                return eval(re.sub('L([+-]\d+)-L/(\d+)', f"range({length}\\1, {length}+1, \\2)", x))
            # L-N - L
            elif re.match("L[+-]\d+-L", x):
                return eval(re.sub('L([+-]\d+)-L', f"range({length}\\1, {length}+1)", x))

            # N - L-N / N
            elif re.match("\d+-L[+-]\d+/\d+", x):
                return eval(re.sub('(\d+)-L([+-]\d+)/(\d+)', f"range(\\1, {length}\\2+1, \\3)", x))

            # N - L-N
            elif re.match("\d+-L[+-]\d+", x):
                return eval(re.sub('(\d+)-L([+-]\d+)', f"range(\\1, {length}\\2+1)", x))

            # L-N / N
            elif re.match("L[+-]\d+/\d+", x):
                return eval(re.sub('L([+-]\d+)/(\d+)', f"range({length}\\1, {length}+1, \\2)", x))

            # L-N
            elif re.match("L[+-]\d+", x):
                return [eval(re.sub('L([+-]\d+)', f"{length}\\1", x))]

            # L
            elif x == "L":

                return [length]

            else:
                return [int(x)]

        def calc_time():
            target = now

            while True:

                _real = {}

                days = target.statistics.number_of_days_for_this_month
                for k, v in rules.items():
                    if k == 'days':
                        v = [
                            d
                            for day in v for d in _fmt_days(day, days)
                        ]

                    _real[k] = list(sorted(v))

                for _product in itertools.product(*reversed(_real.values())):
                    target = target.replace(**dict(zip([name[:-1] for name in _real.keys()][::-1], _product)))
                    if (
                            target > now and
                            (
                                    target.isoweekday() in weekdays
                                    or f'{target.isoweekday()}#{target.day // 7 + 1}' in weekdays
                            )
                    ):
                        return target
                else:

                    for index, k in enumerate(_units):
                        if k in _real:
                            next_unit = _units[index - 1]
                            break
                    else:
                        next_unit = ''

                    target = target.shift(**{next_unit: 1})

        rules, weekdays = parsing_rules()
        return calc_time()


class IntervalTrigger(BaseTrigger):

    def get_next_fire_time(self, now: arrow.Arrow):
        rules = {k: float(v) for k, v in urllib.parse.parse_qsl(self.exprs)}
        return now.shift(**rules)


class DateTrigger(BaseTrigger):
    def get_next_fire_time(self, now: arrow.Arrow):
        return arrow.Arrow.get(self.exprs)

    def run(self):
        if arrow.Arrow.now() > self.next_fire_time: return TSTATE.CANCEL
        super().run()
        return TSTATE.CANCEL


class Scheduler:
    def __init__(self) -> None:
        self.jobs: List[BaseTrigger] = []
        self._is_running = threading.Event()
        self._shutdown = threading.Event()

    def cron(self, exprs: str, **options) -> CronTrigger:
        return self.bind_trigger(CronTrigger, exprs, **options)

    def interval(self, exprs: str, **options) -> IntervalTrigger:
        return self.bind_trigger(IntervalTrigger, exprs, **options)

    def date(self, exprs: str, **options) -> DateTrigger:
        return self.bind_trigger(DateTrigger, exprs, **options)

    def bind_trigger(self, trigger: type(BaseTrigger), exprs: str, **options):
        trigger = trigger(exprs=exprs, **options)
        self.jobs.append(trigger)
        return trigger

    def add(
            self,
            form: Union[Literal['cron', 'date', 'interval'], BaseTrigger],
            exprs: str,
            **options
    ) -> BaseTrigger:
        if isinstance(form, str):
            f = getattr(self, form)
            return f(exprs=exprs, **options)
        else:
            return self.bind_trigger(form, exprs, **options)

    def run(self, debug=True, callback: Optional[Callable] = None):

        if not self._is_running.is_set():
            self._is_running.set()

        else:
            return

        try:
            while self.jobs:
                run_callback = False
                try:
                    runnable_jobs = (job for job in self.jobs if job.should_run)
                    for job in sorted(runnable_jobs):

                        self.run_job(job)
                        run_callback = True

                    else:
                        debug and self.jobs and logger.debug(f'-> {min(self.jobs)}')
                        run_callback and self.jobs and callback and callback()

                    self.wait()

                except KeyboardInterrupt:
                    raise

                except Exception as e:
                    logger.exception(e)

        finally:
            self._is_running.clear()

    def start(self, debug=True, callback: Optional[Callable] = None):
        threading.Thread(target=self.run, kwargs={"debug": debug, "callback": callback}, daemon=True).start()
        threading.Thread(target=self._shutdown.wait).start()
        return self

    def shutdown(self):
        self._shutdown.set()

    def wait(self):
        """
        判断时间进行休眠
        找到下一个要运行的任务,判断还有多久要运行
        如果间隔时间大于10秒,则休眠10秒后再次判断 -> 防止中途有需要先执行的新任务添加
        否则直接休眠间隔秒

        :return:
        """
        while True:
            _time = self.idle_seconds
            _time = 0 if _time and _time < 0 else _time
            if _time and _time > 10:
                time.sleep(10)
            else:
                time.sleep(_time or 0)
                break

    @property
    def idle_seconds(self) -> Optional[float]:
        """

        :return: Number of seconds until
                 :meth:`next_run <Scheduler.next_run>`
                 or None if no jobs are scheduled
        """
        if not self.next_run:
            return None
        return (self.next_run - arrow.Arrow.now()).total_seconds()

    @property
    def next_run(self) -> Optional[arrow.Arrow]:
        """
        arrow.Arrow when the next job should run.

        :return: A :class:`~arrow.Arrow` object
                 or None if no jobs scheduled
        """
        if not self.jobs:
            return None
        return min(self.jobs).next_fire_time

    def run_job(self, job: BaseTrigger):
        res = job.run()
        if res == TSTATE.CANCEL: self.jobs.remove(job)

    def submit(
            self,
            func: Union[str, Callable],
            args: list = None,
            kwargs: dict = None,
            jobs: Union[BaseTrigger, List[BaseTrigger], dict, List[dict]] = None,
            start_now: bool = False,
    ):
        args = args or []
        kwargs = kwargs or {}

        for job in pandora.iterable(jobs):
            if not isinstance(job, BaseTrigger):
                job = self.add(**job)
            else:
                self.jobs.append(job)

            job.do(func, *args, **kwargs)
            job not in self.jobs and self.jobs.append(job)
        else:
            if start_now: func(*args, **kwargs)


if __name__ == '__main__':
    s = Scheduler()
    s.add("interval", 'seconds=1').do(lambda: (time.sleep(5), print('xxx')))
    s.run()
