# -*- coding: utf-8 -*-
# @Time    : 2023-11-15 12:49
# @Author  : Kem
# @Desc    :
import functools
import time
from typing import Union

from loguru import logger

from bricks import const
from bricks.core import signals, dispatch
from bricks.core.events import EventManger, Task
from bricks.lib.context import Flow, Context
from bricks.utils import pandora


class MetaClass(type):

    def __call__(cls, *args, **kwargs):
        instance = type.__call__(cls, *args, **kwargs)

        # 加载拦截器
        interceptors = filter(lambda x: x.startswith('_when_'), dir(instance))
        for interceptor in interceptors:
            # 修改被拦截的方法
            raw_method_name = interceptor.replace("_when_", "")
            raw_method = getattr(instance, raw_method_name, None)
            if not raw_method:
                continue

            method_wrapper = getattr(instance, interceptor)
            raw_method and setattr(instance, raw_method_name, method_wrapper(raw_method))

        return instance


class Chaos(metaclass=MetaClass):

    def obtain(self, name, default=None):
        """
        获取属性
        :param name:
        :param default:
        :return:
        """
        return getattr(self, name, default)

    def install(self, name, value, nx=False):
        if nx:
            if hasattr(self, name):
                return getattr(self, name)
            else:
                setattr(self, name, value)
                return value
        else:
            setattr(self, name, value)
            return value

    def run(self, task_name: str = "all", args=None, kwargs=None):
        """
        Run a task

        :param kwargs:
        :param args:
        :param task_name: task name
        :return:
        """
        args = args or []
        kwargs = kwargs or {}

        if not task_name:
            return

        self.install("task_name", task_name)
        method = getattr(self, f'run_{task_name}', None)
        if method:
            return method(*args, **kwargs)
        else:
            logger.warning(f"Task {task_name} not found")
            return None

    def _when_run(self, raw_method):
        @functools.wraps(raw_method)
        def wrapper(*args, **kwargs):
            try:
                self.before_start()

            except (signals.Failure, signals.Success):
                logger.debug(f'[{const.BEFORE_START}] 任务被中断')
                return

            except signals.Signal as e:
                logger.warning(f"[{const.BEFORE_START}] 无法处理的信号类型: {e}")

            ret = raw_method(*args, **kwargs)

            try:
                self.before_close()
            except signals.Signal as e:
                logger.warning(f"[{const.BEFORE_START}] 无法处理的信号类型: {e}")

            return ret

        return wrapper

    def before_start(self):
        """
        Called before the task start
        """
        pass

    def _when_before_start(self, raw_method):

        @functools.wraps(raw_method)
        def wrapper(*args, **kwargs):
            context = Context(
                form=const.BEFORE_START,
                target=self,
                args=args,
                kwargs=kwargs
            )

            EventManger.invoke(context)

            ret = raw_method(*args, **kwargs)
            return ret

        return wrapper

    def before_close(self):
        """
        Called before the task close
        """
        pass

    def _when_before_close(self, raw_method):

        @functools.wraps(raw_method)
        def wrapper(*args, **kwargs):
            context = Context(
                form=const.BEFORE_CLOSE,
                target=self,
                args=args,
                kwargs=kwargs
            )
            EventManger.invoke(context)
            ret = raw_method(*args, **kwargs)
            return ret

        return wrapper


class Pangu(Chaos):

    def __init__(self, **kwargs) -> None:
        for k, v in kwargs.items():
            self.install(k, v, nx=True)

        self.dispatcher = dispatch.Dispatcher(max_workers=self.obtain("concurrency", 1))

    def on_consume(self, context: Flow):  # noqa
        context.next.root == self.on_consume and context.flow()

        while True:
            stuff: Flow = context.produce()
            if stuff is None: return

            while stuff.next and callable(stuff.next):
                try:
                    prepared = pandora.prepare(
                        func=stuff.next.root,
                        annotations={type(stuff): stuff},
                        namespace={"context": stuff}
                    )

                    product = prepared.func(*prepared.args, **prepared.kwargs)
                    callable(stuff.callback) and pandora.invoke(
                        stuff.callback,
                        args=[product],
                        annotations={type(stuff): stuff},
                        namespace={"context": stuff}
                    )

                # 中断信号
                except signals.Break:
                    stuff.next = None

                # 退出信号
                except signals.Exit:
                    return

                # 等待信号
                except signals.Wait as sig:
                    time.sleep(sig.duration)
                    return

                except signals.Switch:
                    pass

                except signals.Retry:
                    stuff.retry()

                except signals.Success:
                    stuff.success(shutdown=True)

                except signals.Failure:
                    stuff.failure(shutdown=True)

                except signals.Signal as e:
                    logger.warning(f"[{context.form}] 无法处理的信号类型: {e}")
                    raise e

                except (KeyboardInterrupt, SystemExit):
                    raise

                except Exception as e:
                    logger.error(f"""\n{pandora.get_simple_stack(e)} [{context.form}] {e.__class__.__name__}({e})""")
                    stuff.failure(shutdown=True)

    def submit(self, task: dispatch.Task, timeout=None) -> dispatch.Task:
        return self.dispatcher.submit_task(task=task, timeout=timeout)

    def active(self, task: dispatch.Task, timeout=-1) -> dispatch.Task:
        return self.dispatcher.active_task(task=task, timeout=timeout)

    def use(self, form: str, *events: Union[Task, dict]):
        context = Context(form=form, target=self)
        EventManger.register(context, *events)


if __name__ == '__main__':
    clazz = Chaos()
    clazz.run('before_start')
