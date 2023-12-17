# -*- coding: utf-8 -*-
# @Time    : 2023-11-15 12:49
# @Author  : Kem
# @Desc    :
import functools
import time
from typing import Union, Literal, Callable, List

from loguru import logger

from bricks.core import signals, dispatch
from bricks.core.context import Flow, Context
from bricks.core.events import EventManager, Task, Register
from bricks.state import const
from bricks.utils import pandora
from bricks.utils.scheduler import BaseTrigger, Scheduler


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
        else:
            hasattr(instance, "install") and instance.install()

        return instance


class Chaos(metaclass=MetaClass):
    Context = Context

    def get(self, name, default=None):
        """
        获取属性

        :param name:
        :param default:
        :return:
        """
        return getattr(self, name, default)

    def set(self, name, value, nx=False):
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

        self.set("task_name", task_name)
        method = getattr(self, f'run_{task_name}', None)
        if method:
            return method(*args, **kwargs)
        else:
            logger.warning(f"Task {task_name} not found")
            return None

    def launch(self, scheduler: dict, task_name: str = "all", args=None, kwargs=None, callback: Callable = None):
        """
        同 launch, 但是是提交给调度器运行的, 可以定时执行

        :param scheduler:
        :param task_name:
        :param args:
        :param kwargs:
        :param callback:
        :return:
        """

        def job():
            self.run(task_name=task_name, args=args, kwargs=kwargs)
            callback and pandora.invoke(callback, namespace={"spider": self}, annotations={type(self): self})

        form: Union[Literal['cron', 'date', 'interval'], BaseTrigger] = scheduler.pop("form")
        exprs: str = scheduler.pop("exprs")
        scheduler_ = Scheduler()
        scheduler_.add(form=form, exprs=exprs, **scheduler).do(job)
        scheduler_.run()

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
            context = self.make_context(form=const.BEFORE_START)

            EventManager.invoke(context)

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
            context = self.make_context(form=const.BEFORE_CLOSE)
            EventManager.invoke(context)
            ret = raw_method(*args, **kwargs)
            return ret

        return wrapper

    def make_context(self, **kwargs):
        raise NotImplementedError


class Pangu(Chaos):
    Context = Flow

    def __init__(self, **kwargs) -> None:
        for k, v in kwargs.items():
            self.set(k, v, nx=True)

        self.dispatcher = dispatch.Dispatcher(max_workers=self.get("concurrency", 1))
        self.plugins: List[Register] = []

    def on_consume(self, context: Flow):
        context.next.root == self.on_consume and context.flow()

        while True:
            context: Flow = context.produce()
            if context is None: return
            with context:
                while context.next and callable(context.next.root):
                    try:
                        prepared = pandora.prepare(
                            func=context.next.root,
                            annotations={type(context): context},
                            namespace={"context": context}
                        )

                        product = prepared.func(*prepared.args, **prepared.kwargs)
                        callable(context.callback) and pandora.invoke(
                            context.callback,
                            args=[product],
                            annotations={type(context): context},
                            namespace={"context": context}
                        )

                    # 中断信号
                    except signals.Break:
                        context.flow({"next": None})
                        pass
                    # 退出信号
                    except signals.Exit:
                        return

                    # 等待信号
                    except signals.Wait as sig:
                        time.sleep(sig.duration)
                        return

                    except signals.Switch:
                        continue

                    except signals.Retry:
                        context.retry()

                    except signals.Success:
                        context.success(shutdown=True)

                    except signals.Failure:
                        context.failure(shutdown=True)

                    except signals.Signal as e:
                        logger.warning(f"[{context.form}] 无法处理的信号类型: {e}")
                        raise e

                    except (KeyboardInterrupt, SystemExit):
                        raise

                    except Exception as e:
                        logger.error(
                            f"\n{pandora.get_simple_stack(e)} [{context.form}] {e.__class__.__name__}({e})"
                        )
                        context.error(shutdown=True)

    def submit(self, task: dispatch.Task, timeout=None) -> dispatch.Task:
        return self.dispatcher.submit_task(task=task, timeout=timeout)

    def active(self, task: dispatch.Task, timeout=-1) -> dispatch.Task:
        return self.dispatcher.active_task(task=task, timeout=timeout)

    def use(self, form: str, *events: Union[Task, dict]):
        context = self.make_context(form=form)
        register = EventManager.register(context, *events)
        self.plugins.extend(register)
        return register

    def make_context(self, **kwargs):
        kwargs.setdefault("flows", self.flows)
        kwargs.setdefault("target", self)
        _Context = kwargs.pop("_Context", self.Context)
        context = _Context(**kwargs)
        return context

    @property
    def flows(self):
        raise NotImplementedError

    def install(self):
        """
        实例化之后会运行一次

        :return:
        """
        pass
