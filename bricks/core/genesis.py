# -*- coding: utf-8 -*-
# @Time    : 2023-11-15 12:49
# @Author  : Kem
# @Desc    :
import functools

from loguru import logger

from bricks.core import signals, events
from bricks.lib import context


class MetaClass(type):

    def __call__(cls, *args, **kwargs):
        instance = type.__call__(cls, *args, **kwargs)

        # 加载拦截器
        interceptors = filter(lambda x: x.startswith('_when_'), dir(instance))
        for interceptor in interceptors:
            # 修改被拦截的方法
            raw_method_name = interceptor.replace("_when_", "")
            raw_method = getattr(instance, raw_method_name)
            method_wrapper = getattr(instance, interceptor)
            raw_method and setattr(instance, raw_method_name, method_wrapper(raw_method))

        return instance


class Chaos(metaclass=MetaClass):

    def get_attr(self, name, default=None):
        """
        获取属性
        :param name:
        :param default:
        :return:
        """
        return getattr(self, name, default)

    def set_attr(self, name, value):
        """
        设置属性
        :param name:
        :param value:
        :return:
        """
        return setattr(self, name, value)

    def run_task(self, task_name: str, *args, **kwargs):
        """
        Run a task

        :param task_name: task name
        :return:
        """
        method = getattr(self, f'run_{task_name}', None)
        if method:
            return method(*args, **kwargs)
        else:
            logger.warning(f"Task {task_name} not found")
            return None

    def _when_run_task(self, raw_method):
        @functools.wraps(raw_method)
        def wrapper(*args, **kwargs):
            try:
                self.before_start()
            except signals.Signal as sig:
                if sig.form == signals.Break:
                    logger.debug(
                        f'[收到信号] 信号类型: 中断信号; 任务: {self.__class__.__name__}; 信号来源: 启动之前; 信号影响: 中断后续流程')
                    return

            ret = raw_method(*args, **kwargs)

            try:
                self.before_close()
            except signals.Signal:
                pass
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
            events.Event.invoke(context.Context(
                form=events.EventEnum.BeforeStart,
                target=self,
                args=args,
                kwargs=kwargs
            ))
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
            events.Event.invoke(context.Context(
                form=events.EventEnum.BeforeClose,
                target=self,
                args=args,
                kwargs=kwargs
            ))
            ret = raw_method(*args, **kwargs)
            return ret

        return wrapper


if __name__ == '__main__':
    clazz = Chaos()
    clazz.run_task('before_start')
