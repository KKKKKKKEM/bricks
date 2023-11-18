# -*- coding: utf-8 -*-
# @Time    : 2023-11-15 16:17
# @Author  : Kem
# @Desc    :
import copy
import datetime
import functools
import json
import os
import queue
import threading
import time
from collections import deque, defaultdict
from typing import Optional, Union

from loguru import logger

from bricks import const
from bricks.core import genesis
from bricks.db.redis_ import Redis
from bricks.utils import pandora


class Item(dict):

    def __init__(self, __dict: Optional[Union[dict, str]] = ..., **kwargs) -> None:
        if isinstance(__dict, self.__class__):
            self.fingerprint = __dict.fingerprint

        elif isinstance(__dict, str):
            self.fingerprint = __dict
            __dict = pandora.json_or_eval(__dict)

        elif isinstance(__dict, dict):
            self.fingerprint = copy.deepcopy(__dict)
            __dict = copy.deepcopy(__dict)

        else:
            self.fingerprint = None
        try:
            super().__init__(__dict, **kwargs)
        except:
            self.fingerprint = __dict

    fingerprint = property(
        fget=lambda self: getattr(self, "_fingerprint", None) or self,
        fset=lambda self, v: setattr(self, "_fingerprint", v),
        fdel=lambda self: setattr(self, "_fingerprint", None)
    )

    def rebuild_fingerprint(self):
        self.fingerprint = json.dumps(self, default=str, sort_keys=True)

    def __str__(self):
        if self:
            return repr(self)
        else:
            return repr(self.fingerprint)


class SmartQueue(queue.Queue):
    """
    基于 queue.Queue 封装的 Queue
    支持更多智能的特性
        - in 操作 (但是可能会线程不安全)
        - 双端队列投放的特性
        - 可选的不抛出异常
        - 可选的唯一去重

    """
    __slots__ = ['unique']

    def __init__(self, maxsize=0, unique=False):
        """
        实例化 一个 SmartQueue

        :param maxsize: 队列最大大小限制
        :param unique: 队列元素是否保证唯一去重
        """
        self.unique = unique
        super().__init__(maxsize=maxsize)
        self.queue: deque

    def put(self, *items, block=True, timeout=None, unique=None, limit=0, head=False) -> int:
        """
        将 `items` 投放至 队列中

        :param items: 需要投放的 items
        :param block: 是否阻塞
        :param timeout: 超时时间
        :param unique: 是否唯一
        :param limit: 动态限制队列大小
        :param head: 是否投放至头部
        :return:

        .. code:: python

            from lego.libs.models import SmartQueue
            q = SmartQueue()

            q.put(
                *[{'name':'kem'}, {'name':'kem2'}, {'name':'kem3'}, {'name':'kem4'}],
            )

        """

        before = self.unfinished_tasks
        unique = self.unique if unique is None else unique
        for item in items:

            with self.not_full:
                if unique:
                    if item in self.queue:
                        continue

                maxsize = limit if limit > 0 else self.maxsize
                if maxsize > 0:
                    if not block:
                        if self._qsize() >= maxsize:
                            raise queue.Full
                    elif timeout is None:
                        while self._qsize() >= maxsize:
                            self.not_full.wait()
                    elif timeout < 0:
                        raise ValueError("'timeout' must be a non-negative number")
                    else:
                        endtime = time.time() + timeout
                        while self._qsize() >= maxsize:
                            remaining = endtime - time.time()
                            if remaining <= 0.0:
                                raise queue.Full
                            self.not_full.wait(remaining)

                self._put(item) if head is False else self._put_head(item)
                self.unfinished_tasks += 1
                self.not_empty.notify()

        return self.unfinished_tasks - before

    def _put_head(self, item):
        """
        将 item 放置队列头部

        :param item:
        :return:
        """
        self.queue.appendleft(item)

    def get(self, block=True, timeout=None, count=1, tail=False):
        """
        从队列中获取值

        :param block: 是否阻塞
        :param timeout: 等待时长
        :param count: 一次拿多少个
        :param tail: 是否从尾巴处拿(拿出最后的)
        :return:

        .. code:: python

            from lego.libs.models import SmartQueue
            q = SmartQueue()

            # 从尾部拿出一个
            q.get(tail=True)

            # 从头部拿出一个
            q.get()


        """
        with self.not_empty:
            if not block:
                if not self._qsize():
                    return None
            elif timeout is None:
                while not self._qsize():
                    self.not_empty.wait()
            elif timeout < 0:
                raise ValueError("'timeout' must be a non-negative number")
            else:
                endtime = time.time() + timeout
                while not self._qsize():
                    remaining = endtime - time.time()
                    if remaining <= 0.0:
                        return None
                    self.not_empty.wait(remaining)
            items = []
            try:
                for _ in range(count):
                    items.append(self._get() if tail is False else self._get_tail())
            except IndexError:
                pass

            self.not_full.notify()
            return items[0] if len(items) == 1 else items

    def remove(self, *values):
        """
        从多列中删除 `values`

        :param values: 需要删除的元素
        :return:

        .. code:: python

            from lego.libs.models import SmartQueue
            q = SmartQueue()

            # 删除一个元素
            q.remove({"name":"kem"})

        """
        count = 0
        with self.mutex:
            try:
                for value in values:
                    self.queue.remove(value)
            except:
                pass
            else:
                count += 1

        return count

    def _get_tail(self):
        """
        从尾部获取一个值

        :return:
        """
        return self.queue.pop()

    def __contains__(self, item):
        """
        使之支持 in 操作

        :param item:
        :return:
        """
        return item in self.queue

    def clear(self):
        self.queue.clear()


class TaskQueue(metaclass=genesis.MetaClass):
    subscribe = False

    class COMMANDS:
        GET_PERMISSION = "GET_PERMISSION"
        GET_INIT_RECORD = "GET_INIT_RECORD"
        RESET_QUEUE = "RESET_QUEUE"
        CONTINUE_INIT_RECORD = "CONTINUE_INIT_RECORD"
        SET_INIT_RECORD = "SET_INIT_RECORD"
        BACKUP_INIT_RECORD = "BACKUP_INIT_RECORD"
        WAIT_FOR_INIT_START = "WAIT_FOR_INIT_START"
        SET_INIT_STATUS = "SET_INIT_STATUS"
        IS_INIT_STATUS = "IS_INIT_STATUS"
        RELEASE_INIT_STATUS = "RELEASE_INIT_STATUS"
        RUN_SUBSCRIBE = "RUN_SUBSCRIBE"

    reversible = property(
        fget=lambda self: getattr(self, "_reversible", True),
        fset=lambda self, value: setattr(self, "_reversible", value),
        fdel=lambda self: setattr(self, "_reversible", True),
    )

    @staticmethod
    def _to_str(*args):
        return [
            json.dumps(value, default=str, sort_keys=True, ensure_ascii=False)
            if not isinstance(value, (bytes, str, int, float)) else value
            for value in args
        ]

    def continue_(self, name: str, maxsize=None, interval=1, **kwargs):
        """
        判断 name 是否可与继续投放

        :param interval: 休眠间隔
        :param maxsize: 队列最大大小
        :param name:
        :return:
        """
        if maxsize is None:
            return
        else:
            while self.size(name, **kwargs) >= maxsize:
                logger.debug(f'队列内种子数量已经超过 {maxsize}, 暂停投放')
                time.sleep(interval)

    @staticmethod
    def name2key(name: str, _type: str) -> str:
        """
        将 name 转换为 key

        :param name: 队列名
        :param _type: 队列类型
        :return:
        """
        if not _type:
            return name

        if name.endswith(f":{_type}"):
            return name
        else:
            return f'{name}:{_type}'

    def is_empty(self, name: str, **kwargs) -> bool:
        """
        判断队列是否为空

        :param name: 队列名
        :param kwargs: 传入 size 的其他参数
        :return:
        """
        threshold = kwargs.get('threshold', 0)
        return self.size(name, **kwargs) <= threshold

    def size(self, *names: str, qtypes: tuple = ('current', 'temp', 'failure'), **kwargs) -> int:
        """
        获取队列大小

        :param qtypes:
        :param names:
        :param kwargs:
        :return:
        """
        raise NotImplementedError

    def reverse(self, name: str, **kwargs) -> bool:
        """
        强制翻转队列

        :param name:
        :param kwargs:
        :return:
        """
        raise NotImplementedError

    def _when_reverse(self, func):  # noqa
        @functools.wraps(func)
        def inner(*args, **kwargs):
            reversible = self.reversible
            if reversible:
                return func(*args, **kwargs)
            else:
                logger.debug("[翻转失败] reversible 属性为 False")
                return False

        return inner

    def smart_reverse(self, name: str, status=0) -> bool:
        """
        智能翻转队列

        :return:
        """
        raise NotImplementedError

    _when_smart_reverse = _when_reverse

    def merge(self, dest: str, *queues: str, **kwargs):
        raise NotImplementedError

    def replace(self, name: str, old, new, **kwargs):
        """
        替换

        :return:
        """
        raise NotImplementedError

    def _when_replace(self, func):  # noqa
        def inner(name, old, new, **kwargs):
            if isinstance(old, Item): old = old.fingerprint
            if isinstance(new, Item): new = new.fingerprint

            return func(name, old, new, **kwargs)

        return inner

    def clear(self, *names, qtypes=('current', 'temp', "failure", "lock", "record"), **kwargs):
        """
        清空任务队列

        :param qtypes:
        :param kwargs:
        :return:
        """
        raise NotImplementedError

    def get(self, name: str, count: int = 1, **kwargs) -> Item:
        """
        获取一个任务

        :param count:
        :param name:
        :param kwargs:
        :return:
        """
        raise NotImplementedError

    def _when_get(self, func):  # noqa
        def inner(*args, **kwargs):
            ret = func(*args, **kwargs)
            if ret in [None, []]:
                return None

            elif isinstance(ret, list) and len(ret) != 1:
                return [Item(i) for i in ret]

            else:
                if isinstance(ret, list):
                    ret = ret[0]
                return Item(ret)

        return inner

    def put(self, name: str, *values, **kwargs):
        """
        放入一个任务

        :param name:
        :param values:
        :param kwargs:
        :return:
        """
        raise NotImplementedError

    def _when_put(self, func):  # noqa
        def inner(name, *values, **kwargs):
            return func(name, *[i.fingerprint if isinstance(i, Item) else i for i in values], **kwargs)

        return inner

    def remove(self, name: str, *values, **kwargs):
        """
        删除一个任务

        :param name:
        :param values:
        :param kwargs:
        :return:
        """
        raise NotImplementedError

    def _when_remove(self, func):  # noqa
        def inner(name, *values, **kwargs):
            return func(name, *[i.fingerprint if isinstance(i, Item) else i for i in values], **kwargs)

        return inner

    def command(self, name: str, order: dict):
        raise NotImplementedError


class LocalQueue(TaskQueue):

    def __init__(self) -> None:
        self._box = dict()
        self._container = getattr(self, "_container", None) or defaultdict(SmartQueue)
        self._locks = defaultdict(threading.Lock)
        self._status = defaultdict(threading.Event)

    def __str__(self):
        return f'<LocalQueue>'

    def size(self, *names: str, qtypes: tuple = ('current', 'temp', 'failure'), **kwargs) -> int:
        if not names:
            return 0
        else:
            names = [self.name2key(name, _) for name in names for _ in qtypes]

        count = 0

        for name in names:
            count += self._container[name].qsize()

        return count

    def reverse(self, name: str, **kwargs) -> bool:
        qtypes = kwargs.pop('qtypes', None) or ["temp", "failure"]

        dest = self.name2key(name, 'current')
        for qtype in qtypes:
            _queue = self.name2key(name, qtype)
            with self._container[_queue].mutex:
                self._container[dest].put(*self._container[_queue].queue)
                self._container[_queue].queue.clear()
        return True

    def smart_reverse(self, name: str, status=0) -> bool:
        tc = self.size(name, qtypes=("temp",))
        cc = self.size(name, qtypes=("current",))
        fc = self.size(name, qtypes=("failure",))

        if cc == 0 and fc != 0:
            qtypes = ['failure']
            need_reverse = True

        elif cc == 0 and fc == 0 and tc != 0 and status == 0:
            qtypes = ['temp']
            need_reverse = True

        else:
            need_reverse = False
            qtypes = []

        if need_reverse:
            self.reverse(name, qtypes=qtypes)
            return True
        else:
            return False

    def merge(self, dest: str, *queues: str, **kwargs):
        with self._container[queues].mutex:
            for _queue in queues:
                self._container[dest].put(*self._container[_queue].queue)
                self._container[_queue].queue.clear()
        return True

    def replace(self, name: str, old, new, **kwargs):
        kwargs.setdefault('qtypes', 'current')
        self.remove(name, *pandora.iterable(old), **kwargs)
        return self.put(name, *pandora.iterable(new), **kwargs)

    def remove(self, name: str, *values, **kwargs):
        backup = kwargs.pop('backup', None)
        backup and self.put(name, *values, qtypes=backup)
        name = self.name2key(name, kwargs.get('qtypes', 'temp'))
        return self._container[name].remove(*values)

    def put(self, name: str, *values, **kwargs):
        name = self.name2key(name, kwargs.pop('qtypes', "current"))
        unique = kwargs.pop('unique', None)
        priority = kwargs.pop('priority', None)
        timeout = kwargs.pop('timeout', None)
        limit = kwargs.pop('limit', 0)
        head = bool(priority)
        return self._container[name].put(*values, block=True, timeout=timeout, unique=unique, limit=limit, head=head)

    def get(self, name: str, count: int = 1, **kwargs) -> Item:
        pop_key = self.name2key(name, 'current')
        add_key = self.name2key(name, 'temp')
        tail = kwargs.pop('tail', False)
        items = self._container[pop_key].get(block=False, timeout=None, count=count, tail=tail)
        items is not None and self._container[add_key].put(*pandora.iterable(items))
        return items

    def clear(self, *names, qtypes=('current', 'temp', "failure", "lock", "record"), **kwargs):
        for name in names:
            for qtype in qtypes:
                self._container.pop(self.name2key(name, qtype), None)

    def command(self, name: str, order: dict):
        def set_init_record():
            os.environ[f'{name}-init-record'] = json.dumps(order['record'], default=str)

        def reset_init_record():
            os.environ.pop(f'{name}-init-record', None)
            self.clear(name)

        def backup_init_record():
            record = order['record']
            os.environ[f'{name}-init-record-backup'] = json.dumps(record, default=str)

        def wait_for_init_start():

            while not self._status[f'{name}-init-status'].is_set() and self.is_empty(name):
                time.sleep(1)
                logger.debug('等待初始化开始')

        actions = {
            self.COMMANDS.GET_PERMISSION: lambda: {"state": True, "msg": "success"},
            self.COMMANDS.GET_INIT_RECORD: lambda: json.loads(os.environ.get(f'{name}-init-record') or "{}"),
            self.COMMANDS.RESET_QUEUE: reset_init_record,
            self.COMMANDS.CONTINUE_INIT_RECORD: lambda: self.reverse(name),
            self.COMMANDS.SET_INIT_RECORD: set_init_record,
            self.COMMANDS.BACKUP_INIT_RECORD: backup_init_record,

            self.COMMANDS.WAIT_FOR_INIT_START: wait_for_init_start,
            self.COMMANDS.SET_INIT_STATUS: lambda: self._status[f'{name}-init-status'].set(),
            self.COMMANDS.IS_INIT_STATUS: lambda: self._status[f'{name}-init-status'].is_set(),
            self.COMMANDS.RELEASE_INIT_STATUS: lambda: self._status[f'{name}-init-status'].clear(),
        }
        action = order['action']
        if action in actions:
            return actions[action]()


class RedisQueue(TaskQueue):
    subscribe = True

    def __init__(self, host='127.0.0.1', password=None, port=6379, database=0, genre="set", **kwargs):

        self.redis_db = Redis(
            host=host,
            password=password,
            port=port,
            database=database,
            **kwargs
        )
        self.host = host
        self.database = database
        self.genre = genre
        self.scripts = {
            "get": self.redis_db.register_script("""
local function pop_item(ktype, key)
    if ktype == "zset" then
        return redis.call(ARGV[3], key, ARGV[2])
    elseif ktype == "set" then
        return redis.call("spop", key, ARGV[2])
    else
        return redis.call("spop", key, ARGV[2])
    end
end

local function get_size(ktype, key)
    if ktype == "zset" then
        return redis.call("zcard", key)
    elseif ktype == "set" then
        return redis.call("scard", key)
    else
        return redis.call("scard", key)
    end
end

local function add_item(ktype, key, values)
    local ret = {}

    if ktype == "zset" then
        for flag = 1, #values do
            if flag % 2 ~= 0 then
                table.insert(ret, values[flag])
                redis.call("zadd", key, values[flag + 1], values[flag])
            end
        end

    elseif ktype == "set" then
        for flag = 1, #values do
            table.insert(ret, values[flag])
            redis.call("sadd", key, values[flag])
        end

    else
        for flag = 1, #values do
            table.insert(ret, values[flag])
            redis.call("sadd", key, values[flag])
        end

    end

    return ret
end

redis.replicate_commands()
redis.call("select", ARGV[1])

-- 获取类型
local pop_key = KEYS[1]
local pop_key_type = redis.call("TYPE", pop_key).ok

local add_key = KEYS[2]
local add_key_type = redis.call("TYPE", add_key).ok

local temp = {}

if add_key_type == 'none' then add_key_type = ARGV[4] end
if pop_key_type == 'none' then pop_key_type = ARGV[4] end

temp = pop_item(pop_key_type, pop_key)
return add_item(add_key_type, add_key, temp)



"""),
            "put": self.redis_db.register_script("""
redis.replicate_commands()
redis.call("select", KEYS[1])

local success = 0
if KEYS[3] == "zset" then

    local v
    if KEYS[4] == "max" then
        v = redis.call("zrevrangebyscore", KEYS[2], "+inf", "-inf", "LIMIT", 0, 1, "withscores")[2]

    elseif KEYS[4] == "min" then
        v = redis.call("zrevrangebyscore", KEYS[2], "-inf", "+inf", "LIMIT", 0, 1, "withscores")[2]

    else

        v = tonumber(KEYS[4])
    end 


    if v == nil then v = 0 end
    for i = 1, #ARGV do
        local r = redis.call("zadd", KEYS[2], v+1, ARGV[i])
        success = success + r
        v = v + 1
    end

else
    for i = 1, #ARGV do
        local r = redis.call("sadd", KEYS[2], ARGV[i])
        success = success + r
    end

end
return success

            """),
            "replace": self.redis_db.register_script('''
redis.replicate_commands()
redis.call("select", ARGV[1])
local ret = 0

for index = 1, #KEYS, 1 do
    local key_type = redis.call("TYPE", KEYS[index]).ok
    local need_remove = {}
    local need_add = {}

    for i = 2, #ARGV, 2 do
        table.insert(need_remove, ARGV[i])
        table.insert(need_add, ARGV[i + 1])
    end

    if key_type == "set" then
        -- 删除
        redis.call("srem", KEYS[index], unpack(need_remove))
        -- 添加
        local r = redis.call("sadd", KEYS[index], unpack(need_add))
        ret = ret + r


    elseif key_type == "zset" then
        -- 删除
        redis.call("zrem", KEYS[index], unpack(need_remove))

        -- 计算初试分数
        local v = redis.call("zrevrangebyscore", KEYS[index], "+inf", "-inf", "LIMIT", 0, 1, "withscores")[2]
        if v == nil then v = 0 end
        local values = {}

        for i = 1, #need_add, 1 do
            table.insert(values, v + i+1)
            table.insert(values, need_add[i])
        end

        -- 添加
        local r = redis.call("zadd", KEYS[index], unpack(values))
        ret = ret + r

    elseif key_type == "list" then
        -- 删除
        redis.call("lrem", KEYS[index], 0, unpack(need_remove))
        -- 添加
        local r = redis.call("lpush", KEYS[index], unpack(need_add))
        ret = ret + r

    else
        -- 删除
        redis.call("srem", KEYS[index], unpack(need_remove))
        -- 添加
        local r = redis.call("sadd", KEYS[index], unpack(need_add))
        ret = ret + r

    end


end
return ret


'''),
            "remove": self.redis_db.register_script("""
redis.replicate_commands()
redis.call("select", KEYS[1])


local key_type = redis.call("TYPE", KEYS[2]).ok
if key_type == "zset" then
    local success = redis.call("zrem", KEYS[2], unpack(ARGV))
    if KEYS[3] ~= '' then
        local v = redis.call("zrevrangebyscore", KEYS[2], "+inf", "-inf", "LIMIT", 0, 1, "withscores")[2]
        if v == nil then v = 0 end
        local values = {{}}

        for i = 1, #ARGV, 1 do
            table.insert(values, v + i+1)
            table.insert(values, ARGV[i])
        end

        redis.call("zadd", KEYS[3], unpack(values))

    end

    return success

else
    local success = redis.call("srem", KEYS[2], unpack(ARGV))
    if KEYS[3] ~= '' then
        redis.call("sadd", KEYS[3], unpack(ARGV))
    end

    return success
end


        """),
            "clear": self.redis_db.register_script("""
        redis.replicate_commands()
        redis.call("select", KEYS[1])

        local success = 0
        for i = 1, #ARGV 
        do 
            success= success + redis.call("DEL",ARGV[i])
        end
        return success
                """),
            "size": self.redis_db.register_script("""
redis.replicate_commands()
redis.call("select", ARGV[1])
local count = 0
local cmd = 'scard'
for flag = 1, #KEYS do
    local key_type = redis.call("TYPE", KEYS[flag]).ok
    if key_type == "zset" then
        cmd = 'zcard'
    else
        cmd = 'scard'
    end

    count = count + redis.call(cmd, KEYS[flag])
end
return count

        """),
            "reverse": self.redis_db.register_script(f"""
redis.replicate_commands()
redis.call("select", KEYS[2])

for flag = 1, #ARGV do
    local key_type = redis.call("TYPE", ARGV[flag]).ok
    if key_type == 'zset' then
        redis.call("ZUNIONSTORE", KEYS[1], #ARGV + 1, KEYS[1], ARGV[flag])

    else
        redis.call("SUNIONSTORE", KEYS[1], KEYS[1], ARGV[flag])
    end
    redis.call("DEL", ARGV[flag])
end
return true

"""),
            "merge": self.redis_db.register_script(f"""
redis.replicate_commands()
redis.call("select", KEYS[2])

for flag = 1, #ARGV do
    local key_type = redis.call("TYPE", ARGV[flag]).ok
    if key_type == 'zset' then
        redis.call("ZUNIONSTORE", KEYS[1], #ARGV + 1, KEYS[1], ARGV[flag])

    else
        redis.call("SUNIONSTORE", KEYS[1], KEYS[1], ARGV[flag])
    end
    redis.call("DEL", ARGV[flag])
end
return true

"""),
            "smart_reverse": self.redis_db.register_script("""

redis.replicate_commands()

-- 初始化变量
local current_key = KEYS[1]
local temp_key = KEYS[2]
local failure_key = KEYS[3]
local status_key = KEYS[4]
local report_key = KEYS[5]
local db_num = ARGV[1]
local time_str = ARGV[2]

local function get_queue_info(queue_name)
    local ret = {}
    local queue_type = redis.call("TYPE", queue_name).ok
    local queue_size
    if queue_type == "zset" then
        queue_size = redis.call("zcard", queue_name)
    else
        queue_size = redis.call("scard", queue_name)
    end 
    
    ret['type'] = queue_type
    ret['size'] = queue_size
    ret['name'] = queue_name
    return ret
end

local function merge(dest_type,dest_name, target)
    if dest_type == "zset" then
        -- 合并
        redis.call("ZUNIONSTORE", target, 2, target, dest_name)
        -- 删除 failure key
        redis.call("DEL", dest_name)
        
    else
        -- 合并
        redis.call("SUNIONSTORE", target, target, dest_name)
        -- 删除 failure key
        redis.call("DEL", dest_name)
        
    end
end

-- 切换数据库
redis.call("select", ARGV[1])

-- 设置初拾变量 running
local running = 0
local clients = redis.call("HGETALL", report_key)

-- 将所有客户端上报上来的正在运行的线程数相加得到集群总运行的任务数量
for flag = 1, #clients,2 do
    local c = clients[flag + 1]
    running = running + tonumber(c)
    redis.call("HDEL", report_key, clients[flag])
end

-- 获取 failure 队列中的数量
local failure_queue_info = get_queue_info(failure_key)

-- failure 有值 -> 将 failure 放入 current
if failure_queue_info.size > 0 then
    merge(failure_queue_info.type, failure_queue_info.name , current_key)
    return true
end

-- 判断 running 是否大于 0
if running > 0 then 
    return 'running > 0'
end

-- failure 无值
-- 获取 temp 队列中的数量
local temp_queue_info = get_queue_info(temp_key)
local current_queue_info = get_queue_info(current_key)

-- temp 有值, running == 0 -> 将 temp 放入 current
if temp_queue_info.size > 0 then
    merge(temp_queue_info.type, temp_queue_info.name, current_queue_info.name)
    return true
else
    return "temp.size <=0"
end


"""),
            "get_permission": self.redis_db.register_script("""
            redis.replicate_commands()
            local current_key = KEYS[1]
            local temp_key = KEYS[2]
            local failure_key = KEYS[3]
            local record_key = KEYS[4]
            local machine_id = ARGV[1]
            
            local function get_queue_info(queue_name)
                local ret = {}
                local queue_type = redis.call("TYPE", queue_name).ok
                local queue_size
                if queue_type == "zset" then
                    queue_size = redis.call("zcard", queue_name)
                else
                    queue_size = redis.call("scard", queue_name)
                end 
                
                ret['type'] = queue_type
                ret['size'] = queue_size
                ret['name'] = queue_name
                return ret
            end
            
            local queue_size = get_queue_info(current_key).size + get_queue_info(failure_key).size + get_queue_info(temp_key).size
            if queue_size > 0 then
                -- 1. 存在种子 
                local is_record_key_exists = redis.call("EXISTS", record_key)
                if is_record_key_exists == 1 then
                    -- 1.1 存在 record + record 内的 machine_id 与当前机器的 machine_id 相同 -> true
                    local identifier = redis.call("HGET", record_key, "identifier")
                    if machine_id == identifier then
                        return true
                    else
                        return "非初始化机器"
                    end
                else
                    -- 1.2 不存在 record -> false
                    return "存在未消耗完毕的种子"
                end
            
            else
                -- 2. 不存在种子
                -- 2.1 存在 record + record 内的 machine_id 与当前机器的 machine_id 相同 -> true
                local is_record_key_exists = redis.call("EXISTS", record_key)
                if is_record_key_exists == 1 then
                    -- 1.1 存在 record + record 内的 machine_id 与当前机器的 machine_id 相同 -> true
                    local identifier = redis.call("HGET", record_key, "identifier")
                    if machine_id == identifier then
                        return true
                    else
                        return "非初始化机器"
                    end
                else
                    -- 不存在 record -> true
                    return true
                end
            
            
            end
            
            """),
        }

    def get(self, name, **kwargs):
        """
        从 `name` 中获取种子

        :param name:
        :param kwargs:
        :return:
        """

        pop_key = self.name2key(name, 'current')
        add_key = self.name2key(name, 'temp')
        action = kwargs.pop('action', 'zpopmin')
        db_num = kwargs.get('db_num', self.database)
        keys = [pop_key, add_key]
        args = [db_num, kwargs.get('count', 1), action, self.genre]
        items = self.scripts["get"](keys=keys, args=args)
        return items

    def put(self, name, *values, **kwargs):
        """
        投放种子至 `name`

        :param name:
        :param values:
        :param kwargs:
        :return:
        """
        db_num = kwargs.get('db_num', self.database)
        genre = kwargs.get('genre', self.genre)
        priority = kwargs.get('priority', False)
        if type(priority) is not int:
            score = "max" if not priority else "min"
        else:
            score = priority

        name = self.name2key(name, kwargs.pop('qtypes', "current"))
        if not name or not values:
            return 0

        name = name['current'] if isinstance(name, dict) else name
        values = self._to_str(*values)

        if db_num is None:
            return self.redis_db.sadd(name, *values)
        else:
            keys = [db_num, name, genre, score]
            args = [*values]
            count = self.scripts["put"](keys=keys, args=args)
            return count

    def replace(self, name, old, new, **kwargs):
        """
        从 `name` 中 pop 出 `count` 个值出来

        :param new: 新
        :param old: 旧
        :param name: 队列名称

        :return:
        """
        if not name:
            return

        db_num = kwargs.get('db_num', self.database)
        name = self.name2key(name, kwargs.pop('qtypes', "current"))

        db_num = self.database if db_num is None else db_num
        keys = [name]
        args = [db_num, *[j for i in zip(pandora.iterable(old), pandora.iterable(new)) for j in self._to_str(*i)]]
        return self.scripts["replace"](keys=keys, args=args)

    def remove(self, name, *values, **kwargs):
        """
        从 `name` 中移除 `values`

        :param name:
        :param values:
        :return:
        """
        backup = kwargs.pop('backup', "") or ""
        if backup: backup = self.name2key(name, backup)

        name = self.name2key(name, kwargs.pop('qtypes', "temp"))
        keys = [kwargs.pop('db_num', self.database), name, backup]
        args = self._to_str(*values)
        return self.scripts["remove"](keys=keys, args=args)

    def clear(self, *names, qtypes=('current', 'temp', "lock", "record", "failure"), **kwargs):
        db_num = kwargs.pop('db_num', self.database)
        keys = [db_num]
        args = [self.name2key(name, qtype) for name in names for qtype in qtypes]
        if db_num is None:
            return self.redis_db.delete(*args)

        return self.scripts["clear"](keys=keys, args=args)

    def size(self, *names, qtypes=('current', 'temp', "failure"), **kwargs):
        """
        获取 `names` 的队列大小

        :param qtypes:
        :param names:
        :return:
        """
        db_num = kwargs.pop('db_num', self.database)
        if not names:
            return 0
        else:
            names = [self.name2key(name, _) for name in names for _ in qtypes]
        db_num = self.database if db_num is None else db_num
        keys = [*names]
        args = [db_num]
        return self.scripts["size"](keys=keys, args=args)

    def reverse(self, name, **kwargs):
        """
        队列翻转

        :param name:
        :return:
        """
        db_num = kwargs.pop('db_num', self.database)
        qtypes = kwargs.pop('qtypes', None) or ["temp", "failure"]
        dest = self.name2key(name, 'current')

        db_num = self.database if db_num is None else db_num
        keys = [dest, db_num]
        args = [self.name2key(name, qtype) for qtype in pandora.iterable(qtypes)]
        return self.scripts["reverse"](keys=keys, args=args) or False

    def merge(self, dest, *queues, **kwargs):
        """
        队列合并

        :param dest:
        :param queues:
        :return:
        """
        db_num = kwargs.pop('db_num', self.database)
        db_num = self.database if db_num is None else db_num
        keys = [dest, db_num]
        args = queues
        return self.scripts["merge"](keys=keys, args=args) or False

    def smart_reverse(self, name, timeout=1, **kwargs):
        """
        智能翻转队列
        翻转队列的条件是:
        1. failure 有值 -> 将 failure 放入 current
        2. failure 无值, temp 有值, running == 0 -> 将 temp 放入 current

        :param name:
        :param timeout:
        :return:
        """

        # 告诉其他机器开始上报状态
        self.redis_db.publish(f'{name}-subscribe', "collect-status")
        # 等在上报完成
        time.sleep(timeout)

        db_num = kwargs.pop('db_num', self.database)
        keys = [self.name2key(name, i) for i in ('current', 'temp', 'failure', 'status', 'report')]
        args = [
            db_num or self.database,
            str(datetime.datetime.now())
        ]

        ret = self.scripts["smart_reverse"](keys=keys, args=args)
        return ret == 1

    def is_empty(self, name, timeout=5, threshold=0, **kwargs):
        """
        判断 `name` 是否为空

        :param threshold:
        :param name:
        :param timeout:
        :return:
        """
        db_num = kwargs.pop('db_num', self.database)
        stime = time.time()
        while time.time() - stime < timeout:
            try:
                assert self.size(name, db_num=db_num) <= threshold and not self.redis_db.exists(
                    self.name2key(name, 'status'),
                    self.name2key(name, 'lock'),
                )
            except AssertionError:
                return False
        return True

    @classmethod
    def from_redis(cls, obj, **kwargs):
        connection_kwargs: dict = obj.connection_pool.connection_kwargs
        params = dict(
            host=connection_kwargs.get('host', '127.0.0.1'),
            password=connection_kwargs.get('password'),
            port=connection_kwargs.get('port', 6379),
            database=connection_kwargs.get('db', 0),
        )
        params.update(**kwargs)
        return cls(**params)

    def command(self, name: str, order: dict):

        def run_subscribe(channel, target):
            """
            订阅
            """

            def main(message):
                # 上报状态
                if message['data'] == "collect-status":
                    self.redis_db.hset(
                        self.name2key(name, 'report'),
                        mapping={const.MACHINE_ID: target.dispatcher.running}
                    )

            pubsub = self.redis_db.pubsub()
            pubsub.subscribe(**{channel: main})
            pubsub.run_in_thread(sleep_time=0.001, daemon=True)

        def get_permission():
            """
            判断是否有初始化权限
            判断依据:
            1. 存在种子
                1.1 存在 record + record 内的 machine_id 与当前机器的 machine_id 相同 -> true
                1.2 不存在 record -> false
            2. 不存在种子
                2.1 存在 record + record 内的 machine_id 与当前机器的 machine_id 相同 -> true
                2.2 不存在 record -> true

            :return:
            :rtype:
            """
            keys = [self.name2key(name, i) for i in ['current', 'temp', 'failure', 'record']]
            args = [const.MACHINE_ID]
            ret = self.scripts["get_permission"](keys=keys, args=args)
            return {
                "state": ret == 1,
                "msg": ret
            }

        def set_init_record():
            self.redis_db.hset(
                self.name2key(name, 'record'),
                mapping=json.loads(json.dumps(order['record'], default=str))
            )

        def backup_init_record():
            dst = self.name2key(name, 'history')
            src = self.name2key(name, 'record')
            record = order['record']
            self.redis_db.delete(dst, src)
            self.redis_db.hset(dst, mapping=json.loads(
                json.dumps({**record, "finish": str(datetime.datetime.now())}, default=str)))
            history_ttl = order.get('ttl')
            history_ttl and self.redis_db.expire(dst, history_ttl)

        def wait_for_init_start():
            # 没有 init-status 这个 key 并且队列为空 -> 一直等待
            while not self.redis_db.exists(self.name2key(name, 'init-status')) and self.is_empty(name):
                logger.debug('等待初始化开始')
                time.sleep(1)

        def set_init_status():
            # 设置 init-status 信息
            self.redis_db.hset(self.name2key(name, 'init-status'), mapping={
                "identifier": const.MACHINE_ID,
                "time": str(datetime.datetime.now())
            })

        actions = {
            self.COMMANDS.RUN_SUBSCRIBE: lambda: run_subscribe(f'{name}-subscribe', order['target']),
            self.COMMANDS.GET_PERMISSION: get_permission,
            self.COMMANDS.GET_INIT_RECORD: lambda: self.redis_db.hgetall(self.name2key(name, 'record')),
            self.COMMANDS.RESET_QUEUE: lambda: self.clear(name),
            self.COMMANDS.CONTINUE_INIT_RECORD: lambda: self.reverse(name),
            self.COMMANDS.SET_INIT_RECORD: set_init_record,
            self.COMMANDS.BACKUP_INIT_RECORD: backup_init_record,
            self.COMMANDS.WAIT_FOR_INIT_START: wait_for_init_start,
            self.COMMANDS.SET_INIT_STATUS: set_init_status,
            self.COMMANDS.IS_INIT_STATUS: lambda: self.redis_db.exists(self.name2key(name, 'init-status')),
            self.COMMANDS.RELEASE_INIT_STATUS: lambda: self.redis_db.delete(self.name2key(name, 'init-status')),
        }
        action = order['action']
        if action in actions:
            return actions[action]()

    def __str__(self):
        return f'<RedisQueue [ HOST: {self.host} | DB: {self.database} ]>'


if __name__ == '__main__':
    def my_callback(msg):
        print(msg)
        rds.redis_db.hset(rds.name2key("xxx", "report"), const.MACHINE_ID, "0")


    rds = RedisQueue()
    # rds.command("xxx", {"action": rds.COMMANDS.RUN_SUBSCRIBE, "callback": my_callback})
    print(rds.command("xxx", {"action": rds.COMMANDS.GET_PERMISSION, "callback": my_callback}))
    # time.sleep(5)
