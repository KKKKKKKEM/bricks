# -*- coding: utf-8 -*-
# @Time    : 2023-11-15 16:43
# @Author  : Kem
# @Desc    :
from __future__ import absolute_import

import copy
import datetime
import json
import time
from typing import Union, List

import redis
from loguru import logger

from bricks.utils import pandora


def _to_str(*args):
    return [
        json.dumps(
            value,
            default=str,  # noqa
            sort_keys=True,
            ensure_ascii=False,
            escape_forward_slashes=False
        )
        if not isinstance(value, (bytes, str, int, float)) else value
        for value in args
    ]


class Redis(redis.client.Redis):
    """
    Redis 工具
    """

    def __init__(self, host='127.0.0.1', password=None, port=6379, database=0, **kwargs):
        """
        实例化一个 Redis 对象

        :param host: host
        :param password: 密码
        :param port: 端口
        :param database: 数据库
        :param kwargs: 其他参数
        """

        self.database = database
        self.host = host
        self.password = password
        self.port = port
        self._kwargs = copy.deepcopy(kwargs)

        super().__init__(
            host=host,
            password=password,
            port=port,
            db=database,
            decode_responses=True,
            **kwargs
        )

    def setbits(self, names, offsets, value, db_num=None):
        """
        bloom setbits

        :param names:
        :param offsets:
        :param value:
        :param db_num:
        :return:
        """
        db_num = self.database if db_num is None else db_num
        keys = [value, *names]
        args = [db_num, *offsets]
        lua = """
redis.replicate_commands()
redis.call("select", ARGV[1])
for flag = 2, #ARGV 
do 
    redis.call("setbit",KEYS[flag],ARGV[flag], KEYS[1])
end
 """
        script = self.register_script(lua)
        return script(keys=keys, args=args)

    def getbits(self, names, offsets, db_num=None):
        db_num = self.database if db_num is None else db_num
        keys = names
        args = [db_num, *offsets]
        lua = """
redis.replicate_commands()
redis.call("select", ARGV[1])
local ret = {}
for flag = 2, #ARGV 
do 
    local r = redis.call("getbit",KEYS[flag-1],ARGV[flag])
    table.insert(ret, r)
end
return ret
            """
        script = self.register_script(lua)
        return script(keys=keys, args=args)

    def acquire_lock(self, name, timeout=3600 * 24, block=False, prefix='', value=None, db_num=None):
        """
        获取redis全局锁

        :param db_num: 数据库编号
        :param value: 锁的内容
        :param prefix: 锁前缀
        :param name: 全局锁的名称
        :param timeout: 自动释放的时间
        :param block: 拿不到锁的时候是否要阻塞
        :return:

        """
        prefix = prefix + ":Lock:" if prefix else ""
        db_num = db_num or self.database
        keys = [f'{prefix}{name}', value or datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), timeout]
        args = [db_num]
        lua = """
redis.replicate_commands()
redis.call("select", ARGV[1])
local ret = redis.call("SETNX",KEYS[1],KEYS[2])
if(ret == 1)
then
   redis.call("EXPIRE",KEYS[1],KEYS[3])
end
return ret

"""
        script = self.register_script(lua)
        while True:
            ret = script(keys=keys, args=args)
            if ret or not block:
                return ret
            else:
                logger.debug(f'等待全局锁 {name} 释放')
                time.sleep(1)

    def release_lock(self, name, prefix=''):
        """
        释放redis全局锁

        :param prefix: 锁前缀
        :param name: 锁的名称

        :return:
        """

        prefix = prefix + ":Lock:" if prefix else ""
        name = f'{prefix}{name}'
        self.delete(name)

    def wait_lock(self, name, prefix=''):
        """
        等待锁释放

        :param name: 锁的名称
        :param prefix:前缀
        :return:
        """

        prefix = prefix + ":Lock:" if prefix else ""
        name = f'{prefix}{name}'

        while True:
            try:
                if not self.keys(name):
                    return
                else:
                    logger.debug(f"等待全局锁 {name} 释放")
                    time.sleep(1)

            except Exception as e:
                logger.exception(e)

    def limit(self, *names, maxsize=1000000, interval=5, db_num=None):
        """
        限制redis队列的数量

        :param names: 被限制的队列名称
        :param maxsize: 最大尺寸
        :param interval: 睡眠间隔
        :param db_num: 数据库编号
        :return:
        """
        maxsize = maxsize or 1000000
        while True:
            # If the number of the current queue is greater than the critical value, then sleep
            try:
                if sum(pandora.iterable(self.count(*names, db_num=db_num))) >= maxsize:
                    time.sleep(interval)
                else:
                    return True
            except Exception as e:
                logger.exception(e)

    def count(self, *names, db_num=None):
        """
        获取 set / zset / list / hash 等队列的数量

        :param names: 要计数的队列名称
        :param db_num: 数据库编号
        :return:
        """
        if not names:
            return 0

        db_num = self.database if db_num is None else db_num

        keys = names
        args = [db_num]
        lua = """
redis.replicate_commands()
redis.call("select", ARGV[1])
local ret = {}
for flag = 1, #KEYS do
    local key_type = redis.call("TYPE", KEYS[flag]).ok
    if key_type == "zset" then
        ret[flag] = redis.call("zcard", KEYS[flag])
    elseif key_type == "none" then
        ret[flag] = 0
    elseif key_type == "list" then
        ret[flag] = redis.call("llen", KEYS[flag])
    elseif key_type == "hash" then
        local v = redis.call("hgetall", KEYS[flag])
        local t = 0
        for i = 1, #v, 2 do t = t + tonumber(v[i + 1]) end
        ret[flag] = t
    else
        ret[flag] = redis.call("scard", KEYS[flag])
    end
end
return ret
"""
        script = self.register_script(lua)
        ret = script(keys=keys, args=args)
        return pandora.single(ret) if len(names) == 1 else ret

    def delete(self, *names, db_num=None):
        """
        删除 `names`

        :param names: 你需要删除的names
        :param db_num: 数据库编号
        :return:
        """
        if not names:
            return 0
        db_num = self.database if db_num is None else db_num
        keys = [db_num]
        args = names
        lua = '''
redis.replicate_commands()
redis.call("select", KEYS[1])
local success = 0
for i = 1, #ARGV do
    local r = redis.call("DEL", ARGV[i])
    success = success + r
end
return success
'''
        script = self.register_script(lua)
        return script(keys=keys, args=args)

    def add(self, name: Union[str, List[str]], *values, db_num=None, genre=None, maxsize=0):
        """
        添加 `values` 至 `name` 中

        :param maxsize: 队列最大种子数量
        :param name: 需要添加的值的名称
        :param values: 需要增加的 values
        :param db_num: 数据库编号
        :param genre: 名称的类型可以手动制定，如果存在会自动判断。默认已设置
        :return:
        """
        if not name:
            return 0

        if genre is None:
            genre = "redis.call('TYPE', KEYS[index]).ok"

        db_num = self.database if db_num is None else db_num
        keys = [db_num, maxsize, *pandora.iterable(name)]
        args = _to_str(*values)
        lua = f'''
redis.replicate_commands()
redis.call("select", KEYS[1])
local success = 0
local maxsize = tonumber(KEYS[2])
for index = 3, #KEYS do
    local key_type = {"redis.call('TYPE', KEYS[index]).ok" if genre is None else genre!r}
    if key_type == "zset" then

        if maxsize ~= 0 then
            -- 将重复的移除
            redis.call("zrem", KEYS[index], unpack(ARGV))
            -- 计算当前总数量
            local current_count = redis.call("zcard", KEYS[index])
            -- 计算需要删除多少数量
            local remove_count = current_count - maxsize + #ARGV
            if remove_count > 0 then
                -- 将多余的删除
                redis.call("zpopmin", KEYS[index], remove_count)
            end
        end
        -- 获取当前最大的分数
        local v = redis.call("zrevrangebyscore", KEYS[index], "+inf", "-inf", "LIMIT", 0, 1, "withscores")[2]
        if v == nil then v = 0 end
        local values = {{}}

        for i = 1, #ARGV, 1 do
            table.insert(values, v + i+1)
            table.insert(values, ARGV[i])
        end

        local r = redis.call("zadd", KEYS[index], unpack(values))
        success = success + r

    elseif key_type == "list" then
        if maxsize ~= 0 then

            local current_count = redis.call("llen", KEYS[index])
            local remove_count = current_count + #ARGV - maxsize
            if remove_count > 0 then
                for i = 1, remove_count do
                    redis.call("rpop", KEYS[index])
                end
            end

        end
        local r = redis.call("lpush", KEYS[index], unpack(ARGV))
        success = success + r

    else
        if maxsize ~= 0 then
            -- 将重复的移除
            redis.call("srem", KEYS[index], unpack(ARGV))
            -- 计算当前总数量
            local current_count = redis.call("scard", KEYS[index])
            -- 计算需要删除多少数量
            local remove_count = current_count - maxsize + #ARGV
            if remove_count > 0 then
                -- 将多余的删除
                redis.call("spop", KEYS[index], remove_count)
            end

        end

        local r = redis.call("sadd", KEYS[index], unpack(ARGV))
        success = success + r
    end
end
return success

'''
        script = self.register_script(lua)
        return script(keys=keys, args=args)

    def pop(self, name: Union[str, List[str]], count=1, db_num=None, backup=None):
        """
        从 `name` 中 pop 出 `count` 个值出来

        :param backup: pop 的同时加入到 backup 队列中
        :param name: 队列名称
        :param count: 数量;
        :param db_num: 数据库编号
        :return:
        """
        if not name:
            return
        db_num = self.database if db_num is None else db_num
        keys = [backup or '', *pandora.iterable(name)]
        args = [db_num, count]
        lua = '''
redis.replicate_commands()
redis.call("select", ARGV[1])
local ret = {}

for index = 2, #KEYS, 1 do
    local key_type = redis.call("TYPE", KEYS[index]).ok

    if key_type == "set" then
        local temp = redis.call("spop", KEYS[index], ARGV[2])
        for flag = 1, #temp do
            if KEYS[1] ~= '' then
                redis.call("sadd", KEYS[1], temp[flag])
            end
            table.insert(ret, temp[flag])
        end

    elseif key_type == "zset" then
        local temp = redis.call("zpopmin", KEYS[index], ARGV[2])
        for i = 1, #temp do
            if i % 2 ~= 0 then
                table.insert(ret, temp[i])
                if KEYS[1] ~= '' then
                    redis.call("zadd", KEYS[1], temp[i + 1], temp[i])
                end
            end
        end

    elseif key_type == "list" then
        local count = 0
        while count < tonumber(ARGV[2]) do
            local t = redis.call("rpop", KEYS[index])
            count = count + 1
            if not t then
                break
            else
                table.insert(ret, t)

            end
        end

        if KEYS[1] ~= '' then
            for flag = 1, #ret do
                redis.call("lpush", KEYS[1], ret[flag])
            end
        end

    else
        table.insert(ret, nil)
    end

end
return ret
'''
        script = self.register_script(lua)
        ret = script(keys=keys, args=args)
        if ret:
            return ret[0] if len(ret) == 1 else ret
        return None

    def remove(self, name: Union[str, List[str]], *values, db_num=None):
        """
        从 `name` 中删除 `values`

        :param name: 队列名称
        :param values: 需要删除的  values
        :param db_num: 数据库编号
        :return:
        """
        if not name:
            return
        db_num = self.database if db_num is None else db_num
        keys = [db_num, *pandora.iterable(name)]
        args = _to_str(*values)
        lua = '''
redis.replicate_commands()
redis.call("select", KEYS[1])
local success = 0

for index = 2, #KEYS, 1 do
    local key_type = redis.call("TYPE", KEYS[index]).ok
    local cmd = ""

    if key_type == "set" then
        cmd = "srem"
    elseif key_type == "zset" then
        cmd = "zrem"
    elseif key_type == "list" then
        cmd = "lrem"
    else
        cmd = "srem"
    end

    local r
    if cmd == 'lrem' then
        r = redis.call(cmd, KEYS[index], 0, unpack(ARGV))
    else
        r = redis.call(cmd, KEYS[index], unpack(ARGV))
    end
    success = success + r

end
return success

'''
        script = self.register_script(lua)
        return script(keys=keys, args=args)

    def replace(self, name: Union[str, List[str]], old, new, db_num=None):
        """
        从 `name` 中 pop 出 `count` 个值出来

        :param new: 新
        :param old: 旧
        :param name: 队列名称
        :param db_num: 数据库编号
        :return:
        """
        if not name:
            return
        db_num = self.database if db_num is None else db_num
        keys = [*pandora.iterable(name)]
        args = [db_num, *[
            j for i in zip(pandora.iterable(old), pandora.iterable(new)) for j in _to_str(*i)
        ]]
        lua = '''
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


'''
        script = self.register_script(lua)
        return script(keys=keys, args=args)

    def merge(self, dest, *sources, db_num=None):
        """
        合并队列

        :param dest: 目标队列
        :param sources: 源队列
        :param db_num: 数据库编号
        :return:
        """
        db_num = self.database if db_num is None else db_num
        keys = [dest, db_num]
        args = sources
        lua = '''
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
'''
        script = self.register_script(lua)
        return script(keys=keys, args=args)

    def command(self, cmd, *args, db_num=None):
        """
        可以跨库执行命令

        :param cmd: 命令名称：sadd、spop 等
        :param args: 命令对应的参数
        :param db_num: 在哪个数据库中执行
        :return:
        """
        db_num = self.database if db_num is None else db_num
        keys = [db_num]
        args = [cmd, *args]
        lua = f"""
redis.replicate_commands()
redis.call("select", KEYS[1])
return redis.call({",".join([json.dumps(i, escape_forward_slashes=False) for i in args])})
        """
        script = self.register_script(lua)
        return script(keys=keys, args=args)


if __name__ == '__main__':
    redis = Redis()
    print(redis.hgetall('__main__:PC:history'))
