# -*- coding: utf-8 -*-
# @Time    : 2023-11-15 16:43
# @Author  : Kem
# @Desc    :
from __future__ import absolute_import

import copy
import datetime
import functools
import json
import time
from typing import Union, List, Literal

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
            # escape_forward_slashes=False
        )
        if not isinstance(value, (bytes, str, int, float)) else value
        for value in args
    ]


class LUA:

    def __init__(self, conn: "Redis", base: str = ...) -> None:
        self.conn = conn
        if base is ...:
            base = """
            
--更换数据库
local function changeDataBase(db_num)
    redis.replicate_commands()
    redis.call("select", db_num)
end
--获取key的类型
local function getKeyType(key, default_type)
    -- .ok 正式情况下要加上, 调试的时候不需要 .ok
    local kt = redis.call("TYPE", key).ok
    if default_type and kt == "none" then
        return default_type
    else
        return kt
    end

end
--获取 key 内的数据条数
local function getKeySize(key, default_type)
    local keyType = getKeyType(key, default_type)
    if keyType == "none" then
        return 0
    elseif keyType == "set" then
        return redis.call("SCARD", key)
    elseif keyType == "zset" then
        return redis.call("ZCARD", key)
    elseif keyType == "list" then
        return redis.call("LLEN", key)
    elseif keyType == "hash" then
        local v = redis.call("hgetall", KEYS[flag])
        local t = 0
        for i = 1, #v, 2 do t = t + tonumber(v[i + 1]) end
        return t 
    else
        return redis.error_reply("[getKeySize] ERR unknown key type: ".. keyType)
    end


end
-- 合并多个 key 到 dest 内
local function mergeKeys(dest, froms, default_type)
    local keyType = getKeyType(dest, default_type)
    local ret
    if keyType == "set" then
        ret = redis.call("SUNIONSTORE", dest, dest, unpack(froms))
    elseif keyType == 'zset' then
        ret = redis.call("ZUNIONSTORE", dest, #froms + 1, dest, unpack(froms))
    elseif keyType == 'list' then
        local mergedList = {}
        for i = 1, #froms do
            for j = 1, redis.call('LLEN', froms[i]) do
                table.insert(mergedList, redis.call('LINDEX', froms[i], j - 1))
            end
        end
        ret = redis.call('RPUSH', dest, unpack(mergedList))
    else
        return redis.error_reply("ERR unknown key type: ".. keyType)
    end
    redis.call("DEL", unpack(froms))
    return ret
end

--将 values 添加至 key 中 (原封不同)
local function backupItems(key, values, default_type)
    local keyType = getKeyType(key, default_type)

    -- list, 从右边加进去
    if keyType == "list" then
        redis.call("RPUSH", key, unpack(values))
        return #values

        -- set, 一次全部加进去
    elseif keyType == "set" then
        return redis.call("SADD", key, unpack(values))

        -- zset, 从最小分数开始递增
    elseif keyType == "zset" then
        return redis.call("ZADD", key, unpack(values))
    else
        return redis.error_reply("ERR unknown key type: ".. keyType)
    end


end
--从key 中 pop 出 count 条数据
local function popItems(key, count, default_type, backup_key)
    local keyType = getKeyType(key, default_type)
    local ret
    local values = {}
    count = count or 1

    -- list, 从左边弹出
    if keyType == "list" then
        ret = redis.call("LPOP", key, count)
        values = ret

        -- set, 从左边弹出
    elseif keyType == "set" then
        ret = redis.call("SPOP", key, count)
        values = ret

        -- zset, 从最小分数开始弹出
    elseif keyType == "zset" then
        -- 获取最大的分数
        values = redis.call("ZPOPMIN", key, count)
        ret = {}
        for i = 1, #values, 2 do
            table.insert(ret, values[i])
        end
    elseif keyType == "none" then
        ret = {}
    else
        return redis.error_reply("ERR unknown key type: ".. keyType)
    end

    if #values > 0 and backup_key and backup_key ~= "" then
        backupItems(backup_key, values, keyType)
    end
    return ret

end

--将 values 添加至 key 中
local function addItems(key, values, default_type, maxsize)
    -- 移除多余的数量
    if maxsize and tonumber(maxsize) ~= 0 then
        local currentCount = getKeySize(key, default_type)
        local removeCount = currentCount + #values - maxsize
        popItems(key, removeCount, default_type)
    end

    local keyType = getKeyType(key, default_type)
    -- list, 从右边加进去
    if keyType == "list" then
        redis.call("RPUSH", key, unpack(values))
        return #values

        -- set, 一次全部加进去
    elseif keyType == "set" then
        return redis.call("SADD", key, unpack(values))

        -- zset, 从最小分数开始递增
    elseif keyType == "zset" then
        -- 获取最大的分数
        local maxScore = redis.call('ZRANGE', key, "+inf", "-inf", "BYSCORE", "REV", "LIMIT", 0, 1, "withscores")[2]
        -- 不存在的时候初始为 0
        maxScore = maxScore or 0
        -- 转为数字
        maxScore = tonumber(maxScore)
        local ret = 0
        for i = 1, #values, 1 do
            local status, currentValue = pcall(cjson.decode, values[i])
            if status and tonumber(currentValue["$score"]) then
                ret = ret + redis.call('ZADD', key, currentValue["$score"], values[i])
            else
                maxScore = maxScore + 1
                ret = ret + redis.call('ZADD', key, maxScore, values[i])
            end
            
        end

        return ret

    else
        return redis.error_reply("ERR unknown key type: ".. keyType)
    end


end

--从key 中删除 values
local function removeItems(key, values, default_type, backup_key)
    local keyType = getKeyType(key, default_type)
    local removed = 0
    local backupd = {}

    if keyType == "set" then

        for i = 1, #values do
            local r = redis.call("SREM", key, values[i])
            removed = removed + r
            if r ~= 0 then
                table.insert(backupd, values[i])
            end

        end
    elseif keyType == "zset" then
        for i = 1, #values do
            local score = redis.call("ZSCORE", key, values[i])
            local r = redis.call("ZREM", key, values[i])
            removed = removed + r
            if r ~= 0 then
                table.insert(backupd, score)
                table.insert(backupd, values[i])
            end
        end

    elseif keyType == 'list' then
        for i = 1, #values do
            local r = redis.call("LREM", key, 0, values[i])
            removed = removed + r
            if r ~= 0 then
                table.insert(backupd, values[i])
            end
        end

    end
    if #backupd > 0 and backup_key and backup_key ~= "" then
        return backupItems(backup_key, backupd, keyType)
    else 
        return removed
    end
    

end
--将 key 中的 old 替换为 new
local function replaceItem(key, old, new, default_type)
    local ret = 0
    local keyType = getKeyType(key, default_type)
    local r = removeItems(key, { old }, keyType)
    if r ~= 0 then
        addItems(key, { new }, keyType)
        ret = ret + 1
    end
    return ret
end

            
            
            """

        self.base = base or ""

    def register(self, lua: str):
        return self.get_script(lua)

    def run(self, lua, keys=None, args=None):
        script = self.get_script(lua)
        return script(keys=keys, args=args)

    @functools.lru_cache(maxsize=None)
    def get_script(self, lua: str):
        lua = self.base + lua.strip()
        script = self.conn.register_script(lua)
        return script


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
        self.lua = LUA(self)

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

        return self.lua.run(
            lua="""
    changeDataBase(ARGV[1])
    for flag = 2, #ARGV
    do
        redis.call("setbit", KEYS[flag], ARGV[flag], KEYS[1])
    end
            
            """,
            keys=keys,
            args=args
        )

    def getbits(self, names, offsets, db_num=None):
        db_num = self.database if db_num is None else db_num
        keys = names
        args = [db_num, *offsets]
        lua = """
    changeDataBase(ARGV[1])
    local ret = {}
    for flag = 2, #ARGV
    do
        local r = redis.call("getbit", KEYS[flag - 1], ARGV[flag])
        table.insert(ret, r)
    end
    return ret
            """
        return self.lua.run(lua=lua, args=args, keys=keys)

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
changeDataBase(ARGV[1])
local ret = redis.call("SETNX",KEYS[1],KEYS[2])
if(ret == 1)
then
   redis.call("EXPIRE",KEYS[1],KEYS[3])
end
return ret

"""
        while True:
            ret = self.lua.run(lua=lua, args=args, keys=keys)
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
        args = [db_num, "none"]
        lua = """
changeDataBase(ARGV[1])
local ret = {}
for flag = 1, #KEYS do
    ret[flag] = getKeySize(KEYS[flag], ARGV[2])
end
return ret
"""
        ret = self.lua.run(lua=lua, args=args, keys=keys)
        return pandora.first(ret) if len(names) == 1 else ret

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
changeDataBase(KEYS[1])
return  redis.call("DEL", unpack(ARGV))
'''
        return self.lua.run(lua=lua, keys=keys, args=args)

    def add(self, name: Union[str, List[str]], *values, db_num=None, genre: Literal['set', 'zset', 'list'] = "set",
            maxsize=0):
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

        db_num = self.database if db_num is None else db_num
        keys = [db_num, genre or "set", maxsize, *pandora.iterable(name)]
        args = _to_str(*values)
        lua = f'''
    local db_num = KEYS[1]
    local default_type = KEYS[2]
    local maxsize = KEYS[3]
    local values = ARGV

    changeDataBase(db_num)
    local success = 0
    for i = 4, #KEYS do
        local key = KEYS[i]
        success = success + addItems(key, values, default_type, maxsize)
    end
    return success
'''
        return self.lua.run(lua=lua, keys=keys, args=args)

    def pop(self, name: Union[str, List[str]], count=1, db_num=None, backup=None, genre: Literal['set', 'zset', 'list'] = "set"):
        """
        从 `name` 中 pop 出 `count` 个值出来

        :param genre:
        :param backup: pop 的同时加入到 backup 队列中
        :param name: 队列名称
        :param count: 数量;
        :param db_num: 数据库编号
        :return:
        """
        if not name:
            return
        db_num = self.database if db_num is None else db_num
        keys = [db_num, count, genre, backup or '']
        args = [*pandora.iterable(name)]
        lua = '''
    local db_num = KEYS[1]
    local count = KEYS[2]
    local default_type = KEYS[3]
    local backup_key = KEYS[4]
    changeDataBase(db_num)
    local s = {}
    for i = 1, #ARGV do
        local t = popItems(ARGV[i], count, default_type, backup_key)  
        for j = 1, #t do
            table.insert(s, t[j])
        end 
    end
    return s
'''
        ret = self.lua.run(lua=lua, keys=keys, args=args)
        if ret:
            return ret[0] if len(ret) == 1 else ret
        return None

    def remove(self, name: Union[str, List[str]], *values, db_num=None, backup: str = "", genre: Literal['set', 'zset', 'list'] = "set"):
        """
        从 `name` 中删除 `values`

        :param genre:
        :param backup:
        :param name: 队列名称
        :param values: 需要删除的  values
        :param db_num: 数据库编号
        :return:
        """
        if not name:
            return
        db_num = self.database if db_num is None else db_num
        keys = [db_num, genre, backup, *pandora.iterable(name)]
        args = _to_str(*values)
        lua = '''
    local db_num = KEYS[1]
    local default_type = KEYS[2]
    local backup_key = KEYS[3]
    changeDataBase(db_num)
    local s = 0
    for i = 4, #KEYS do
        s = s + removeItems(KEYS[i], ARGV, default_type, backup_key)

    end
    return s

'''
        return self.lua.run(lua=lua, keys=keys, args=args)

    def replace(self, name: Union[str, List[str]], *values, db_num=None, genre: Literal['set', 'zset', 'list'] = "set"):
        """
        从 `name` 中 pop 出 `count` 个值出来

        :param genre:
        :param name: 队列名称
        :param db_num: 数据库编号
        :return:
        """
        if not name:
            return
        db_num = self.database if db_num is None else db_num
        keys = [db_num, genre, *pandora.iterable(name)]
        args = [j for i in values for j in _to_str(*i)]
        lua = '''
    local db_num = KEYS[1]
    local default_type = KEYS[2]
    changeDataBase(db_num)
    local ret = 0
    for i = 3, #KEYS do
        for j = 1, #ARGV, 2 do
            ret = ret + replaceItem(KEYS[i], ARGV[j], ARGV[j + 1], default_type)
        end
    end
    return ret

'''
        return self.lua.run(lua=lua, keys=keys, args=args)

    def merge(self, dest, *sources, db_num=None, genre: Literal['set', 'zset', 'list'] = "set"):
        """
        合并队列

        :param genre:
        :param dest: 目标队列
        :param sources: 源队列
        :param db_num: 数据库编号
        :return:
        """
        db_num = self.database if db_num is None else db_num
        keys = [db_num, genre, dest]
        args = sources
        lua = '''
    local db_num = KEYS[1]
    local default_type = KEYS[2]
    local dest = KEYS[3]
    local froms = ARGV
    changeDataBase(db_num)
    return mergeKeys(dest, froms, default_type)
'''
        return self.lua.run(lua=lua, keys=keys, args=args)

    def command(self, cmd, *args, db_num=None):
        """
        可以跨库执行命令

        :param cmd: 命令名称：sadd、spop 等
        :param args: 命令对应的参数
        :param db_num: 在哪个数据库中执行
        :return:
        """
        db_num = self.database if db_num is None else db_num
        keys = [db_num, cmd]
        args = [*args]
        lua = f"""
    local db_num = KEYS[1]
    local cmd = KEYS[2]
    changeDataBase(db_num)
    return redis.call(cmd, unpack(ARGV))
        """
        return self.lua.run(lua=lua, keys=keys, args=args)


if __name__ == '__main__':
    rds = Redis()
    for i in range(100):
        rds.add("test2", {"name": f"name-{i:02}"}, genre="zset")

    rds.merge("ttt", "test2", genre="zset")
