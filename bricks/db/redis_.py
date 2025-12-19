# -*- coding: utf-8 -*-
# @Time    : 2023-11-15 16:43
# @Author  : Kem
# @Desc    :
from __future__ import absolute_import

import copy
import csv
import datetime
import functools
import json
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Literal, Optional, Union

import redis
from loguru import logger

from bricks.db.sqlite import Sqlite
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
        if not isinstance(value, (bytes, str, int, float))
        else value
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

    def __init__(
            self, host="127.0.0.1", password=None, port=6379, database=0, **kwargs
    ):
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

        # 设置连接保活参数，防止长时间运行时连接被关闭
        kwargs.setdefault('socket_keepalive', True)
        kwargs.setdefault('socket_connect_timeout', 5)
        kwargs.setdefault('socket_timeout', 5)
        kwargs.setdefault('retry_on_timeout', True)
        kwargs.setdefault('health_check_interval', 30)

        super().__init__(
            host=host,
            password=password,
            port=port,
            db=database,
            decode_responses=True,
            **kwargs,
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
            args=args,
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

    def acquire_lock(
            self, name, timeout=3600 * 24, block=False, prefix="", value=None, db_num=None
    ):
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
        keys = [
            f"{prefix}{name}",
            value or datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            timeout,
        ]
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
                logger.debug(f"等待全局锁 {name} 释放")
                time.sleep(1)

    def release_lock(self, name, prefix=""):
        """
        释放redis全局锁

        :param prefix: 锁前缀
        :param name: 锁的名称

        :return:
        """

        prefix = prefix + ":Lock:" if prefix else ""
        name = f"{prefix}{name}"
        self.delete(name)

    def wait_lock(self, name, prefix=""):
        """
        等待锁释放

        :param name: 锁的名称
        :param prefix:前缀
        :return:
        """

        prefix = prefix + ":Lock:" if prefix else ""
        name = f"{prefix}{name}"

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
        lua = """
changeDataBase(KEYS[1])
return  redis.call("DEL", unpack(ARGV))
"""
        return self.lua.run(lua=lua, keys=keys, args=args)

    def add(
            self,
            name: Union[str, List[str]],
            *values,
            db_num=None,
            genre: Literal["set", "zset", "list"] = "set",
            maxsize=0,
    ):
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
        lua = """
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
"""
        return self.lua.run(lua=lua, keys=keys, args=args)

    def pop(
            self,
            name: Union[str, List[str]],
            count=1,
            db_num=None,
            backup=None,
            genre: Literal["set", "zset", "list"] = "set",
    ):
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
        keys = [db_num, count, genre, backup or ""]
        args = [*pandora.iterable(name)]
        lua = """
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
"""
        ret = self.lua.run(lua=lua, keys=keys, args=args)
        if ret:
            return ret[0] if len(ret) == 1 else ret
        return None

    def remove(
            self,
            name: Union[str, List[str]],
            *values,
            db_num=None,
            backup: str = "",
            genre: Literal["set", "zset", "list"] = "set",
    ):
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
        lua = """
    local db_num = KEYS[1]
    local default_type = KEYS[2]
    local backup_key = KEYS[3]
    changeDataBase(db_num)
    local s = 0
    for i = 4, #KEYS do
        s = s + removeItems(KEYS[i], ARGV, default_type, backup_key)

    end
    return s

"""
        return self.lua.run(lua=lua, keys=keys, args=args)

    def replace(
            self,
            name: Union[str, List[str]],
            *values,
            db_num=None,
            genre: Literal["set", "zset", "list"] = "set",
    ):
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
        lua = """
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

"""
        return self.lua.run(lua=lua, keys=keys, args=args)

    def merge(
            self, dest, *sources, db_num=None, genre: Literal["set", "zset", "list"] = "set"
    ):
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
        lua = """
    local db_num = KEYS[1]
    local default_type = KEYS[2]
    local dest = KEYS[3]
    local froms = ARGV
    changeDataBase(db_num)
    return mergeKeys(dest, froms, default_type)
"""
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
        lua = """
    local db_num = KEYS[1]
    local cmd = KEYS[2]
    changeDataBase(db_num)
    return redis.call(cmd, unpack(ARGV))
        """
        return self.lua.run(lua=lua, keys=keys, args=args)

    def _flatten_dict(
        self, data: Dict[str, Any], parent_key: str = "", sep: str = "_", max_depth: int = 1, current_depth: int = 0
    ) -> Dict[str, Any]:
        """
        展平嵌套的字典

        :param data: 要展平的字典
        :param parent_key: 父键名
        :param sep: 分隔符
        :param max_depth: 最大展平深度
        :param current_depth: 当前深度
        :return: 展平后的字典
        """
        items = []
        for k, v in data.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict) and current_depth < max_depth:
                items.extend(
                    self._flatten_dict(
                        v, new_key, sep=sep, max_depth=max_depth, current_depth=current_depth + 1).items()
                )
            elif isinstance(v, (list, tuple)) and current_depth < max_depth:
                # 将列表转换为索引字典
                for i, item in enumerate(v):
                    if isinstance(item, dict):
                        items.extend(
                            self._flatten_dict(
                                item, f"{new_key}{sep}{i}", sep=sep, max_depth=max_depth, current_depth=current_depth + 1
                            ).items()
                        )
                    else:
                        items.append((f"{new_key}{sep}{i}", item))
            else:
                items.append((new_key, v))
        return dict(items)

    def _iter_redis_data(
        self,
        name: str,
        key_type: str,
        chunk_size: int = 1000,
        db_num: Optional[int] = None,
        include_score: bool = True,
        max_retries: int = 3,
    ):
        """
        批量迭代 Redis 数据的生成器

        :param name: Redis key 名称
        :param key_type: Redis 数据类型
        :param chunk_size: 每次读取的数据量
        :param db_num: 数据库编号
        :param include_score: 对于 zset，是否包含 score
        :param max_retries: 连接失败时的最大重试次数
        :yield: 批量数据列表
        """
        db_num = self.database if db_num is None else db_num

        # 切换数据库
        if db_num != self.database:
            self.select(db_num)

        def _retry_operation(operation, *args, **kwargs):
            """带重试的操作执行"""
            for attempt in range(max_retries):
                try:
                    return operation(*args, **kwargs)
                except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError) as e:
                    if attempt < max_retries - 1:
                        logger.warning(
                            f"Redis connection error, retrying ({attempt + 1}/{max_retries})...")
                        time.sleep(1)  # 等待1秒后重试
                        try:
                            self.ping()  # 尝试重新连接
                        except:
                            pass
                    else:
                        raise

        try:
            if key_type == "list":
                # 使用 LRANGE 分批读取
                start = 0
                while True:
                    items = _retry_operation(
                        self.lrange, name, start, start + chunk_size - 1)
                    if not items:
                        break
                    yield items
                    start += chunk_size

            elif key_type == "set":
                # 使用 SSCAN 迭代
                cursor = 0
                while True:
                    cursor, items = _retry_operation(
                        self.sscan, name, cursor, count=chunk_size)
                    if items:
                        yield list(items)
                    if cursor == 0:
                        break

            elif key_type == "zset":
                # 使用 ZSCAN 迭代
                cursor = 0
                while True:
                    cursor, items = _retry_operation(
                        self.zscan, name, cursor, count=chunk_size)
                    if items:
                        batch = []
                        for member, score in items.items():
                            if include_score:
                                batch.append({"value": member, "score": score})
                            else:
                                batch.append(member)
                        yield batch
                    if cursor == 0:
                        break

            elif key_type == "hash":
                # 使用 HSCAN 迭代
                cursor = 0
                while True:
                    cursor, items = _retry_operation(
                        self.hscan, name, cursor, count=chunk_size)
                    if items:
                        batch = [{"field": field, "value": value}
                                 for field, value in items.items()]
                        yield batch
                    if cursor == 0:
                        break
        finally:
            # 恢复原数据库
            if db_num != self.database:
                self.select(self.database)

    def export_to_csv(
        self,
        name: str,
        output_path: Union[str, Path],
        *,
        db_num: Optional[int] = None,
        chunk_size: int = 1000,
        headers: Optional[List[str]] = None,
        auto_detect_headers: bool = True,
        delimiter: str = ",",
        encoding: str = "utf-8-sig",
        quote_all: bool = False,
        include_score: bool = True,
        flatten: bool = False,
        max_depth: int = 1,
        progress_callback: Optional[Callable[[int, int], None]] = None,
        write_mode: Literal["w", "a"] = "w",
        skip_errors: bool = True,
        default_value: str = "",
        show_progress: bool = True,
    ) -> Dict[str, Any]:
        """
        将 Redis key 中的数据导出为 CSV/TSV 文件

        支持流式导出，避免内存占用过高

        Examples:
            >>> redis = Redis()
            >>> # 基本用法
            >>> redis.export_to_csv("my_set", "output.csv")

            >>> # 导出为 TSV
            >>> redis.export_to_csv("my_list", "output.tsv", delimiter="\\t")

            >>> # 自定义表头和进度回调
            >>> def progress(current, total):
            ...     print(f"Progress: {current}/{total}")
            >>> redis.export_to_csv(
            ...     "my_zset",
            ...     "output.csv",
            ...     headers=["name", "age", "score"],
            ...     progress_callback=progress,
            ...     include_score=True
            ... )

            >>> # 不展平嵌套 JSON
            >>> redis.export_to_csv("my_data", "output.csv", flatten=False)

        :param name: Redis key 名称
        :param output_path: 输出文件路径
        :param db_num: 数据库编号
        :param chunk_size: 每次读取和写入的批量大小，默认 1000
        :param headers: CSV 表头，None 则自动检测
        :param auto_detect_headers: 是否自动检测表头（从第一批数据中提取键名）
        :param delimiter: 分隔符，默认逗号（TSV 使用 \\t）
        :param encoding: 文件编码，默认 utf-8-sig（带 BOM，Excel 友好）
        :param quote_all: 是否对所有字段加引号
        :param include_score: 对于 zset，是否包含 score 列
        :param flatten: 是否展平嵌套的 JSON
        :param max_depth: 展平的最大深度
        :param progress_callback: 进度回调函数 callback(current, total)
        :param write_mode: 写入模式，'w' 覆盖，'a' 追加
        :param skip_errors: 是否跳过解析错误的数据
        :param default_value: 缺失字段的默认值
        :param show_progress: 是否显示进度条，默认 True
        :return: 导出统计信息字典
        """
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        db_num = self.database if db_num is None else db_num

        # 切换数据库获取 key 类型和总数
        if db_num != self.database:
            self.select(db_num)

        try:
            # 检查 key 是否存在
            if not self.exists(name):
                logger.warning(
                    f"Key '{name}' does not exist in database {db_num}")
                return {
                    "success": False,
                    "exported_count": 0,
                    "error_count": 0,
                    "total_count": 0,
                    "key_type": None,
                    "output_path": str(output_path),
                }

            # 获取 key 类型和总数
            key_type = self.type(name)
            total_count = self.count(name, db_num=db_num)

            if key_type not in ["list", "set", "zset", "hash"]:
                raise ValueError(
                    f"Unsupported key type: {key_type}. Only list, set, zset, hash are supported.")

            logger.info(
                f"Exporting {name} ({key_type}, {total_count} items) to {output_path}")

        finally:
            # 恢复原数据库
            if db_num != self.database:
                self.select(self.database)

        # 统计信息
        stats = {
            "exported_count": 0,
            "error_count": 0,
            "total_count": total_count,
            "key_type": key_type,
            "output_path": str(output_path),
        }

        # CSV 写入配置
        quoting = csv.QUOTE_ALL if quote_all else csv.QUOTE_MINIMAL

        # 检查文件是否需要写入表头（追加模式下检查文件大小）
        should_write_header = write_mode == "w" or (write_mode == "a" and (
            not output_path.exists() or output_path.stat().st_size == 0))

        # 尝试导入 tqdm
        pbar = None
        if show_progress:
            try:
                pandora.require("tqdm")
                from tqdm import tqdm
                pbar = tqdm(total=total_count, desc=f"导出 {name}", unit="条")
            except ImportError:
                logger.warning(
                    "tqdm not installed, progress bar disabled. Install with: pip install tqdm")
                show_progress = False

        try:
            # 打开文件准备写入
            with open(output_path, write_mode, encoding=encoding, newline="") as f:
                writer = None
                detected_headers = None

                # 批量迭代数据
                for batch_items in self._iter_redis_data(name, key_type, chunk_size, db_num, include_score):
                    batch_rows = []

                    # 处理批量数据
                    for raw_item in batch_items:
                        try:
                            # 解析 JSON
                            if isinstance(raw_item, str):
                                try:
                                    item = json.loads(raw_item)
                                except json.JSONDecodeError:
                                    if skip_errors:
                                        logger.warning(
                                            f"Failed to parse JSON: {raw_item[:100]}")
                                        stats["error_count"] += 1
                                        continue
                                    else:
                                        raise
                            else:
                                item = raw_item

                            # 确保是字典
                            if not isinstance(item, dict):
                                item = {"value": item}

                            # 展平嵌套结构
                            if flatten:
                                item = self._flatten_dict(
                                    item, max_depth=max_depth)

                            # 初始化 writer（第一次写入时）
                            if writer is None:
                                # 确定表头
                                if headers:
                                    detected_headers = headers
                                elif auto_detect_headers:
                                    detected_headers = list(item.keys())
                                else:
                                    detected_headers = []

                                # 创建 CSV writer
                                writer = csv.DictWriter(
                                    f,
                                    fieldnames=detected_headers,
                                    delimiter=delimiter,
                                    quoting=quoting,
                                    extrasaction="ignore",
                                )

                                # 写入表头
                                if detected_headers and should_write_header:
                                    writer.writeheader()

                            # 补充缺失的字段
                            row = {header: item.get(
                                header, default_value) for header in detected_headers}
                            batch_rows.append(row)
                            stats["exported_count"] += 1

                        except Exception as e:
                            if skip_errors:
                                logger.warning(f"Error processing item: {e}")
                                stats["error_count"] += 1
                            else:
                                raise

                    # 批量写入这一批数据
                    if batch_rows:
                        writer.writerows(batch_rows)

                        # 更新进度条
                        if pbar:
                            pbar.update(len(batch_rows))

                        # 进度回调
                        if progress_callback:
                            progress_callback(
                                stats["exported_count"], total_count)

            # 最后一次进度回调
            if progress_callback:
                progress_callback(stats["exported_count"], total_count)

        finally:
            # 关闭进度条
            if pbar:
                pbar.close()

        stats["success"] = True
        logger.info(
            f"Export completed: {stats['exported_count']} records exported, "
            f"{stats['error_count']} errors, output: {output_path}"
        )

        return stats

    def export_to_sqlite(
        self,
        name: str,
        database: str,
        table: str,
        *,
        db_num: Optional[int] = None,
        chunk_size: int = 1000,
        headers: Optional[List[str]] = None,
        auto_detect_headers: bool = True,
        include_score: bool = True,
        flatten: bool = True,
        max_depth: int = 1,
        progress_callback: Optional[Callable[[int, int], None]] = None,
        skip_errors: bool = True,
        default_value: Any = None,
        show_progress: bool = True,
        primary_keys: Optional[List[str]] = None,
        auto_create_table: bool = True,
        table_structure: Optional[Dict[str, type]] = None,
    ) -> Dict[str, Any]:
        """
        将 Redis key 中的数据导出到 SQLite 数据库

        支持流式导出、自动建表、主键去重

        Examples:
            >>> redis = Redis()
            >>> # 基本用法
            >>> redis.export_to_sqlite("my_set", "output.db", "my_table")

            >>> # 指定主键去重
            >>> redis.export_to_sqlite(
            ...     "my_zset",
            ...     "output.db",
            ...     "my_table",
            ...     primary_keys=["id"],
            ...     include_score=True
            ... )

            >>> # 自定义表结构
            >>> redis.export_to_sqlite(
            ...     "my_data",
            ...     "output.db",
            ...     "my_table",
            ...     table_structure={"id": int, "name": str, "age": int}
            ... )

        :param name: Redis key 名称
        :param database: SQLite 数据库文件路径
        :param table: 表名
        :param db_num: Redis 数据库编号
        :param chunk_size: 每次读取和写入的批量大小，默认 1000
        :param headers: 字段列表，None 则自动检测
        :param auto_detect_headers: 是否自动检测字段（从第一批数据中提取键名）
        :param include_score: 对于 zset，是否包含 score 列
        :param flatten: 是否展平嵌套的 JSON
        :param max_depth: 展平的最大深度
        :param progress_callback: 进度回调函数 callback(current, total)
        :param skip_errors: 是否跳过解析错误的数据
        :param default_value: 缺失字段的默认值
        :param show_progress: 是否显示进度条，默认 True
        :param primary_keys: 主键列表，用于去重（ON CONFLICT DO UPDATE）
        :param auto_create_table: 是否自动创建表，默认 True
        :param table_structure: 表结构定义 {"field": type}，None 则自动推断
        :return: 导出统计信息字典
        """
        db_num = self.database if db_num is None else db_num

        # 切换数据库获取 key 类型和总数
        if db_num != self.database:
            self.select(db_num)

        try:
            # 检查 key 是否存在
            if not self.exists(name):
                logger.warning(
                    f"Key '{name}' does not exist in database {db_num}")
                return {
                    "success": False,
                    "exported_count": 0,
                    "error_count": 0,
                    "total_count": 0,
                    "key_type": None,
                    "database": database,
                    "table": table,
                }

            # 获取 key 类型和总数
            key_type = self.type(name)
            total_count = self.count(name, db_num=db_num)

            if key_type not in ["list", "set", "zset", "hash"]:
                raise ValueError(
                    f"Unsupported key type: {key_type}. Only list, set, zset, hash are supported.")

            logger.info(
                f"Exporting {name} ({key_type}, {total_count} items) to SQLite {database}.{table}")

        finally:
            # 恢复原数据库
            if db_num != self.database:
                self.select(self.database)

        # 统计信息
        stats = {
            "exported_count": 0,
            "error_count": 0,
            "total_count": total_count,
            "key_type": key_type,
            "database": database,
            "table": table,
        }

        # 尝试导入 tqdm
        pbar = None
        if show_progress:
            try:
                pandora.require("tqdm")
                from tqdm import tqdm
                pbar = tqdm(total=total_count, desc=f"导出 {name}", unit="条")
            except ImportError:
                logger.warning(
                    "tqdm not installed, progress bar disabled. Install with: pip install tqdm")
                show_progress = False

        # 连接 SQLite 数据库
        sqlite = Sqlite(database=database)
        detected_headers = None
        table_created = False

        try:
            # 批量迭代数据
            for batch_items in self._iter_redis_data(name, key_type, chunk_size, db_num, include_score):
                batch_rows = []

                # 处理批量数据
                for raw_item in batch_items:
                    try:
                        # 解析 JSON
                        if isinstance(raw_item, str):
                            try:
                                item = json.loads(raw_item)
                            except json.JSONDecodeError:
                                if skip_errors:
                                    logger.warning(
                                        f"Failed to parse JSON: {raw_item[:100]}")
                                    stats["error_count"] += 1
                                    continue
                                else:
                                    raise
                        else:
                            item = raw_item

                        # 确保是字典
                        if not isinstance(item, dict):
                            item = {"value": item}

                        # 展平嵌套结构
                        if flatten:
                            item = self._flatten_dict(
                                item, max_depth=max_depth)

                        # 初始化表结构（第一次写入时）
                        if detected_headers is None:
                            # 确定字段
                            if headers:
                                detected_headers = headers
                            elif auto_detect_headers:
                                detected_headers = list(item.keys())
                            else:
                                detected_headers = []

                            # 自动创建表
                            if auto_create_table and not table_created:
                                if table_structure:
                                    structure = table_structure
                                else:
                                    # 自动推断字段类型
                                    structure = {}
                                    for key in detected_headers:
                                        value = item.get(key, default_value)
                                        if value is None:
                                            structure[key] = None
                                        else:
                                            structure[key] = type(value)

                                # 创建表（支持主键约束）
                                sqlite.create_table(
                                    table, structure=structure, primary_keys=primary_keys)
                                table_created = True

                        # 补充缺失的字段
                        row = {header: item.get(
                            header, default_value) for header in detected_headers}
                        batch_rows.append(row)
                        stats["exported_count"] += 1

                    except Exception as e:
                        if skip_errors:
                            logger.warning(f"Error processing item: {e}")
                            stats["error_count"] += 1
                        else:
                            raise

                # 批量写入这一批数据
                if batch_rows:
                    try:
                        if primary_keys:
                            # 带主键去重的插入
                            sqlite.insert(table, *batch_rows,
                                          row_keys=primary_keys)
                        else:
                            # 普通插入
                            sqlite.insert(table, *batch_rows)

                        # 更新进度条
                        if pbar:
                            pbar.update(len(batch_rows))

                        # 进度回调
                        if progress_callback:
                            progress_callback(
                                stats["exported_count"], total_count)

                    except Exception as e:
                        if skip_errors:
                            logger.warning(f"Error inserting batch: {e}")
                            stats["error_count"] += len(batch_rows)
                        else:
                            raise

            # 最后一次进度回调
            if progress_callback:
                progress_callback(stats["exported_count"], total_count)

        finally:
            # 关闭进度条和数据库连接
            if pbar:
                pbar.close()
            sqlite.close()

        stats["success"] = True
        logger.info(
            f"Export completed: {stats['exported_count']} records exported to {database}.{table}, "
            f"{stats['error_count']} errors"
        )

        return stats


if __name__ == "__main__":
    rds = Redis()
    for i in range(100):
        rds.add("test2", {"name": f"name-{i:02}"}, genre="zset")

    rds.merge("ttt", "test2", genre="zset")
