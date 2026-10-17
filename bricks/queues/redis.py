"""以 Redis Lua 原子操作实现可跨进程共享的种子任务存储。"""

from __future__ import annotations

import hashlib
import json
from typing import Any

from .base import _Queue
from .models import encode, text

_SCRIPT = r"""
-- 每个队列固定使用同一 hash tag 的四个键；业务 JSON 保持原始字符串。
local records, ready, active, meta = KEYS[1], KEYS[2], KEYS[3], KEYS[4]
local op, a = ARGV[1], cjson.decode(ARGV[2])
local tm = redis.call('TIME')
local now = tonumber(tm[1]) + tonumber(tm[2]) / 1000000
local null = cjson.null

-- 读取记录，不向 Lua 解码用户业务数据。
local function read(key, field)
    local value = redis.call('HGET', key, field)
    if value then return cjson.decode(value) end
    return nil
end

-- 将原子操作结果统一编码。
local function out(value)
    if value == nil then return 'null' end
    return cjson.encode(value)
end

-- 凭据过期后即失去写入权，即使尚未被其他 Worker 领取。
local function valid(r, c, initial)
    return r and r.status == (initial and 'running' or 'leased')
        and tonumber(r.deadline) > now and r.token == c.token and r.owner == c.owner
        and (not initial or r.fence == c.fence)
end

-- 保存任务和索引；计数仅保存终态，活动态直接读取有序集合。
local function save(id, r)
    redis.call('HSET', records, id, cjson.encode(r))
end

local r = a.id and read(records, a.id) or nil
if op == 'put' or op == 'put_claim' then
    if op == 'put' and a.init ~= null and not valid(read(meta, 'init'), a.init, true) then
        return out({failure='lease'})
    end
    if r then
        if r.body ~= a.body then return out({failure='conflict'}) end
        if op == 'put' then return out(false) end
        if r.status == 'leased' and tonumber(r.deadline) <= now then
            redis.call('ZREM', active, a.id)
            r.token, r.owner, r.deadline = '', '', '0'
            r.error, r.ready = 'delivery lease expired', string.format('%.17g', now)
            if r.attempt >= r.maximum then
                r.status = 'dead'
                redis.call('HINCRBY', meta, 'dead', 1)
            else
                r.status = 'pending'
                redis.call('ZADD', ready, now, a.id)
            end
            save(a.id, r)
        end
        if r.status ~= 'pending' or tonumber(r.ready) > now then return out(nil) end
    else
        r = {body=a.body, status='pending', attempt=0, maximum=a.maximum,
             ready=string.format('%.17g', now), deadline='0', token='', owner='', error='', result=null}
        save(a.id, r)
        redis.call('ZADD', ready, now, a.id)
        if op == 'put' then return out(true) end
    end
    r.status, r.attempt, r.token, r.owner = 'leased', r.attempt + 1, a.token, a.owner
    r.deadline = string.format('%.17g', now + a.ttl)
    save(a.id, r)
    redis.call('ZREM', ready, a.id)
    redis.call('ZADD', active, tonumber(r.deadline), a.id)
    r._id = a.id
    return out(r)
end
if op == 'claim' then
    local expired = redis.call('ZRANGEBYSCORE', active, '-inf', now, 'LIMIT', 0, 100)
    for _, id in ipairs(expired) do
        local old = read(records, id)
        old.token, old.owner, old.deadline = '', '', '0'
        old.error, old.ready = 'delivery lease expired', string.format('%.17g', now)
        redis.call('ZREM', active, id)
        if old.attempt >= old.maximum then
            old.status = 'dead'
            redis.call('HINCRBY', meta, 'dead', 1)
        else
            old.status = 'pending'
            redis.call('ZADD', ready, now, id)
        end
        save(id, old)
    end
    local ids = redis.call('ZRANGEBYSCORE', ready, '-inf', now, 'LIMIT', 0, 1)
    if #ids == 0 then return out(nil) end
    local id = ids[1]
    r = read(records, id)
    r.status, r.attempt, r.token, r.owner = 'leased', r.attempt + 1, a.token, a.owner
    r.deadline = string.format('%.17g', now + a.ttl)
    save(id, r)
    redis.call('ZREM', ready, id)
    redis.call('ZADD', active, tonumber(r.deadline), id)
    r._id = id
    return out(r)
end
if op == 'get' then return out(r) end
if op == 'counts' then
    return out({pending=redis.call('ZCARD', ready), leased=redis.call('ZCARD', active),
        acked=tonumber(redis.call('HGET', meta, 'acked') or '0'),
        dead=tonumber(redis.call('HGET', meta, 'dead') or '0')})
end
if op == 'forget' then
    if not r then return out(false) end
    if r.status ~= 'acked' and r.status ~= 'dead' then
        return out({failure='only terminal tasks can be forgotten'})
    end
    redis.call('HINCRBY', meta, r.status, -1)
    redis.call('HDEL', records, a.id)
    return out(true)
end
if op == 'renew' or op == 'ack' or op == 'retry' or op == 'reject' then
    if not valid(r, a, false) then return out({failure='lease'}) end
    if op == 'renew' then
        r.deadline = string.format('%.17g', now + a.ttl)
        redis.call('ZADD', active, tonumber(r.deadline), a.id)
        save(a.id, r)
        return out(r)
    end
    redis.call('ZREM', active, a.id)
    if op == 'ack' then
        r.status, r.result = 'acked', a.result
    elseif op == 'retry' and r.attempt < r.maximum then
        r.status, r.ready, r.error = 'pending', string.format('%.17g', now + a.delay), a.error
        redis.call('ZADD', ready, tonumber(r.ready), a.id)
    else
        r.status, r.error = 'dead', a.error
    end
    if r.status ~= 'pending' then redis.call('HINCRBY', meta, r.status, 1) end
    r.token, r.owner, r.deadline = '', '', '0'
    save(a.id, r)
    return out(r.status)
end
local initial = read(meta, 'init')
if op == 'initialization' then return out(initial) end
if op == 'acquire_init' then
    if initial and (initial.status == 'succeeded' or
        (initial.status == 'running' and tonumber(initial.deadline) > now)) then return out(nil) end
    initial = {owner=a.owner, token=a.token, deadline=string.format('%.17g', now + a.ttl),
        fence=initial and initial.fence + 1 or 1,
        progress=initial and initial.progress or 'null', status='running', error=''}
    redis.call('HSET', meta, 'init', cjson.encode(initial))
    return out(initial)
end
if op == 'renew_init' or op == 'save_init' or op == 'finish_init' then
    if not valid(initial, a, true) then return out({failure='lease'}) end
    if op == 'renew_init' then initial.deadline = string.format('%.17g', now + a.ttl)
    elseif op == 'save_init' then initial.progress = a.progress
    else initial.status, initial.error = a.success and 'succeeded' or 'failed', a.error end
    redis.call('HSET', meta, 'init', cjson.encode(initial))
    if op == 'renew_init' then return out(initial) end
    return out(nil)
end
return redis.error_reply('unknown queue operation')
"""


class RedisTaskQueue(_Queue):
    """借用同步 Redis 客户端，提供至少一次交付的任务存储。

    Redis 数据不自动过期；终态保留去重身份，需显式 forget。持久性取决于
    Redis 的 AOF、复制及故障切换配置，不保证外部副作用恰好执行一次。

    Attributes:
        _keys: 同一队列的任务、就绪、租约与初始化元数据键。
        _script: 借用客户端注册的 Lua 调用器，不关闭客户端或删除数据。
    """

    def __init__(
        self,
        client: Any,
        name: str,
        *,
        prefix: str = "bricks:tasks",
        max_attempts: int = 3,
    ) -> None:
        """装配调用方拥有的同步 redis-py 客户端。

        Args:
            client: 已配置连接与操作超时的同步 Redis 客户端。
            name: 队列名，不同进程使用相同前缀与名称共享数据。
            prefix: 应用数据前缀，默认 bricks:tasks。
            max_attempts: 新任务的最大交付次数，默认 3，随任务保存。

        Raises:
            TypeError: 客户端不是同步 Redis 实例。
            ValueError: 配置非法。
        """
        from redis import Redis

        super().__init__(name, max_attempts)
        if not isinstance(client, Redis):
            raise TypeError("client must be a synchronous redis.Redis")
        prefix = text(prefix, "prefix")
        if "{" in prefix or "}" in prefix:
            raise ValueError("prefix must not contain Redis hash tag braces")
        tag = hashlib.sha256(name.encode()).hexdigest()
        self._keys = [
            f"{prefix}:{{{tag}}}:{part}"
            for part in ("records", "ready", "active", "meta")
        ]
        self._script = client.register_script(_SCRIPT)

    def _call(self, operation: str, **args: Any) -> Any:
        """在 Redis 内一次执行校验与迁移，网络错误原样传播。

        Args:
            operation: 操作名。
            args: 已校验参数，业务 JSON 作为不透明字符串传递。

        Returns:
            独立解码的操作结果。
        """
        return json.loads(self._script(keys=self._keys, args=[operation, encode(args)]))
