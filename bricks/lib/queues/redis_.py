# -*- coding: utf-8 -*-
# @Time    : 2023-12-11 13:14
# @Author  : Kem
# @Desc    :
import datetime
import json
import time

from loguru import logger

from bricks import state
from bricks.db.redis_ import Redis
from bricks.lib.queues import TaskQueue
from bricks.utils import pandora


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
        local is_replace = redis.call("srem", KEYS[index], unpack(need_remove))
        
        if (is_replace == 1) then
            -- 添加
            local r = redis.call("sadd", KEYS[index], unpack(need_add))
            ret = ret + r
        end


    elseif key_type == "zset" then
        -- 删除
        local is_replace = redis.call("zrem", KEYS[index], unpack(need_remove))
        if (is_replace == 1) then
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
        
        end
        


    elseif key_type == "list" then
        -- 删除
        local is_replace = redis.call("lrem", KEYS[index], 0, unpack(need_remove))
        if is_replace then 
            -- 添加
            local r = redis.call("lpush", KEYS[index], unpack(need_add))
            ret = ret + r 
        
        end
        


    else
        -- 删除
        local is_replace = redis.call("srem", KEYS[index], unpack(need_remove))
        
        if is_replace then
             -- 添加
            local r = redis.call("sadd", KEYS[index], unpack(need_add))
            ret = ret + r       
        end

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
    if success and KEYS[3] ~= '' then
        local v = redis.call("zrevrangebyscore", KEYS[2], "+inf", "-inf", "LIMIT", 0, 1, "withscores")[2]
        if v == nil then v = 0 end
        local values = {}

        for i = 1, #ARGV, 1 do
            table.insert(values, v + i+1)
            table.insert(values, ARGV[i])
        end

        redis.call("zadd", KEYS[3], unpack(values))

    end

    return success

else
    local success = redis.call("srem", KEYS[2], unpack(ARGV))
    if success and KEYS[3] ~= '' then
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
            local is_record_key_exists = redis.call("EXISTS", record_key)
            local status = '0'
            local identifier = ''
            if is_record_key_exists == 1 then
                status = redis.call("HGET", record_key, "status")
                identifier = redis.call("HGET", record_key, "identifier")
            end
            
            
            if status == '1' then
                -- 没投完
                
                if not identifier or identifier == machine_id then
                    -- 不存在 identifier / identifier == machine_id -> 继续投放
                    redis.call("HSET", record_key, "identifier", machine_id)
                    return true
                else
                    -- 存在 identifier / identifier != machine_id
                    return "非初始化机器, 初始化机器 ID 为" .. identifier
                end
            else
                -- 投完了
                
                if queue_size > 0 then
                    -- 没消费完
                    return "存在种子没有消费完毕"
                else
                    -- 消费完了 -> 重新投放
                    redis.call("DEL", record_key)
                    redis.call("HSET", record_key, "identifier", machine_id)
                    return true
                end           
            end
            """),
            "release_init": self.redis_db.register_script("""
            redis.replicate_commands()
            local record_key = KEYS[1]
            local history_key = KEYS[2]
            local machine_id = ARGV[1]
            local ttl = ARGV[2]
            local tt = ARGV[3]
            local identifier = redis.call("HGET", record_key, "identifier")
            if machine_id == identifier then
                redis.call("HSET", record_key, "status", 0)
                redis.call("HDEL", record_key, "identifier")
                redis.call("HDEL", record_key, "time")
                if ttl ~= 0 then
                    redis.call("DEL", history_key)    
                    local dump = redis.call('DUMP', record_key)
                    redis.call('RESTORE', history_key, ttl, dump)
                end
                return true

            end
            return false



            """)
        }

    def get(self, name, count: int = 1, **kwargs):
        """
        从 `name` 中获取种子

        :param count:
        :param name:
        :param kwargs:
        :return:
        """

        pop_key = self.name2key(name, 'current')
        add_key = self.name2key(name, 'temp')
        action = kwargs.pop('action', 'zpopmin')
        db_num = kwargs.get('db_num', self.database)
        keys = [pop_key, add_key]
        args = [db_num, count, action, self.genre]
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
        values = self.py2str(*values)

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
        names = [
            self.name2key(name, qtype) for qtype in
            pandora.iterable(kwargs.pop('qtypes', ["current", "temp", "failure"]))
        ]

        db_num = self.database if db_num is None else db_num
        keys = names
        args = [db_num, *[j for i in zip(pandora.iterable(old), pandora.iterable(new)) for j in self.py2str(*i)]]
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
        args = self.py2str(*values)
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
        self.publish(
            chanel=f'{name}-subscribe',
            msg={
                "action": "collect-status",
                "key": self.name2key(name, "report")
            },
            timeout=timeout
        )

        db_num = kwargs.pop('db_num', self.database)
        keys = [self.name2key(name, i) for i in ('current', 'temp', 'failure', 'status', 'report')]
        args = [
            db_num or self.database,
            str(datetime.datetime.now())
        ]

        ret = self.scripts["smart_reverse"](keys=keys, args=args)
        return ret == 1

    def publish(self, chanel: str, msg: dict, timeout=0):
        # 告诉其他机器开始上报状态
        self.redis_db.publish(chanel, json.dumps(msg))
        # 等待回复
        timeout and time.sleep(timeout)

    def is_empty(self, name, threshold=0, **kwargs):
        """
        判断 `name` 是否为空

        :param threshold:
        :param name:
        :return:
        """
        db_num = kwargs.pop('db_num', self.database)
        return self.size(name, db_num=db_num) <= threshold

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

        def run_subscribe(chanel, target):
            """
            订阅消息
            """

            def main(message):
                msg: dict = json.loads(message['data'])
                _action = msg['action']
                recv: str = msg.get('recv', state.MACHINE_ID)
                if recv != state.MACHINE_ID:
                    return

                    # 上报状态
                if _action == "collect-status":
                    key = msg["key"]
                    self.redis_db.hset(key, mapping={state.MACHINE_ID: target.dispatcher.running})
                    self.redis_db.expire(key, 5)

            pubsub = self.redis_db.pubsub()
            pubsub.subscribe(**{chanel: main})
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
            args = [state.MACHINE_ID]
            ret = self.scripts["get_permission"](keys=keys, args=args)
            return {
                "state": ret == 1,
                "msg": ret
            }

        def set_record():
            self.redis_db.hset(
                self.name2key(name, 'record'),
                mapping=json.loads(json.dumps(order['record'], default=str))
            )

        def wait_init():
            key = self.name2key(name, 'record')
            t1 = order.get('time')
            while True:
                if not self.is_empty(name):
                    return

                t2 = self.redis_db.hget(key, "time")
                if self.redis_db.exists(key) and (t2 and float(t2) >= t1):
                    return

                logger.debug('等待初始化开始')
                time.sleep(1)

        def set_init():
            key = self.name2key(name, "record")
            if self.redis_db.hget(key, 'identifier') == state.MACHINE_ID:
                self.redis_db.hset(key, mapping={
                    "identifier": state.MACHINE_ID,
                    "time": int(time.time() * 1000),
                    "status": 1
                })

        def is_init():
            key = self.name2key(name, 'record')
            # 队列不为空 -> true
            if not self.is_empty(name):
                return True

            # record 存在, 并且 status 为 1 -> true
            if self.redis_db.exists(key) and self.redis_db.hget(key, 'status') == "1":
                return True

            return False

        def release_init():
            key = self.name2key(name, "record")
            history = self.name2key(name, 'history')
            ttl = order.get('ttl') or 0
            t = order.get('time')

            ret = self.scripts["release_init"](
                keys=[
                    key, history
                ],
                args=[
                    state.MACHINE_ID,
                    ttl,
                    t
                ]
            )
            return bool(ret)

        def get_record():
            key = self.name2key(name, 'record')
            record = self.redis_db.hgetall(key) or {}
            if record.get('status') == "0":
                self.redis_db.delete(key)
                return {}
            else:
                return record

        actions = {
            self.COMMANDS.RUN_SUBSCRIBE: lambda: run_subscribe(f'{name}-subscribe', order['target']),
            self.COMMANDS.GET_PERMISSION: get_permission,

            self.COMMANDS.GET_RECORD: get_record,
            self.COMMANDS.CONTINUE_RECORD: lambda: self.reverse(name),
            self.COMMANDS.SET_RECORD: set_record,

            self.COMMANDS.WAIT_INIT: wait_init,
            self.COMMANDS.RESET_INIT: lambda: self.clear(name),
            self.COMMANDS.RELEASE_INIT: release_init,
            self.COMMANDS.IS_INIT: is_init,
            self.COMMANDS.SET_INIT: set_init,
        }
        action = order['action']
        if action in actions:
            return actions[action]()

    def __str__(self):
        return f'<RedisQueue [ HOST: {self.host} | DB: {self.database} ]>'
