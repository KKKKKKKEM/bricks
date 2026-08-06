# -*- coding: utf-8 -*-
# @Time    : 2023-12-11 13:14
# @Author  : Kem
# @Desc    :
import datetime
import json
import threading
import time
from typing import Literal

from loguru import logger

from bricks import state
from bricks.db.redis_ import LUA, Redis
from bricks.lib.queues import TaskQueue
from bricks.utils import pandora


class RedisQueue(TaskQueue):
    subscribe = True

    def __init__(
        self,
        host="127.0.0.1",
        password=None,
        port=6379,
        database=0,
        genre: Literal["list", "set", "zset"] = "set",
        **kwargs,
    ):
        self.redis_db = Redis(
            host=host, password=password, port=port, database=database, **kwargs
        )
        self.host = host
        self.database = database
        self.genre = genre
        self.lua = LUA(self.redis_db)

        class Scripts:
            get = self.lua.register("""
    local db_num = KEYS[1]
    local key = KEYS[2]
    local count = KEYS[3]
    local default_type = KEYS[4]
    local backup_key = KEYS[5]
    changeDataBase(db_num)
    return popItems(key, count, default_type, backup_key)   
                    """)
            put = self.lua.register("""
    local db_num = KEYS[1]
    local default_type = KEYS[2]
    local values = ARGV

    changeDataBase(db_num)
    local success = 0
    for i = 3, #KEYS do
        local key = KEYS[i]
        success = success + addItems(key, values, default_type)
    end
    return success
                    """)
            put_init = self.lua.register("""
    local db_num = KEYS[1]
    local default_type = KEYS[2]
    local heartbeat_key = KEYS[3]
    local lease_token = ARGV[1]
    local values = {}

    changeDataBase(db_num)
    if redis.call("GET", heartbeat_key) ~= lease_token then
        return 0
    end

    for i = 2, #ARGV do
        table.insert(values, ARGV[i])
    end

    local success = 0
    for i = 4, #KEYS do
        success = success + addItems(KEYS[i], values, default_type)
    end
    return success
                    """)
            replace = self.lua.register("""
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
                    """)
            remove = self.lua.register("""
    local db_num = KEYS[1]
    local default_type = KEYS[2]
    local backup_key = KEYS[3]
    local values = ARGV
    changeDataBase(db_num)
    local ret = 0
    for i = 3, #KEYS do
        ret = ret + removeItems(KEYS[i], values, default_type, backup_key)
    end
    return ret
                    """)
            delete = self.lua.register("""
    local db_num = KEYS[1]
    local keys = ARGV
    changeDataBase(db_num)
    return redis.call("DEL", unpack(keys))
                    """)
            count = self.lua.register("""
    local db_num = KEYS[1]
    local default_type = KEYS[2]
    local keys = ARGV
    local count = 0
    changeDataBase(db_num)
    for i = 1, #keys do
        count = count + getKeySize(keys[i], default_type)
    end
    return count
                    """)
            reverse = self.lua.register("""
    local db_num = KEYS[1]
    local default_type = KEYS[2]
    local dest = KEYS[3]
    local froms = ARGV
    changeDataBase(db_num)
    return mergeKeys(dest, froms, default_type)
                    """)
            smart_reverse = self.lua.register("""
    local db_num = KEYS[1]
    local default_type = KEYS[2]

    local current_key = KEYS[3]
    local temp_key = KEYS[4]
    local failure_key = KEYS[5]
    local report_key = KEYS[6]

    changeDataBase(db_num)
    -- 设置初拾变量 running
    local running = 0
    local clients = redis.call("HGETALL", report_key)

    -- 将所有客户端上报上来的正在运行的线程数相加得到集群总运行的任务数量
    for flag = 1, #clients, 2 do
        local c = clients[flag + 1]
        running = running + tonumber(c)
        redis.call("HDEL", report_key, clients[flag])
    end
    
    -- failure 有值 -> 将 failure 放入 current
    if getKeySize(failure_key) >0 then
        mergeKeys(current_key, {failure_key}, default_type)
        return 1
    end

    -- 判断 running 是否大于 0
    if running > 0 then
        return 'running > 0'
    end

    -- failure 无值, temp 有值 -> 将 temp 放入 current
    if getKeySize(temp_key) >0 then
        mergeKeys(current_key, {temp_key}, default_type)
        return 1
    else
        return 'temp.size <=0'
    end
                    """)
            get_permission = self.lua.register("""
    -- Acquire an initialization lease and return its fencing token.
    local db_num = KEYS[1]
    local current_key = KEYS[2]
    local temp_key = KEYS[3]
    local failure_key = KEYS[4]
    local record_key = KEYS[5]
    local heartbeat_key = KEYS[6]
    local epoch_key = KEYS[7]
    local default_type = KEYS[8]
    local owner_id = ARGV[1]
    local lease_id = ARGV[2]
    local interval = tonumber(ARGV[3])

    changeDataBase(db_num)

    local queue_size = getKeySize(current_key, default_type) + getKeySize(failure_key, default_type) + getKeySize(temp_key, default_type)
    local init_state = redis.call("HGET", record_key, "init_state")
    local status = redis.call("HGET", record_key, "status")
    local init_running = init_state == "INIT_RUNNING" or (not init_state and status == "1")

    if init_state == "INIT_FAILED_FINAL" then
        return cjson.encode({state=false, msg="初始化失败且不允许重试"})
    end

    if not init_running and queue_size > 0 then
        return cjson.encode({state=false, msg="已投完且存在种子没有消费完毕"})
    end

    local lease_value = redis.call("GET", heartbeat_key)
    if lease_value then
        return cjson.encode({state=false, msg="存在其他活跃的初始化机器"})
    end

    local fencing_token = redis.call("INCR", epoch_key)
    lease_value = owner_id .. "|" .. lease_id .. "|" .. fencing_token
    local ok = redis.call("SET", heartbeat_key, lease_value, "NX", "EX", interval)
    if not ok then
        return cjson.encode({state=false, msg="存在其他活跃的初始化机器"})
    end

    return cjson.encode({
        state=true,
        msg="成功获取权限",
        owner_id=owner_id,
        lease_id=lease_id,
        fencing_token=fencing_token,
        lease_token=lease_value
    })
                    """)
            set_init = self.lua.register("""
    local db_num = KEYS[1]
    local record_key = KEYS[2]
    local heartbeat_key = KEYS[3]
    local lease_token = ARGV[1]
    local now_ms = ARGV[2]
    local owner_id = ARGV[3]
    local lease_id = ARGV[4]
    local fencing_token = ARGV[5]

    changeDataBase(db_num)
    if redis.call("GET", heartbeat_key) ~= lease_token then
        return 0
    end

    redis.call("HSET", record_key,
        "time", now_ms,
        "status", 1,
        "init_state", "INIT_RUNNING",
        "owner_id", owner_id,
        "lease_id", lease_id,
        "fencing_token", fencing_token)
    redis.call("HDEL", record_key, "stop_heartbeat", "init_error")
    return 1
                    """)
            set_record = self.lua.register("""
    local db_num = KEYS[1]
    local record_key = KEYS[2]
    local heartbeat_key = KEYS[3]
    local lease_token = ARGV[1]

    changeDataBase(db_num)
    if redis.call("GET", heartbeat_key) ~= lease_token then
        return 0
    end

    for i = 2, #ARGV, 2 do
        redis.call("HSET", record_key, ARGV[i], ARGV[i + 1])
    end
    return 1
                    """)
            validate_init = self.lua.register("""
    local db_num = KEYS[1]
    local heartbeat_key = KEYS[2]
    local lease_token = ARGV[1]
    changeDataBase(db_num)
    return redis.call("GET", heartbeat_key) == lease_token
                    """)
            release_init = self.lua.register("""
    local db_num = KEYS[1]
    local record_key = KEYS[2]
    local history_key = KEYS[3]
    local heartbeat_key = KEYS[4]
    local lease_token = ARGV[1]
    local init_state = ARGV[2]
    local init_error = ARGV[3]
    local finish_time = ARGV[4]
    local history_ttl = tonumber(ARGV[5])
    local record_ttl = tonumber(ARGV[6])

    changeDataBase(db_num)
    if redis.call("GET", heartbeat_key) ~= lease_token then
        return 0
    end

    redis.call("HSET", record_key,
        "status", 0,
        "init_state", init_state,
        "finish", finish_time)
    if init_error and init_error ~= "" then
        redis.call("HSET", record_key, "init_error", init_error)
    else
        redis.call("HDEL", record_key, "init_error")
    end
    redis.call("HSET", record_key, "stop_heartbeat", 1)
    redis.call("DEL", history_key)
    local dump = redis.call('DUMP', record_key)

    if history_ttl ~= 0 then
        if history_ttl < 0 then
            history_ttl = 0
        end
        redis.call('RESTORE', history_key, history_ttl, dump)
    end

    if record_ttl >= 0 then
        redis.call('EXPIRE', record_key, record_ttl)
    end
    redis.call("DEL", heartbeat_key)
    return 1
                    """)

            continue_init_heartbeat = self.lua.register("""
    local db_num = KEYS[1]
    local record_key = KEYS[2]
    local heartbeat_key = KEYS[3]
    local lease_token = ARGV[1]
    local interval = tonumber(ARGV[2])

    changeDataBase(db_num)
    if redis.call("GET", heartbeat_key) ~= lease_token then
        return false
    end
    if redis.call("HGET", record_key, "stop_heartbeat") == "1" then
        return false
    end
    redis.call("SETEX", heartbeat_key, interval, lease_token)
    return true
            """)

        self.scripts = Scripts

    def get(self, name, count: int = 1, **kwargs):
        """
        从 `name` 中获取种子

        :param count:
        :param name:
        :param kwargs:
        :return:
        """

        pop_key = self.name2key(name, "current")
        add_key = self.name2key(name, "temp")
        db_num = kwargs.get("db_num", self.database)
        genre = kwargs.get("genre", self.genre)
        keys = [db_num, pop_key, count or 1, genre, add_key]
        return self.scripts.get(keys=keys)

    def put(self, name, *values, **kwargs):
        """
        投放种子至 `name`

        :param name:
        :param values:
        :param kwargs:
        :return:
        """
        db_num = kwargs.get("db_num", self.database)
        genre = kwargs.get("genre", self.genre)
        qtypes = kwargs.get("qtypes", ["current"])
        init_lease = kwargs.get("init_lease") or {}

        if not name or not values:
            return 0

        values = self.py2str(*values)
        queue_keys = [self.name2key(name, qtype) for qtype in pandora.iterable(qtypes)]
        if init_lease:
            lease_token = init_lease.get("lease_token")
            if not lease_token:
                return 0
            keys = [
                db_num,
                genre,
                self.name2key(name, "heartbeat"),
                *queue_keys,
            ]
            args = [lease_token, *values]
            return self.scripts.put_init(keys=keys, args=args)

        keys = [db_num, genre, *queue_keys]
        args = [*values]
        count = self.scripts.put(keys=keys, args=args)
        return count

    def replace(self, name, *values, **kwargs):
        """
        将 values 里面的 old 替换为 new
        values = (old, new), (old, new), (old, new)

        :param name: 队列名称

        :return:
        """
        if not name or not values:
            return 0

        db_num = kwargs.get("db_num", self.database)
        genre = kwargs.get("genre", self.genre)
        qtypes = kwargs.get("qtypes", ["current", "temp", "failure"])

        keys = [
            db_num,
            genre,
            *[self.name2key(name, qtype) for qtype in pandora.iterable(qtypes)],
        ]
        args = [j for i in values for j in self.py2str(*i)]
        return self.scripts.replace(keys=keys, args=args)

    def remove(self, name, *values, **kwargs):
        """
        从 `name` 中移除 `values`

        :param name:
        :param values:
        :return:
        """
        if not name or not values:
            return 0

        backup = kwargs.get("backup", "")
        if backup:
            backup = self.name2key(name, backup)

        db_num = kwargs.get("db_num", self.database)
        genre = kwargs.get("genre", self.genre)
        qtypes = kwargs.get("qtypes", ["temp"])
        keys = [
            db_num,
            genre,
            backup,
            *[self.name2key(name, qtype) for qtype in pandora.iterable(qtypes)],
        ]
        args = self.py2str(*values)
        return self.scripts.remove(keys=keys, args=args)

    def clear(
        self, *names, qtypes=("current", "temp", "lock", "record", "failure"), **kwargs
    ):
        db_num = kwargs.pop("db_num", self.database)
        keys = [db_num]
        args = [
            self.name2key(name, qtype)
            for name in names
            for qtype in pandora.iterable(qtypes)
        ]
        return self.scripts.delete(keys=keys, args=args)

    def size(self, *names, qtypes=("current", "temp", "failure"), **kwargs):
        """
        获取 `names` 的队列大小

        :param qtypes:
        :param names:
        :return:
        """
        if not names:
            return 0

        db_num = kwargs.pop("db_num", self.database)
        genre = kwargs.get("genre", self.genre)
        keys = [db_num, genre]
        args = [
            self.name2key(name, _) for name in names for _ in pandora.iterable(qtypes)
        ]
        return self.scripts.count(keys=keys, args=args)

    def reverse(self, name, **kwargs):
        """
        队列翻转

        :param name:
        :return:
        """
        if not name:
            return 0

        db_num = kwargs.pop("db_num", self.database)
        qtypes = kwargs.pop("qtypes", ["temp", "failure"])
        genre = kwargs.get("genre", self.genre)
        dest = self.name2key(name, "current")
        args = [self.name2key(name, qtype) for qtype in pandora.iterable(qtypes)]
        return self.merge(dest, *args, db_num=db_num, genre=genre)

    def merge(self, dest, *queues, **kwargs):
        """
        队列合并

        :param dest:
        :param queues:
        :return:
        """
        if not dest or not queues:
            return 0

        db_num = kwargs.pop("db_num", self.database)
        genre = kwargs.get("genre", self.genre)
        keys = [db_num, genre, dest]
        args = queues
        return self.scripts.reverse(keys=keys, args=args)

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
            chanel=f"{name}-subscribe",
            msg={"action": "collect-status", "key": self.name2key(name, "report")},
            timeout=timeout,
        )

        db_num = kwargs.pop("db_num", self.database)
        genre = kwargs.get("genre", self.genre)
        keys = [
            db_num,
            genre,
            *[
                self.name2key(name, qtype)
                for qtype in ["current", "temp", "failure", "report"]
            ],
        ]
        ret = self.scripts.smart_reverse(keys=keys)
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
        return self.size(name) <= threshold

    @classmethod
    def from_redis(cls, obj, **kwargs):
        connection_kwargs: dict = obj.connection_pool.connection_kwargs
        params = dict(
            host=connection_kwargs.get("host", "127.0.0.1"),
            password=connection_kwargs.get("password"),
            port=connection_kwargs.get("port", 6379),
            database=connection_kwargs.get("db", 0),
        )
        params.update(**kwargs)
        return cls(**params)

    def command(self, name: str, order: dict):
        def run_subscribe(chanel, adapters: dict):
            """
            订阅消息
            """

            def main(message):
                msg: dict = json.loads(message["data"])
                _action = msg["action"]
                recv: str = msg.get("recv", state.MACHINE_ID)
                if recv != state.MACHINE_ID:
                    return

                if _action in adapters:
                    func = adapters[_action]
                    ret = pandora.invoke(func, args=[msg], kwargs={"queue": self})
                    key = msg["key"]
                    self.redis_db.hset(key, mapping={state.MACHINE_ID: ret})
                    self.redis_db.expire(key, 5)

            pubsub = self.redis_db.pubsub()
            pubsub.subscribe(**{chanel: main})
            return pubsub.run_in_thread(sleep_time=0.001, daemon=True)

        def get_permission():
            def heartbeat():
                while True:
                    try:
                        if not self.scripts.continue_init_heartbeat(
                            keys=[db_num, self.name2key(name, "record"), heartbeat_key],
                            args=[lease_token, interval],
                        ):
                            break
                        time.sleep(max(interval - 1, 1))
                    except (KeyboardInterrupt, SystemExit):
                        raise

                    except Exception as e:
                        logger.error(f"[heartbeat] {e}")
                        time.sleep(1)

            db_num = order.get("db_num", self.database)
            genre = order.get("genre", self.genre)
            interval = max(int(float(order.get("interval", 5) or 5)), 2)
            owner_id = order.get("owner_id") or state.MACHINE_ID
            lease_id = order.get("lease_id") or state.MACHINE_ID
            epoch_key = self.name2key(name, "init_epoch")

            heartbeat_key = self.name2key(name, "heartbeat")
            keys = [
                db_num,
                *[
                    self.name2key(name, i)
                    for i in ["current", "temp", "failure", "record"]
                ],
                heartbeat_key,
                epoch_key,
                genre,
            ]

            while True:
                try:
                    msg = self.scripts.get_permission(
                        keys=keys,
                        args=[owner_id, lease_id, interval],
                    )
                    if isinstance(msg, bytes):
                        msg = msg.decode()
                    result = json.loads(msg) if isinstance(msg, str) else msg

                    if result.get("state"):
                        lease_token = result["lease_token"]
                        threading.Thread(
                            target=heartbeat,
                            daemon=True,
                            name=f"RedisQueueHeartbeat:{name}",
                        ).start()
                    return result
                except Exception as permission_e:
                    logger.error(f"[get_permission] {permission_e}")
                    time.sleep(30)

        def set_record():
            record = json.loads(json.dumps(order["record"], default=str))
            lease_token = order.get("lease_token")
            if not lease_token:
                return self.redis_db.hset(
                    self.name2key(name, "record"),
                    mapping=record,
                )

            values = []
            for field, value in record.items():
                values.extend([field, json.dumps(value, default=str) if isinstance(value, (dict, list)) else str(value)])
            return bool(
                self.scripts.set_record(
                    keys=[
                        order.get("db_num", self.database),
                        self.name2key(name, "record"),
                        self.name2key(name, "heartbeat"),
                    ],
                    args=[lease_token, *values],
                )
            )

        def wait_init():
            key = self.name2key(name, "record")
            # 爬虫的启动时间
            t1 = order.get("time")

            while True:
                # 如果队列不为空 -> 不需要等待初始化
                if not self.is_empty(name):
                    return

                # 初始化爬虫设置的开始初始化时间
                t2 = self.redis_db.hget(key, "time")
                init_state = self.redis_db.hget(key, "init_state")
                if init_state in {
                    TaskQueue.INIT_SUCCEEDED,
                    TaskQueue.INIT_FAILED_RETRYABLE,
                    TaskQueue.INIT_FAILED_FINAL,
                }:
                    return

                # 兼容旧记录: status=1 表示初始化已开始
                status = int(self.redis_db.hget(key, "status") or "0")
                if status == 1:
                    return

                # 初始化时间大于启动的时间
                if t2 and float(t2) >= t1:
                    return

                logger.debug("等待初始化开始")
                time.sleep(1)

        def set_init():
            lease_token = order.get("lease_token")
            if not lease_token:
                key = self.name2key(name, "record")
                return self.redis_db.hset(
                    key, mapping={"time": int(time.time() * 1000), "status": 1}
                )

            return bool(
                self.scripts.set_init(
                    keys=[
                        order.get("db_num", self.database),
                        self.name2key(name, "record"),
                        self.name2key(name, "heartbeat"),
                    ],
                    args=[
                        lease_token,
                        int(time.time() * 1000),
                        order.get("owner_id", ""),
                        order.get("lease_id", ""),
                        order.get("fencing_token", ""),
                    ],
                )
            )

        def is_init():
            key = self.name2key(name, "record")
            # 队列不为空 -> true
            if not self.is_empty(name):
                return True

            # record 存在, 并且初始化仍在运行 -> true
            if self.redis_db.exists(key) and (
                self.redis_db.hget(key, "init_state") == TaskQueue.INIT_RUNNING
                or self.redis_db.hget(key, "status") == "1"
            ):
                return True

            return False

        def validate_init():
            lease_token = order.get("lease_token")
            if not lease_token:
                return False
            return bool(
                self.scripts.validate_init(
                    keys=[
                        order.get("db_num", self.database),
                        self.name2key(name, "heartbeat"),
                    ],
                    args=[lease_token],
                )
            )

        def release_init():
            history = self.name2key(name, "history")
            db_num = order.get("db_num", self.database)
            history_ttl = order.get("history_ttl") or 0
            record_ttl = order.get("record_ttl") or 0
            lease_token = order.get("lease_token")

            if not lease_token:
                return False

            ret = self.scripts.release_init(
                keys=[
                    db_num,
                    self.name2key(name, "record"),
                    history,
                    self.name2key(name, "heartbeat"),
                ],
                args=[
                    lease_token,
                    order.get("init_state", TaskQueue.INIT_SUCCEEDED),
                    order.get("init_error", ""),
                    order.get("finish", str(datetime.datetime.now())),
                    history_ttl * 1000,
                    record_ttl,
                ],
            )
            return bool(ret)

        def get_record():
            key = self.name2key(name, "record")
            record = self.redis_db.hgetall(key) or {}
            if record.get("status") == "0" and record.get("init_state") not in {
                TaskQueue.INIT_FAILED_RETRYABLE,
                TaskQueue.INIT_FAILED_FINAL,
            }:
                self.redis_db.delete(key)
                return {}
            else:
                return record

        actions = {
            self.COMMANDS.RUN_SUBSCRIBE: lambda: run_subscribe(
                f"{name}-subscribe", order["target"]
            ),
            self.COMMANDS.GET_PERMISSION: get_permission,
            self.COMMANDS.GET_RECORD: get_record,
            self.COMMANDS.CONTINUE_RECORD: lambda: self.reverse(name),
            self.COMMANDS.SET_RECORD: set_record,
            self.COMMANDS.WAIT_INIT: wait_init,
            self.COMMANDS.RESET_INIT: lambda: self.clear(name),
            self.COMMANDS.RELEASE_INIT: release_init,
            self.COMMANDS.IS_INIT: is_init,
            self.COMMANDS.VALIDATE_INIT: validate_init,
            self.COMMANDS.SET_INIT: set_init,
        }
        action = order["action"]
        if action in actions:
            while True:
                try:
                    return actions[action]()
                except Exception as e:
                    logger.error(f"[command] 执行 {action} 失败: {e}")
                    time.sleep(5)

    def __str__(self):
        return f"<RedisQueue [ HOST: {self.host} | DB: {self.database} ]>"


if __name__ == "__main__":
    q = RedisQueue(genre="zset")
    print(q.put("xxx", "dasdasd4564646"))
    # print(q.replace('xxx', ({"name": "kemxxxx"}, {"name": "kem"})))
    # print(q.remove('xxx', *({"name": "kem"}, {"name": "xxx"}), qtypes=["current"]))
    # print(q.clear('xxx'))
