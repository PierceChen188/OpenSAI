import json

import redis
import logging
from redis.exceptions import RedisError

from config.redis_config import REDIS_HOST, REDIS_PORT, REDIS_DB


class RedisUtils:

    def __init__(self):
        # 创建Redis连接池
        pool = redis.ConnectionPool(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, max_connections=100)
        self.r = redis.Redis(connection_pool=pool)

    def expire(self, key, time):
        """
        指定缓存失效时间
        :param key: 键
        :param time: 时间(秒)
        :return: 是否成功
        """
        try:
            if time > 0:
                self.r.expire(key, time)
            return True
        except RedisError as e:
            logging.error(e)
            return False

    def get_expire(self, key):
        """
        根据key 获取过期时间
        :param key: 键 不能为null
        :return: 时间(秒) 返回0代表为永久有效
        """
        try:
            expire = self.r.ttl(key)
            if expire is None:
                # 如果返回None，表示键是永久有效的
                return 0
            return expire
        except RedisError as e:
            logging.error(e)
            return -1  # 或者其他适当的错误代码

    def has_key(self, key):
        """
        判断key是否存在
        :param key: key 键
        :return: true 存在 false不存在
        """
        try:
            return self.r.exists(key)
        except RedisError as e:
            logging.error(e)
            return False

    def delete(self, *keys):
        """
        删除缓存
        :param key: 可以传一个值 或多个
        """
        try:
            if keys:
                self.r.delete(*keys)
        except RedisError as e:
            logging.error(e)

    # = == == == == == == == == == == == == == =String == == == == == == == == == == == == == == =
    def get(self, key):
        """
        普通缓存获取
        :param key: key 键
        :return: 值
        """
        try:
            value = self.r.get(key)
            return value.decode('utf-8') if value else None
        except RedisError as e:
            logging.error(e)
            return None

    def get_data_type(self, key, data_type):
        """
        获取键值为key的Value并转换为指定类型
        :param key: 键值
        :param data_type: 指定类型
        :return: key对应的value
        """
        try:
            value = self.r.get(key)
            if value:
                decoded_value = value.decode('utf-8')
                return json.loads(decoded_value, cls=data_type)
            return None
        except RedisError as e:
            logging.error(e)
            return None

    def get_partial(self, key, bean_type, start, end):
        """
        普通缓存获取，可设置获取位数
        :param key: 键
        :param bean_type:
        :param start: 开始位置
        :param end: 结束位置
        :return: 值
        """
        try:
            value = self.r.getrange(key, start, end)
            if value:
                decoded_value = value.decode('utf-8')
                return json.loads(decoded_value, cls=bean_type)
            return None
        except RedisError as e:
            logging.error(e)
            return None

    def set_key_value(self, key, value):
        """
         普通缓存放入
        :param key: 键
        :param value: 值
        :return: true成功 false失败
        """
        try:
            self.r.set(key, value)
            return True
        except RedisError as e:
            logging.error(e)
            return False

    def set_key_value_with_ttl(self, key, value, time):
        """
        普通缓存放入并设置时间
        :param key: 键
        :param value: 值
        :param time: 时间(秒) time要大于0 如果time小于等于0 将设置无限期
        :return: true成功 false 失败
        """
        try:
            if time > 0:
                self.r.setex(key, time, value)
            else:
                self.r.set(key, value)
            return True
        except RedisError as e:
            logging.error(e)
            return False

    # 对Redis键执行递增操作
    def incr_key(self, key, delta):
        """
        对Redis键执行递增操作
        :param key: 键
        :param delta: 要增加几(大于0)
        :return: long
        """
        if delta <= 0:
            raise ValueError("递增因子必须大于0")

        try:
            result = self.r.incrby(key, delta)
            return result
        except RedisError as e:
            logging.error(e)

    def decr_key(self, key, delta):
        """
        对Redis键执行递减操作
        :param key: 键
        :param delta: 要减少几(小于0)
        :return:
        """
        if delta <= 0:
            raise ValueError("递减因子必须大于0")

        try:
            result = self.r.incrby(key, -delta)
            return result
        except RedisError as e:
            logging.error(e)

    # = == == == == == == == == == == == == == =Map == == == == == == == == == == == == == == =
    def hget(self, key, item):
        """
        获取哈希表中指定键的值
        :param key:
        :param item:
        :return:
        """
        try:
            value = self.r.hget(key, item)
            return value.decode('utf-8') if value else None
        except RedisError as e:
            logging.error(e)
            return None

    def all_keys(self, key):
        """
        获取哈希表中的所有键
        :param key:
        :return:
        """
        try:
            keys = self.r.hkeys(key)
            return [k.decode('utf-8') for k in keys]
        except RedisError as e:
            logging.error(e)
            return []

    def all_values(self, key):
        """
        获取哈希表中的所有值
        :param key: 键值
        :return:
        """
        try:
            values = self.r.hvals(key)
            return [v.decode('utf-8') for v in values]
        except RedisError as e:
            logging.error(e)
            return []

    def hmget(self, key):
        """
        获取哈希表中所有键值对
        :param key: 键值
        :return: 以字典类型(dict)从Redis的哈希表（hgetall）中获取的所有键值对
        """
        try:
            entries = self.r.hgetall(key)
            decoded_entries = {k.decode('utf-8'): v.decode('utf-8') for k, v in entries.items()}
            return decoded_entries
        except RedisError as e:
            logging.error(e)
            return {}

    def hget_random_key(self, key):
        """
        随机获取一个字段（field）
        :param key:
        :return:
        """
        try:
            field = self.r.hrandfield(key)
            return field.decode('utf-8') if field else None
        except RedisError as e:
            logging.error(e)
            return None

    def hget_random_keys(self, key, count):
        """
        随机获取多个字段（field）
        :param key:
        :param count:
        :return:
        """
        try:
            fields = self.r.hrandfield(key, count)
            return [field.decode('utf-8') for field in fields]
        except RedisError as e:
            logging.error(e)
            return []

    def hget_random_entry(self, key):
        """
        随机获取一个字段和对应的值
        :param key:
        :return:
        """
        try:
            entry = self.r.hrandfield(key, 1, withvalues=True)
            if entry:
                field, value = entry[0]
                return field.decode('utf-8'), value.decode('utf-8')
            return None, None
        except RedisError as e:
            logging.error(e)
            return None, None

    def hget_random_entries(self, key, count):
        """
        随机获取多个字段和对应的值
        :param key:
        :param count:
        :return:
        """
        try:
            entries = self.r.hrandfield(key, count, withvalues=True)
            result = {}
            for field, value in entries:
                result[field.decode('utf-8')] = value.decode('utf-8')
            return result
        except RedisError as e:
            logging.error(e)
            return {}

    def hsize(self, key):
        """
        获取哈希表的大小
        :param key:
        :return:
        """
        try:
            size = self.r.hlen(key)
            return size
        except RedisError as e:
            logging.error(e)
            return None

    def hmset(self, key, map):
        """
        设置哈希表的多个字段和值
        :param key:
        :param map:
        :return:
        """
        try:
            self.r.hmset(key, map)
            return True
        except RedisError as e:
            logging.error(e)
            return False

    def hmset_with_ttl(self, key, map, time):
        """
        设置哈希表的多个字段和值，并设置过期时间
        :param key:
        :param map:
        :param time:
        :return:
        """
        try:
            self.r.hmset(key, map)
            if time > 0:
                self.r.expire(key, time)
            return True
        except RedisError as e:
            logging.error(e)
            return False

    def hset(self, key, item, value):
        """
        向哈希表中放入数据，如果不存在则创建
        :param key:
        :param item:
        :param value:
        :return:
        """
        try:
            self.r.hset(key, item, value)
            return True
        except RedisError as e:
            logging.error(e)
            return False

    def hset_with_ttl(self, key, item, value, time):
        """
        向哈希表中放入数据，如果不存在则创建，并设置过期时间
        :param key:
        :param item:
        :param value:
        :param time:
        :return:
        """
        try:
            self.r.hset(key, item, value)
            if time > 0:
                self.r.expire(key, time)
            return True
        except RedisError as e:
            logging.error(e)
            return False

    def hdel(self, key, *items):
        """
        删除哈希表中的值
        :param key:
        :param items:
        :return:
        """
        try:
            self.r.hdel(key, *items)
        except RedisError as e:
            logging.error(e)

    def hhas_key(self, key, item):
        """
        判断哈希表中是否存在指定项
        :param key:
        :param item:
        :return:
        """
        return self.r.hexists(key, item)

    def hincr(self, key, item, by):
        """
        哈希表递增，如果不存在则创建并返回新值
        :param key:
        :param item:
        :param by:
        :return:
        """
        try:
            new_value = self.r.hincrbyfloat(key, item, by)
            return new_value
        except RedisError as e:
            logging.error(e)
            return None

    def hdecr(self, key, item, by):
        """
        哈希表递减操作
        :param item:
        :param by:
        :return:
        """
        try:
            new_value = self.r.hincrbyfloat(key, item, -by)
            return new_value
        except RedisError as e:
            logging.error(e)
            return None

    # = == == == == == == == == == == == == == =zSet == == == == == == == == == == == == == == =

    def zset_with_ttl(self, key, value, index, time):
        """
        有序集合排序放入并设置过期时间
        :param key: 键
        :param value: 值
        :param index: 索引
        :param time: ttl时间
        :return: 是否存储成功True or False
        """
        try:
            self.r.zadd(key, {value: index})
            if time > 0:
                self.r.expire(key, time)
            return True
        except RedisError as e:
            logging.error(e)
            return False

    def zset(self, key, value, index):
        """
        有序集合排序放入
        :param key: 键
        :param value: 值
        :param index: 索引
        :return: 是否存储成功True or False
        """
        try:
            self.r.zadd(key, {value: index})
            return True
        except RedisError as e:
            logging.error(e)
            return False

    def zget(self, key, start, end):
        """
        有序集合顺序获取，可设置获取范围
        :param key: 键
        :param start: 获取范围的开始位置
        :param end: 获取范围的结束位置
        :return: 值
        """
        try:
            values = self.r.zrangebyscore(key, start, end)
            return values
        except RedisError as e:
            logging.error(e)
            return []

    def zget(self, key, start, end):
        """
        有序集合顺序获取，可设置获取范围
        :param key:
        :param start:
        :param end:
        :return:
        """
        try:
            values = self.r.zrange(key, start, end)
            return values
        except RedisError as e:
            logging.error(e)
            return []

    def zget_reverse(self, key, start, end):
        """
        有序集合逆序获取，可设置获取范围
        :param key:
        :param start:
        :param end:
        :return:
        """
        try:
            values = self.r.zrevrange(key, start, end)
            return values
        except RedisError as e:
            logging.error(e)
            return []

    def remove_range(self, key, start, end):
        """
        有序集合移除范围内的元素
        :param key:
        :param start:
        :param end:
        :return:
        """
        try:
            self.r.zremrangebyrank(key, start, end)
            return True
        except RedisError as e:
            logging.error(e)
            return False

    def remove_by_value(self, key, *values):
        """
        有序集合移除范围内的元素
        :param key:
        :param values:
        :return:
        """
        try:
            self.r.zrem(key, *values)
            return True
        except RedisError as e:
            logging.error(e)
            return False

    def pop_max(self, key):
        """
        弹出最大值
        :param key:
        :return:
        """
        try:
            value = self.r.zpopmax(key)
            if value:
                return value[0][0]  # 返回弹出的最大值
            return None
        except RedisError as e:
            logging.error(e)
            return None

    def pop_max_count(self, key, count):
        """
        弹出表中前count位值
        :param key:
        :param count:
        :return:
        """
        try:
            values = self.r.zpopmax(key, count)
            return [value[0] for value in values]
        except RedisError as e:
            logging.error(e)
            return []

    def zset_size(self, key):
        """
        获取有序集合大小
        :param key:
        :return:
        """
        try:
            size = self.r.zcard(key)
            return size
        except RedisError as e:
            logging.error(e)
            return None

    def sget(self, key):
        """
        根据key获取Set中的所有值
        :param key: 键值
        :return: 返回一个包含集合中所有成员的集合（set）
        """
        try:
            values = self.r.smembers(key)
            return values
        except RedisError as e:
            logging.error(e)
            return None

    def sget_block(self, key):
        """
        根据key获取阻塞任务Set中的所有值
        :param key: 键值
        :return: 返回一个包含集合中所有成员的集合（set）
        """
        try:
            if self.r.exists(key):
                values = self.r.smembers(key)
                return values
            return None
        except RedisError as e:
            logging.error(e)
            return None

    def sget_count(self, key, count):
        """
        根据key随机获取Set中的指定个数的值
        :param key: 键值
        :param count: 数量
        :return: 返回一个包含指定数量随机成员的集合（set）
        """
        try:
            values = self.r.srandmember(key, count)
            return values
        except RedisError as e:
            logging.error(e)
            return None

    def shas_key(self, key, value):
        """
        根据value从一个set中查询是否存在
        :param key:
        :param value:
        :return:
        """
        try:
            return self.r.sismember(key, value)
        except RedisError as e:
            logging.error(e)
            return False

    def sset(self, key, *values):
        """
        将数据放入set缓存
        :param key:
        :param values:
        :return:
        """
        try:
            return self.r.sadd(key, *values)
        except RedisError as e:
            logging.error(e)
            return 0

    def sset_and_time(self, key, time, *values):
        """
        将set数据放入缓存并设置过期时间
        :param key:
        :param time:
        :param values:
        :return:
        """
        try:
            count = self.r.sadd(key, *values)
            if time > 0:
                self.r.expire(key, time)
            return count
        except RedisError as e:
            logging.error(e)
            return 0

    def sget_set_size(self, key):
        """
        获取set缓存的长度
        :param key:
        :return:
        """
        try:
            size = self.r.scard(key)
            return size
        except RedisError as e:
            logging.error(e)
            return 0

    def set_remove(self, key, *values):
        """
        移除值为value的元素
        :param key:
        :param values:
        :return:
        """
        try:
            count = self.r.srem(key, *values)
            return count
        except RedisError as e:
            logging.error(e)
            return 0

    def lget(self, key, start, end):
        """
        获取list缓存的内容
        :param key:
        :param start:
        :param end:
        :return:
        """
        try:
            values = self.r.lrange(key, start, end)
            return values
        except RedisError as e:
            logging.error(e)
            return None

    def lget_list_size(self, key):
        """
        获取list缓存的长度
        :param key:
        :return:
        """
        try:
            size = self.r.llen(key)
            return size
        except RedisError as e:
            logging.error(e)
            return 0

    def lget_index(self, key, index):
        """
        通过索引获取list中的值
        :param key:
        :param index:
        :return:
        """
        try:
            value = self.r.lindex(key, index)
            return value
        except RedisError as e:
            logging.error(e)
            return None

    def lset(self, key, value):
        """
        将list放入缓存
        :param key:
        :param value:
        :return:
        """
        try:
            self.r.rpush(key, value)
            return True
        except RedisError as e:
            logging.error(e)
            return False

    def lset_time(self, key, value, time=None):
        """
        将单个值放入列表
        :param key:
        :param value:
        :param time:
        :return:
        """
        try:
            self.r.rpush(key, value)
            if time is not None and time > 0:
                self.r.expire(key, time)
            return True
        except RedisError as e:
            logging.error(e)
            return False

    def lset_multiple(self, key, values, time=None):
        """
        将多个值放入列表
        :param key:
        :param values:
        :param time:
        :return:
        """
        try:
            self.r.rpush(key, *values)
            if time is not None and time > 0:
                self.r.expire(key, time)
            return True
        except RedisError as e:
            logging.error(e)
            return False

    def lupdate_index(self, key, index, value):
        """
        根据索引修改列表中的某个值
        :param key:
        :param index:
        :param value:
        :return:
        """
        try:
            self.r.lset(key, index, value)
            return True
        except RedisError as e:
            logging.error(e)
            return False

    def lremove(self, key, count, value):
        """
        移除N个值为value
        :param key:
        :param count:
        :param value:
        :return:
        """
        try:
            removed_count = self.r.lrem(key, count, value)
            return removed_count
        except RedisError as e:
            logging.error(e)
            return 0

    # 返回多个给定的key的值
    def mget(self, keys):
        """
        返回多个给定的key的值
        :param keys:
        :return:
        """
        try:
            values = self.r.mget(keys)
            return values
        except RedisError as e:
            logging.error(e)
            return []

    def keys(self, pattern):
        """
        模糊查询获取key值
        :param pattern:
        :return:
        """
        try:
            matched_keys = self.r.keys(pattern)
            return matched_keys
        except RedisError as e:
            logging.error(e)
            return []

    def convert_and_send(self, channel, message):
        """
        使用Redis的消息队列
        :param channel:
        :param message:
        :return:
        """
        try:
            self.r.publish(channel, message)
            return True
        except RedisError as e:
            logging.error(e)
            return False

    # ========================指定db存储用法 Start=========================

    def set_with_db(self, key, value, db, flag_json, time_out):
        """
        写入缓存并指定数据库
        :param key:
        :param value:
        :param db:
        :param flag_json:
        :param time_out:
        :return:
        """
        try:
            self.r.select(db)
            if flag_json:
                value_str = json.dumps(value)
                self.r.set(key, value_str)
            else:
                # 如果存入对象为Set，可以使用sadd方法
                self.r.sadd(key, *value)
            if time_out is not None and time_out != 0:
                self.r.expire(key, time_out)
            return True
        except redis.RedisError as e:
            print(e)
            return False

    def get_with_db(self, key, db):
        """
        读取指定数据库的缓存
        :param key:
        :param db:
        :return:
        """
        try:
            self.r.select(db)
            result = self.r.get(key)
            if result:
                return result.decode('utf-8')
            else:
                return None
        except redis.RedisError as e:
            print(e)
            return None

    def s_members_with_db(self, key, db):
        """
        获取指定数据库中Set的所有元素
        :param key:
        :param db:
        :return:
        """
        try:
            self.r.select(db)
            result = self.r.smembers(key)
            return set(result)
        except redis.RedisError as e:
            print(e)
            return None

    def remove_with_db(self, key, db):
        """
        删除指定数据库的键
        :param key:
        :param db:
        :return:
        """
        try:
            self.r.select(db)
            if self.r.exists(key):
                self.r.delete(key)
        except redis.RedisError as e:
            print(e)

    # 根据起始和结束序号遍历Redis中的list
    def range_list(self, list_key, start, end):
        # 使用绑定操作
        bound_list = redis.client.StrictRedis(connection_pool=self.r.connection_pool).list(list_key)
        # 查询数据
        return bound_list[start:end + 1]

    # 弹出右边的值并从列表中移除
    def right_pop(self, list_key):
        # 使用绑定操作
        bound_list = redis.client.StrictRedis(connection_pool=self.r.connection_pool).list(list_key)
        return bound_list.pop()


if __name__ == '__main__':
    r = RedisUtils()
    # 使用示例
    list_key = "my_list"

    # 将单个值放入列表
    r.lset(list_key, "value1")

    # 将多个值放入列表
    value_list = ["value2", "value3", "value4"]
    r.lset_multiple(list_key, value_list)

    # 根据索引修改值
    r.lupdate_index(list_key, 1, "new_value")

    # 移除N个值为特定值的元素
    r.lremove(list_key, 2, "value3")

    # 获取多个key的值
    keys_to_get = ["key1", "key2", "key3"]
    values = r.mget(keys_to_get)

    # 模糊查询获取key值
    matched_keys = r.keys("pattern*")

    # 使用Redis的消息队列发送消息
    r.convert_and_send("my_channel", "Hello, Redis!")
