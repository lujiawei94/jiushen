import redis


class RedisClient:

    def __init__(self, host, port, password=None, max_connections=10000, decode_responses=True):
        """

        :param host: redis 连接ip地址
        :param port: redis 连接端口号
        :param max_connections: redis 最大连接数
        :param password: redis 连接认证密码
        :param decode_responses: redis 取出的结果默认是字节，我们可以设定 decode_responses=True 改成字符串
        """
        pool = redis.ConnectionPool(host=host, port=port, max_connections=max_connections, decode_responses=decode_responses)
        self.client = redis.Redis(connection_pool=pool, password=password)


if __name__ == '__main__':
    redis = RedisClient("redis-master.sit.blackfi.sh", 6379).client

    # 获取所有的key
    print(redis.keys())

    # redis.set("name", "kuangcx", nx=True)
    # redis.set("age", "18", nx=True)
    #
    # print(redis.mget("name", "age"))
    # print(redis.mget(["name", "age"]))

    # redis.setrange("xxx", 2, "zzz")
    # print(redis.get("xxx"))
    print(redis.mget("token_2020002028"))