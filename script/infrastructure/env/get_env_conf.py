from script.appservice.config.config import *
from script.appservice.config.env import *
from script.appservice.config.config import REDIS


class GetEnvConf:

    @staticmethod
    def get_redis_conf(config_type="test"):
        # config_type分为测试环境和生产环境，如果是生产环境，config_type为"product"
        # redis_config = get_redis_conf()
        # redis = RedisClient(redis_config["host"], redis_config["port"]).client
        redis_config = REDIS[config_type]
        return redis_config

    @staticmethod
    def get_mysql_config():
        return sql_conn_config[platform]