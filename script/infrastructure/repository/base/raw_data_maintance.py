import ast

from loguru import logger

from script.infrastructure.env.get_env_conf import GetEnvConf
from script.infrastructure.utility.db.redis_client import RedisClient
from script.infrastructure.utility.communication.dingding import DingDing
from script.infrastructure.utility.handler.singleton import Singleton
from script.infrastructure.utility.helper.rerun_times_helper import rerun_times


@Singleton
class RawDataMaintance:

    _aggregate_root_storage = {}

    def __init__(self):
        redis_config = GetEnvConf.get_redis_conf()
        self.redis = RedisClient(redis_config["host"], redis_config["port"]).client


