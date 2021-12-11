from functools import wraps

from script.infrastructure.env.get_env_conf import GetEnvConf
from script.infrastructure.utility.db.redis_client import RedisClient
import jqdatasdk as jqd

# jqd.auth('18721053701', 'Lujiawei94116')
jqd.auth('13401689393', 'Lujiaweizuishuai001')
# jqd.auth('18818217095', 'tanC1234')

print(jqd.get_query_count())

class BaseRepo:

    _aggregate_root_storage = {}

    def __init__(self):
        redis_config = GetEnvConf.get_redis_conf()
        self.redis = RedisClient(redis_config["host"], redis_config["port"]).client


    @classmethod
    def sign_up_in_repo(cls, class_obj):
        @wraps(class_obj)
        def getinstance(*args, **kwargs):
            root_id = args[0]
            if root_id not in cls._aggregate_root_storage:
                cls._aggregate_root_storage[root_id] = class_obj(*args, **kwargs)
            return cls._aggregate_root_storage[root_id]

        return getinstance
