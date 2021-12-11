from script.infrastructure.env.get_env_conf import GetEnvConf


class MongoConfig:
    def __init__(self):
        mongo_conn_string, db_name, db_set = GetEnvConf.get_mongo_config()
        self.mongo_merchant = {'conn': mongo_conn_string, 'db_name': db_name, 'set': db_set['merchant']}
        self.mongo_user = {'conn': mongo_conn_string, 'db_name': db_name, 'set': db_set['user']}
