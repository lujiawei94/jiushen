from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import re

from script.infrastructure.env.get_env_conf import GetEnvConf


class GeneralMysql:

    def __init__(self):
        self._db_info = GetEnvConf.get_mysql_config()
        self.engine = create_engine(self._db_info, echo=False, pool_size=10, max_overflow=20, pool_recycle=3600)

    @property
    def session(self):
        return sessionmaker(bind=self.engine)()

    @property
    def db_info(self):
        return re.findall('//.*:.*@(.*:\d*/.*)', self._db_info)[0]


