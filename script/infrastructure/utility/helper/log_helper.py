import datetime

from loguru import logger

LOGPATH_SOURCE = '/Users/apple/PycharmProjects/jiushen/logs'


class LogTraceAdd:

    def __init__(self, log_pkg, **kwargs):
        self.trace = logger.add(f"{LOGPATH_SOURCE}/{log_pkg}/{datetime.datetime.now().strftime('%Y_%m_%d')}.log", **kwargs)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        logger.remove(self.trace)
