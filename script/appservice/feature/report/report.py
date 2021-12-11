import uuid


from loguru import logger


from script.infrastructure.repository.base.raw_data_maintance import RawDataMaintance
from script.infrastructure.utility.handler.singleton import delete_singleton
from script.infrastructure.utility.helper.http_helper import HttpHelper
from script.infrastructure.utility.helper.log_helper import LogTraceAdd
from script.infrastructure.utility.helper.rerun_times_helper import rerun_times



class Report:

    def __init__(self):
        self.task_name = ""


    def to_do(self):
        delete_singleton()
        task_id = uuid.uuid4().hex
        raw_data_maintance = RawDataMaintance()

        self.start()


    def start(self):
        try:
            ...
            with LogTraceAdd('general'):
                logger.debug('')
        except:
            with LogTraceAdd('general'):
                logger.error('')
        finally:
            ...






if __name__ == '__main__':
    match = Report()
    match.to_do()
