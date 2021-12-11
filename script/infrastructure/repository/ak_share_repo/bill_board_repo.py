import datetime

import akshare as ak
import pandas as pd

from script.infrastructure.repository.base.base_repo import BaseRepo
from script.infrastructure.utility.handler.local_cache_maintainer import local_cache_maintainer
from script.infrastructure.utility.handler.singleton import Singleton

CHECK_DATE = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d')


@Singleton
class BillBoardRepo(BaseRepo):

    _aggregate_root_storage = {}

    # list_id : 20211120_05

    @local_cache_maintainer(cache_project='lhb', cache_type='indicators')
    def searchable_indicators_by_date(self, check_date=CHECK_DATE, symbol_list=None):
        searchable_indicators_detail_list = []
        if not symbol_list:
            symbol_list = ak.stock_sina_lhb_detail_daily(trade_date=check_date, symbol='返回当前交易日所有可查询的指标')
        for symbol in symbol_list:
            detail_df = ak.stock_sina_lhb_detail_daily(trade_date=check_date, symbol=symbol)
            detail_df['symbol'] = symbol
            searchable_indicators_detail_list.append(detail_df)
        res = pd.concat(searchable_indicators_detail_list).reset_index()
        return res

    @local_cache_maintainer(cache_project='lhb', cache_type='recent_data')
    def lhb_ggtj_recently_by_days(self, recent_days, check_date=CHECK_DATE):
        if check_date==CHECK_DATE:
            stock_sina_lhb_ggtj = ak.stock_sina_lhb_ggtj(recent_day=f"{recent_days}")
            return stock_sina_lhb_ggtj

    @local_cache_maintainer(cache_project='lhb', cache_type='recent_data')
    def lhb_yytj_recently_by_days(self, recent_days, check_date=CHECK_DATE):
        if check_date==CHECK_DATE:
                stock_sina_lhb_yytj = ak.stock_sina_lhb_yytj(recent_day=f"{recent_days}")
                return stock_sina_lhb_yytj

    @local_cache_maintainer(cache_project='lhb', cache_type='recent_data')
    def lhb_jgzz_recently_by_days(self, recent_days, check_date=CHECK_DATE):
        if check_date == CHECK_DATE:
            stock_sina_lhb_yytj = ak.stock_sina_lhb_jgzz(recent_day=f"{recent_days}")
            return stock_sina_lhb_yytj

    @local_cache_maintainer(cache_project='lhb', cache_type='recent_data')
    def lhb_jgmx_recently(self, check_date=CHECK_DATE):
        if check_date == CHECK_DATE:
            stock_sina_lhb_jgmx = ak.stock_sina_lhb_jgmx()
            return stock_sina_lhb_jgmx


    def run_today_bill_board_repo(self):
        self.searchable_indicators_by_date()
        self.lhb_ggtj_recently_by_days(5)
        self.lhb_yytj_recently_by_days(5)
        self.lhb_jgzz_recently_by_days(5)
        self.lhb_jgmx_recently()



if __name__ == '__main__':
    x = BillBoardRepo().run_today_bill_board_repo()