import copy
import datetime

import akshare as ak

from script.infrastructure.repository.base.base_repo import BaseRepo
from script.infrastructure.utility.handler.local_cache_maintainer import local_cache_maintainer
from script.infrastructure.utility.handler.singleton import Singleton


@Singleton
class StockQuoteRepo(BaseRepo):
    _aggregate_root_storage = {}

    @local_cache_maintainer(cache_project='stock_quote', cache_type='stock_history')
    def stock_history(self, code=None, period='daily', start_date=None, end_date=None):
        if 'minute' in period:  # '1_minute','5_minute','15_minute','30_minute','60_minute'
            stock_history = ak.stock_zh_a_hist_min_em(symbol=str(code), period=period.split('_')[0], start_date=start_date, end_date=end_date, adjust='qfq')
            stock_history = stock_history.set_index('时间')
        else:  # 'daily', 'weekly', 'monthly'
            stock_history = ak.stock_zh_a_hist(symbol=str(code), period=period, start_date=start_date, end_date=end_date, adjust='qfq')
            stock_history = stock_history.set_index('日期')
        return stock_history

    @local_cache_maintainer(cache_project='stock_quote', cache_type='current_closing')
    def all_stocks_closing_figures(self):
        stock_closing_figures = ak.stock_zh_a_spot_em()
        return stock_closing_figures

    @staticmethod
    def stock_real_time_quote(code=None):
        stock_history = ak.stock_zh_a_spot_em()
        if isinstance(code, str):
            res = stock_history.loc[lambda x: x['代码'] == str(code)]
        elif hasattr(code, '__iter__'):
            res = stock_history.loc[lambda x: x['代码'].isin([str(i) for i in code])]
        return res

    def run_stock_quote_repo_weekly(self):
        TODAY = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d')
        all_securities_df = self.all_stocks_closing_figures()
        code_list = list(all_securities_df['代码'][:])

        for code in code_list:
            try:
                print(code)
                res = self.stock_history(code=code, period='60_minute', start_date='2015-01-01', end_date=TODAY)
                # res1 = self.stock_history(code=code, period='30_minute', start_date='2015-01-01', end_date=TODAY)
                # res2 = self.stock_history(code=code, period='15_minute', start_date='2015-01-01', end_date=TODAY)
                # res3 = self.stock_history(code=code, period='5_minute', start_date='2015-01-01', end_date=TODAY)
                # res4 = self.stock_history(code=code, period='1_minute', start_date='2015-01-01', end_date=TODAY)
                res5 = self.stock_history(code=code, period='daily', start_date='2015-01-01', end_date=TODAY)
                res6 = self.stock_history(code=code, period='weekly', start_date='2015-01-01', end_date=TODAY)
                res7 = self.stock_history(code=code, period='monthly', start_date='2015-01-01', end_date=TODAY)
            except Exception as e:
                print(e)


if __name__ == '__main__':
    # SOMEDAY = '20210122'
    # all_securities_df = StockQuoteRepo().all_stocks_closing_figures()
    # code_list = list(all_securities_df['代码'][:])
    #
    # for code in code_list:
    #     print(code)
    #     # res = StockQuoteRepo().stock_history(code=code, period='60_minute', end_date=SOMEDAY)
    #     # res1 = StockQuoteRepo().stock_history(code=code, period='30_minute', end_date=SOMEDAY)
    #     # res2 = StockQuoteRepo().stock_history(code=code, period='15_minute', end_date=SOMEDAY)
    #     # res3 = StockQuoteRepo().stock_history(code=code, period='5_minute', end_date=SOMEDAY)
    #     # res4 = StockQuoteRepo().stock_history(code=code, period='1_minute', end_date=SOMEDAY)
    #     res5 = StockQuoteRepo().stock_history(code=code, period='daily', end_date=SOMEDAY)
    #     # res6 = StockQuoteRepo().stock_history(code=code, period='weekly', end_date=SOMEDAY)
    #     # res7 = StockQuoteRepo().stock_history(code=code, period='monthly', end_date=SOMEDAY)

    # print(StockQuoteRepo().stock_real_time_quote(code=['601919','601899','601020','600737','002309']))
    StockQuoteRepo().run_stock_quote_repo_weekly()
    # res = StockQuoteRepo().stock_history(code='601919', period='60_minute', start_date='2019-12-04', end_date='2021-12-04')
    # res5 = StockQuoteRepo().stock_history(code='601919', period='daily', start_date='2014-01-01', end_date='2021-12-04')

    ...
