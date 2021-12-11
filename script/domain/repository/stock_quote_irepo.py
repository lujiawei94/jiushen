from script.infrastructure.repository.ak_share_repo.stock_quote_repo import StockQuoteRepo
import pandas as pd

class StockQuoteIrepo:


    @staticmethod
    def get_stock_quote_by_days(target_code_list, period, end_date):
        """

        :param code: 601919
        :param period:  '1_minute','5_minute','15_minute','30_minute','60_minute', 'daily', 'weekly', 'monthly'
        :param end_date: '20211130'
        :return:
        """
        df_res = pd.DataFrame()
        for code in target_code_list:
            df = StockQuoteRepo().stock_history(code=code, period=period, end_date=end_date)
            df['代码'] = code
            df_res = pd.concat([df_res,df])
        return df_res


    @staticmethod
    def get_allocation_match_object(code_id):
        return StockQuoteRepo()._aggregate_root_storage.get(code_id)


if __name__ == '__main__':
    res = StockQuoteIrepo.get_stock_quote_by_days(['601919', '601899', '601020', '600737', '002309'],'daily','20210423')
    ...