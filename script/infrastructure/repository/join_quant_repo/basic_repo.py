import datetime
from script.infrastructure.repository.base.base_repo import BaseRepo
from script.infrastructure.utility.handler.local_cache_maintainer import local_cache_maintainer
from script.infrastructure.utility.handler.singleton import Singleton
import pandas as pd
import jqdatasdk as jqd
from loguru import logger



CHECK_DATE = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d')


@Singleton
class BasicRepo(BaseRepo):
    _aggregate_root_storage = {}

    # @local_cache_maintainer(cache_project='jqdata', cache_type='static_data')
    # def get_normalize_code(self, code_list=None):
    #     res = jqd.normalize_code(code_list)
    #     res = pd.DataFrame({'normalize_code':res},index=code_list)
    #     return res

    @local_cache_maintainer(cache_project='jqdata', cache_type='static_data')
    def get_trade_days(self, start_time, end_time):
        res = jqd.get_trade_days(start_date=start_time, end_date=end_time, count=None)
        res = pd.Series(res)
        return res

    @local_cache_maintainer(cache_project='jqdata', cache_type='basic_data')
    def get_all_securities(self, type, check_date=CHECK_DATE):  # 'stocks','index'
        type = [] if type == 'stocks' else type
        res = jqd.get_all_securities(types=type, date=check_date)
        return res

    @local_cache_maintainer(cache_project='jqdata', cache_type='basic_data')
    def get_index_weights(self, index_id, check_date=CHECK_DATE):
        res = jqd.get_index_weights(index_id=index_id, date=check_date)
        return res

    @local_cache_maintainer(cache_project='jqdata', cache_type='static_data')
    def get_price(self, security=None, skip_paused=False, duration_days=365):
        """
        建议使用ak库，无次数限制 stock_history
        """
        end_date = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d')
        start_date = datetime.datetime.strftime(datetime.datetime.now() - datetime.timedelta(days=duration_days), '%Y-%m-%d')
        res = jqd.get_price(security, start_date=start_date, end_date=end_date, frequency='daily',
                            fields=['open', 'close', 'low', 'high', 'volume', 'money', 'factor', 'high_limit',
                                    'low_limit', 'avg', 'pre_close', 'paused'],
                            skip_paused=skip_paused, fq='pre', count=None)
        return res

    @local_cache_maintainer(cache_project='jqdata', cache_type='current_factor')
    def get_factor_kanban_values(self, universe, bt_cycle):
        category = ['quality', 'basics', 'emotion', 'growth', 'risk', 'pershare', 'style', 'technical', 'momentum']
        res = jqd.get_factor_kanban_values(universe=universe, bt_cycle=bt_cycle, model='long_only', category=category,
                                           skip_paused=True, commision_slippage=1)
        return res

    @local_cache_maintainer(cache_project='jqdata', cache_type='static_data')
    def get_all_jq_factor(self):
        res = jqd.get_all_factors()
        return res

    @local_cache_maintainer(cache_project='jqdata', cache_type='stock_history_factor')
    def get_jq_factor_value(self, start_date=None, end_date=None, code=None):
        factors = list(self.get_all_jq_factor().factor)
        res = pd.DataFrame()
        for factor in factors:
            try:
                df = jqd.get_factor_values(jqd.normalize_code(code), factor, start_date=start_date, end_date=end_date)[factor]
                df.columns = [factor]
                res = pd.concat([res,df], axis=1)
            except Exception as e:
                logger.error(e)
        return res

    @local_cache_maintainer(cache_project='jqdata', cache_type='stock_history_factor')
    def get_jq_specific_factor_value_for_index_stocks(self, factor, start_date=None, end_date=None, code=None):
        """
        :param code: 指数code
        :param if_self_index: 是否自有index
        """
        # todo 自编index函数获取stocks
        code = jqd.get_index_stocks(code)
        res = jqd.get_factor_values(jqd.normalize_code(code), factor, start_date=start_date, end_date=end_date)
        return res[factor]

    @local_cache_maintainer(cache_project='jqdata', cache_type='current_factor')
    def get_alpha_factor_from_191(self, alpha_index, code=None):
        enddate = datetime.datetime.strftime(datetime.datetime.now() - datetime.timedelta(days=1), '%Y-%m-%d')
        res = eval(f'jqd.alpha191.alpha_{alpha_index}')(code=code, end_date=enddate, fq='pre')
        return res

    @local_cache_maintainer(cache_project='jqdata', cache_type='current_factor')
    def get_alpha_factor_from_101(self, alpha_index):
        enddate = datetime.datetime.strftime(datetime.datetime.now() - datetime.timedelta(days=1), '%Y-%m-%d')
        res = eval(f'jqd.alpha101.alpha_{alpha_index}')(enddate, index='all')
        return res

    def run_today_join_quant_repo(self):
        self.get_all_securities('stocks')
        self.get_index_weights('hs300')


if __name__ == '__main__':
    # factors1 = BasicRepo().get_factor_kanban_values('hs300', 'month_3')
    # factors2 = BasicRepo().get_factor_kanban_values('hs300', 'year_1')
    # factors3 = BasicRepo().get_factor_kanban_values('hs300', 'year_3')
    # factors4 = BasicRepo().get_factor_kanban_values('hs300', 'year_10')
    # for i in range(1,102):
    #     if n:= 3 - len(str(i)):
    #         i = n*'0'+str(i)
    #     try:
    #         factors_alpha = BasicRepo().get_alpha_factor_from_101(i)
    #     except:
    #         continue
    #
    # all_securities_df = BasicRepo().get_all_securities('index')
    # print(all_securities_df)
    # code_list = list(all_securities_df.index[:])

    #
    # for i in range(60,192):
    #     if n:= 3 - len(str(i)):
    #         i = n*'0'+str(i)
    #     try:
    #         print(i)
    #         factors_alpha = BasicRepo().get_alpha_factor_from_191(i,code=code_list)
    #     except:
    #         continue
    #
    # factors_alpha = BasicRepo().get_alpha_factor_from_191('059', code=code_list)
    # print(factors_alpha)

    # for code in ['601020', '600737', '002309','600845','601318','600745','002241','600298','603605','600036','002913',
    #              '002340','002415','600309','600845','002241','300750','603605','000977']:
    #     print(code)
    #     res2 = BasicRepo().get_jq_factor_value(start_date='2017-01-03', end_date='2021-12-07',code=code)

    factors = list(BasicRepo().get_all_jq_factor().factor)
    # for factor in factors:
    for factor in [
        # 质量
        'net_profit_to_total_operate_revenue_ttm', 'cfo_to_ev', 'accounts_payable_turnover_days',
        'sale_expense_to_operating_revenue', 'net_operating_cash_flow_coverage', 'MLEV', 'super_quick_ratio',
        'operating_profit_growth_rate', 'net_operate_cash_flow_to_asset', 'total_asset_turnover_rate',
        'operating_profit_ratio', 'cash_rate_of_sales', 'operating_profit_to_operating_revenue',
        'cash_to_current_liability', 'ACCA', 'margin_stability',
        # 情绪
        'VEMA12', 'VR', 'VOL5', 'BR', 'WVAD', 'MAWVAD', 'VSTD10', 'VOL10', 'VSTD20', 'VMACD', 'turnover_volatility',
        'VEMA5',
        # 成长
        'operating_revenue_growth_rate', 'net_operate_cashflow_growth_rate', 'np_parent_company_owners_growth_rate',
        'financing_cash_growth_rate','total_profit_growth_rate',
        # 风险
        'Variance20','Skewness20','Kurtosis20','sharpe_ratio_20',
        # 每股
        'cashflow_per_share_ttm','eps_ttm',
        # 风格
        'size','beta','momentum','residual_volatility','non_linear_size','earnings_yield','growth'
    ]:



        res2 = BasicRepo().get_jq_specific_factor_value_for_index_stocks(factor, start_date='2020-01-03', end_date='2021-12-09', code='000300.XSHG')
    ...