import datetime
from script.infrastructure.repository.base.base_repo import BaseRepo
from script.infrastructure.utility.handler.local_cache_maintainer import local_cache_maintainer
from script.infrastructure.utility.handler.singleton import Singleton
from script.infrastructure.repository.join_quant_repo.basic_repo import BasicRepo
import pandas as pd
import jqdatasdk as jqd

jqd.auth('13401689393', 'Lujiaweizuishuai001')
# jqd.auth('18818217095', 'tanC1234')
print(jqd.get_query_count())

CHECK_DATE = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d')


@Singleton
class OverboughtOversold(BaseRepo):
    _aggregate_root_storage = {}


    @local_cache_maintainer(cache_project='jqdata', cache_type='tec_analysis')
    def get_ACCER(self, check_date=CHECK_DATE, N=8, unit='1d'):
        security_list = list(BasicRepo().get_all_securities('stocks', check_date=check_date).index[:])
        res = jqd.technical_analysis.ACCER(security_list, check_date, N=N, unit=unit, include_now=True)
        res = pd.DataFrame(pd.Series(res)).rename(columns={0: 'ACCER'})
        return res

    @local_cache_maintainer(cache_project='jqdata', cache_type='tec_analysis')
    def get_ADTM(self, check_date=CHECK_DATE, N=23, M=8, unit='1d'):
        security_list = list(BasicRepo().get_all_securities('stocks', check_date=check_date).index[:])
        res = jqd.technical_analysis.ADTM(security_list, check_date, N=N, M=M, unit=unit, include_now=True)
        res = pd.DataFrame(res).T.rename(columns={0: 'ADTM', 1: 'MAADTM'})
        return res

    @local_cache_maintainer(cache_project='jqdata', cache_type='tec_analysis')
    def get_ATR(self, check_date=CHECK_DATE, timeperiod=14, unit='1d'):
        security_list = list(BasicRepo().get_all_securities('stocks', check_date=check_date).index[:])
        res = jqd.technical_analysis.ATR(security_list, check_date, timeperiod=timeperiod, unit=unit, include_now=True)
        res = pd.DataFrame(res).T.rename(columns={0: 'MTR', 1: 'ATR'})
        return res

    @local_cache_maintainer(cache_project='jqdata', cache_type='tec_analysis')
    def get_BIAS(self, check_date=CHECK_DATE, N1=6, N2=12, N3=24, unit='1d'):
        security_list = list(BasicRepo().get_all_securities('stocks', check_date=check_date).index[:])
        res = jqd.technical_analysis.BIAS(security_list, check_date, N1=N1, N2=N2, N3=N3, unit=unit, include_now=True)
        res = pd.DataFrame(res).T.rename(columns={0: 'BIAS1', 1: 'BIAS2', 2: 'BIAS3'})
        return res

    @local_cache_maintainer(cache_project='jqdata', cache_type='tec_analysis')
    def get_BIAS_QL(self, check_date=CHECK_DATE, N=6, M=6, unit='1d'):
        security_list = list(BasicRepo().get_all_securities('stocks', check_date=check_date).index[:])
        res = jqd.technical_analysis.BIAS_QL(security_list, check_date, N=N, M=M, unit=unit, include_now=True)
        res = pd.DataFrame(res).T.rename(columns={0: 'BIAS', 1: 'BIASMA'})
        return res

    @local_cache_maintainer(cache_project='jqdata', cache_type='tec_analysis')
    def get_BIAS_36(self, check_date=CHECK_DATE, M=6, unit='1d'):
        security_list = list(BasicRepo().get_all_securities('stocks', check_date=check_date).index[:])
        res = jqd.technical_analysis.BIAS_36(security_list, check_date, M=M, unit=unit, include_now=True)
        res = pd.DataFrame(res).T.rename(columns={0: 'BIAS36', 1: 'BIAS612', 2: 'MABIAS'})
        return res

    @local_cache_maintainer(cache_project='jqdata', cache_type='tec_analysis')
    def get_CCI(self, check_date=CHECK_DATE, N=14, unit='1d'):
        security_list = list(BasicRepo().get_all_securities('stocks', check_date=check_date).index[:])
        res = jqd.technical_analysis.CCI(security_list, check_date, N=N, unit=unit, include_now=True)
        res = pd.DataFrame(pd.Series(res)).rename(columns={0: 'CCI'})
        return res

    @local_cache_maintainer(cache_project='jqdata', cache_type='tec_analysis')
    def get_CYF(self, check_date=CHECK_DATE, N=21, unit='1d'):
        security_list = list(BasicRepo().get_all_securities('stocks', check_date=check_date).index[:])
        res = jqd.technical_analysis.CYF(security_list, check_date, N=N, unit=unit, include_now=True)
        res = pd.DataFrame(pd.Series(res)).rename(columns={0: 'CYF'})
        return res

    @local_cache_maintainer(cache_project='jqdata', cache_type='tec_analysis')
    def get_DKX(self, check_date=CHECK_DATE, M=10, unit='1d'):
        security_list = list(BasicRepo().get_all_securities('stocks', check_date=check_date).index[:])
        res = jqd.technical_analysis.DKX(security_list, check_date, M=M, unit=unit, include_now=True)
        res = pd.DataFrame(res).T.rename(columns={0: 'DKX', 1: 'MADKX'})
        return res

    @local_cache_maintainer(cache_project='jqdata', cache_type='tec_analysis')
    def get_KD(self, check_date=CHECK_DATE, N=9, M1=3, M2=3, unit='1d'):
        security_list = list(BasicRepo().get_all_securities('stocks', check_date=check_date).index[:])
        res = jqd.technical_analysis.KD(security_list, check_date, N=N, M1=M1, M2=M2, unit=unit, include_now=True)
        res = pd.DataFrame(res).T.rename(columns={0: 'K', 1: 'D'})
        return res

    @local_cache_maintainer(cache_project='jqdata', cache_type='tec_analysis')
    def get_KDJ(self, check_date=CHECK_DATE, N=9, M1=3, M2=3, unit='1d'):
        security_list = list(BasicRepo().get_all_securities('stocks', check_date=check_date).index[:])
        res = jqd.technical_analysis.KDJ(security_list, check_date, N=N, M1=M1, M2=M2, unit=unit, include_now=True)
        res = pd.DataFrame(res).T.rename(columns={0: 'K', 1: 'D', 2: 'J'})
        return res

    @local_cache_maintainer(cache_project='jqdata', cache_type='tec_analysis')
    def get_SKDJ(self, check_date=CHECK_DATE, N=9, M=3, unit='1d'):
        security_list = list(BasicRepo().get_all_securities('stocks', check_date=check_date).index[:])
        res = jqd.technical_analysis.SKDJ(security_list, check_date, N=N, M=M, unit=unit, include_now=True)
        res = pd.DataFrame(res).T.rename(columns={0: 'K', 1: 'D'})
        return res

    @local_cache_maintainer(cache_project='jqdata', cache_type='tec_analysis')
    def get_MFI(self, check_date=CHECK_DATE, timeperiod=14, unit='1d', ):
        security_list = list(BasicRepo().get_all_securities('stocks', check_date=check_date).index[:])
        res = jqd.technical_analysis.MFI(security_list, check_date, timeperiod=timeperiod, unit=unit, include_now=True)
        res = pd.DataFrame(pd.Series(res)).rename(columns={0: 'MFI'})
        return res

    @local_cache_maintainer(cache_project='jqdata', cache_type='tec_analysis')
    def get_MTM(self, check_date=CHECK_DATE, timeperiod=14, unit='1d'):
        security_list = list(BasicRepo().get_all_securities('stocks', check_date=check_date).index[:])
        res = jqd.technical_analysis.MTM(security_list, check_date, timeperiod=timeperiod, unit=unit, include_now=True)
        res = pd.DataFrame(pd.Series(res)).rename(columns={0: 'MTM'})
        return res

    @local_cache_maintainer(cache_project='jqdata', cache_type='tec_analysis')
    def get_ROC(self, check_date=CHECK_DATE, timeperiod=12, unit='1d'):
        security_list = list(BasicRepo().get_all_securities('stocks', check_date=check_date).index[:])
        res = jqd.technical_analysis.ROC(security_list, check_date, timeperiod=timeperiod, unit=unit, include_now=True)
        res = pd.DataFrame(pd.Series(res)).rename(columns={0: 'ROC'})
        return res

    @local_cache_maintainer(cache_project='jqdata', cache_type='tec_analysis')
    def get_RSI(self, check_date=CHECK_DATE, N1=6, unit='1d'):
        security_list = list(BasicRepo().get_all_securities('stocks', check_date=check_date).index[:])
        res = jqd.technical_analysis.RSI(security_list, check_date, N1=N1, unit=unit, include_now=True)
        res = pd.DataFrame(pd.Series(res)).rename(columns={0: 'RSI'})
        return res

    @local_cache_maintainer(cache_project='jqdata', cache_type='tec_analysis')
    def get_MARSI(self, check_date=CHECK_DATE, M1=10, M2=6, unit='1d'):
        security_list = list(BasicRepo().get_all_securities('stocks', check_date=check_date).index[:])
        res = jqd.technical_analysis.MARSI(security_list, check_date, M1=M1, M2=M2, unit=unit, include_now=True)
        res = pd.DataFrame(res).T.rename(columns={0: 'RSI10', 1: 'RSI6'})
        return res

    @local_cache_maintainer(cache_project='jqdata', cache_type='tec_analysis')
    def get_OSC(self, check_date=CHECK_DATE, N=20, M=6, unit='1d'):
        security_list = list(BasicRepo().get_all_securities('stocks', check_date=check_date).index[:])
        res = jqd.technical_analysis.OSC(security_list, check_date, N=N, M=M, unit=unit, include_now=True)
        res = pd.DataFrame(res).T.rename(columns={0: 'OSC', 1: 'MAOSC'})
        return res

    @local_cache_maintainer(cache_project='jqdata', cache_type='tec_analysis')
    def get_UDL(self, check_date=CHECK_DATE, N1=3, N2=5, N3=10, N4=20, M=6, unit='1d'):
        security_list = list(BasicRepo().get_all_securities('stocks', check_date=check_date).index[:])
        res = jqd.technical_analysis.UDL(security_list, check_date, N1=N1, N2=N2, N3=N3, N4=N4, M=M, unit=unit, include_now=True)
        res = pd.DataFrame(res).T.rename(columns={0: 'UDL', 1: 'MAUDL'})
        return res

    @local_cache_maintainer(cache_project='jqdata', cache_type='tec_analysis')
    def get_WR(self, check_date=CHECK_DATE, N=10, N1=6, unit='1d'):
        security_list = list(BasicRepo().get_all_securities('stocks', check_date=check_date).index[:])
        res = jqd.technical_analysis.WR(security_list, check_date, N=N, N1=N1, unit=unit, include_now=True)
        res = pd.DataFrame(res).T.rename(columns={0: 'WR', 1: 'MAWR'})
        return res

    @local_cache_maintainer(cache_project='jqdata', cache_type='tec_analysis')
    def get_LWR(self, check_date=CHECK_DATE, N=9, M1=3, M2=3, unit='1d'):
        security_list = list(BasicRepo().get_all_securities('stocks', check_date=check_date).index[:])
        res = jqd.technical_analysis.LWR(security_list, check_date, N=N, M1=M1, M2=M2, unit=unit, include_now=True)
        res = pd.DataFrame(res).T.rename(columns={0: 'LWR1', 1: 'LWR2'})
        return res

    @local_cache_maintainer(cache_project='jqdata', cache_type='tec_analysis')
    def get_TAPI(self, index_s='399106.XSHE', check_date=CHECK_DATE, M=6, unit='1d'):
        security_list = list(BasicRepo().get_all_securities('stocks', check_date=check_date).index[:])
        res = jqd.technical_analysis.TAPI(index_s, security_list, check_date, M=M, unit=unit, include_now=True)
        res = pd.DataFrame(res).T.rename(columns={0: 'TAPI', 1: 'MATAPI'})
        return res

    @local_cache_maintainer(cache_project='jqdata', cache_type='tec_analysis')
    def get_FSL(self, check_date=CHECK_DATE):
        security_list = list(BasicRepo().get_all_securities('stocks', check_date=check_date).index[:])
        res = jqd.technical_analysis.FSL(security_list, check_date)
        res = pd.DataFrame(res).T.rename(columns={0: 'SWL', 1: 'SWS'})
        return res



if __name__ == '__main__':
    # import os
    #
    # cache_file_path = '/Users/apple/PycharmProjects/jiushen/cache/jqdata/2020-12-01/tec_analysis/get_trends_ACCER-.gzip.pickle'
    # if os.path.isfile(cache_file_path):
    #     x = pd.read_pickle(cache_file_path, compression="gzip")


    days = BasicRepo().get_trade_days('2021-12-01','2021-12-09')
    for i in days:
        try:
            print(i)
            print(jqd.get_query_count())
            all_securities_df = BasicRepo().get_all_securities('stocks', check_date=i)
            code_list = list(all_securities_df.index[:])
            res = OverboughtOversold().get_ACCER(check_date=i)
            res2 = OverboughtOversold().get_ADTM(check_date=i)
            res3 = OverboughtOversold().get_ATR(check_date=i)
            res4 = OverboughtOversold().get_BIAS(check_date=i)
            res5 = OverboughtOversold().get_BIAS_QL(check_date=i)
            res6 = OverboughtOversold().get_BIAS_36(check_date=i)
            res7 = OverboughtOversold().get_CCI(check_date=i)
            res8 = OverboughtOversold().get_CYF(check_date=i)
            res9 = OverboughtOversold().get_DKX(check_date=i)
            res10 = OverboughtOversold().get_KD(check_date=i)
            res11 = OverboughtOversold().get_KDJ(check_date=i)
            res12 = OverboughtOversold().get_SKDJ(check_date=i)
            res13 = OverboughtOversold().get_MFI(check_date=i)
            res14 = OverboughtOversold().get_MTM(check_date=i)
            res15 = OverboughtOversold().get_ROC(check_date=i)
            res16 = OverboughtOversold().get_RSI(check_date=i)
            res17 = OverboughtOversold().get_MARSI(check_date=i)
            res18 = OverboughtOversold().get_OSC(check_date=i)
            res19 = OverboughtOversold().get_UDL(check_date=i)
            res20 = OverboughtOversold().get_WR(check_date=i)
            res21 = OverboughtOversold().get_LWR(check_date=i)
            res22 = OverboughtOversold().get_TAPI(check_date=i)
            res23 = OverboughtOversold().get_FSL(check_date=i)
        except:
            pass
    ...
