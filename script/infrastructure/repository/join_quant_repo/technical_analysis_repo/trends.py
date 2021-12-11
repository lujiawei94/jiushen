import datetime
from script.infrastructure.repository.base.base_repo import BaseRepo
from script.infrastructure.repository.join_quant_repo.basic_repo import BasicRepo
from script.infrastructure.utility.handler.local_cache_maintainer import local_cache_maintainer
from script.infrastructure.utility.handler.singleton import Singleton
import pandas as pd
import jqdatasdk as jqd


jqd.auth('13401689393', 'Lujiaweizuishuai001')
# jqd.auth('18818217095', 'tanC1234')
print(jqd.get_query_count())
CHECK_DATE = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d')


@Singleton
class Trends(BaseRepo):
    _aggregate_root_storage = {}

    @local_cache_maintainer(cache_project='jqdata', cache_type='tec_analysis')
    def get_CHO(self,  check_date=CHECK_DATE,  N1=10, N2=20, M=6, unit='1d'):
        security_list = list(BasicRepo().get_all_securities('stocks', check_date=check_date).index[:])
        res = jqd.technical_analysis.CHO(security_list, check_date, N1=N1, N2=N2, M=M, unit=unit, include_now=True)
        res = pd.DataFrame(res).T.rename(columns={0: 'CHO', 1: 'MACHO'})
        return res

    @local_cache_maintainer(cache_project='jqdata', cache_type='tec_analysis')
    def get_CYE(self, check_date=CHECK_DATE,  unit='1d'):
        security_list = list(BasicRepo().get_all_securities('stocks', check_date=check_date).index[:])
        res = jqd.technical_analysis.CYE(security_list, check_date, unit=unit, include_now=True)
        res = pd.DataFrame(res).T.rename(columns={0: 'CYEL', 1: 'CYES'})
        return res

    @local_cache_maintainer(cache_project='jqdata', cache_type='tec_analysis')
    def get_DBQR(self, index_stock='000001.XSHG', check_date=CHECK_DATE, N=5, M1=10, M2=20, M3=60, unit='1d'):
        security_list = list(BasicRepo().get_all_securities('stocks', check_date=check_date).index[:])
        res = jqd.technical_analysis.DBQR(index_stock, security_list, check_date, N=N, M1=M1, M2=M2, M3=M3, unit=unit, include_now=True)
        res = pd.DataFrame(res).T.rename(columns={0: 'zs', 1: 'gg',2: 'madbqr1', 3: 'madbqr2',4: 'madbqr3'})
        return res

    @local_cache_maintainer(cache_project='jqdata', cache_type='tec_analysis')
    def get_DMA(self, check_date=CHECK_DATE,  N1=10, N2=50, M=10, unit='1d'):
        security_list = list(BasicRepo().get_all_securities('stocks', check_date=check_date).index[:])
        res = jqd.technical_analysis.DMA(security_list, check_date, N1=N1, N2=N2, M=M, unit=unit, include_now=True)
        res = pd.DataFrame(res).T.rename(columns={0: 'DIF', 1: 'DIFMA'})
        return res

    @local_cache_maintainer(cache_project='jqdata', cache_type='tec_analysis')
    def get_DMI(self, check_date=CHECK_DATE,  N=14, MM=6, unit='1d'):
        security_list = list(BasicRepo().get_all_securities('stocks', check_date=check_date).index[:])
        res = jqd.technical_analysis.DMI(security_list, check_date, N=N, MM=MM, unit=unit, include_now=True)
        res = pd.DataFrame(res).T.rename(columns={0: 'pdi', 1: 'mdi', 2: 'adx', 3: 'adxr'})
        return res

    @local_cache_maintainer(cache_project='jqdata', cache_type='tec_analysis')
    def get_DPO(self, check_date=CHECK_DATE,  N=20, M=6, unit='1d'):
        security_list = list(BasicRepo().get_all_securities('stocks', check_date=check_date).index[:])
        res = jqd.technical_analysis.DPO(security_list, check_date, N=N, M=M, unit=unit, include_now=True)
        res = pd.DataFrame(res).T.rename(columns={0: 'DPO', 1: 'MADPO'})
        return res

    @local_cache_maintainer(cache_project='jqdata', cache_type='tec_analysis')
    def get_EMV(self, check_date=CHECK_DATE,  N=14, M=9, unit='1d'):
        security_list = list(BasicRepo().get_all_securities('stocks', check_date=check_date).index[:])
        res = jqd.technical_analysis.EMV(security_list, check_date, N=N, M=M, unit=unit, include_now=True)
        res = pd.DataFrame(res).T.rename(columns={0: 'EMV', 1: 'MAEMV'})
        return res

    @local_cache_maintainer(cache_project='jqdata', cache_type='tec_analysis')
    def get_GDX(self, check_date=CHECK_DATE,  N=30, M=9, unit='1d'):
        security_list = list(BasicRepo().get_all_securities('stocks', check_date=check_date).index[:])
        res = jqd.technical_analysis.GDX(security_list, check_date, N=N, M=M, unit=unit, include_now=True)
        res = pd.DataFrame(res).T.rename(columns={0: 'jax', 1: 'ylx', 2: 'zcx'})
        return res

    @local_cache_maintainer(cache_project='jqdata', cache_type='tec_analysis')
    def get_JLHB(self, check_date=CHECK_DATE,  N=7, M=5, unit='1d'):
        security_list = list(BasicRepo().get_all_securities('stocks', check_date=check_date).index[:])
        res = jqd.technical_analysis.JLHB(security_list, check_date, N=N, M=M, unit=unit, include_now=True)
        res = pd.DataFrame(res).T.rename(columns={0: 'b', 1: 'var2', 2: 'jlhb'})
        return res

    @local_cache_maintainer(cache_project='jqdata', cache_type='tec_analysis')
    def get_JS(self, check_date=CHECK_DATE,  N=5, M1=5, M2=10, M3=20, unit='1d'):
        security_list = list(BasicRepo().get_all_securities('stocks', check_date=check_date).index[:])
        res = jqd.technical_analysis.JS(security_list, check_date, N=N, M1=M1, M2=M2, M3=M3, unit=unit, include_now=True)
        res = pd.DataFrame(res).T.rename(columns={0: 'jsx', 1: 'majsx1', 2: 'majsx2', 3: 'majsx3'})
        return res

    @local_cache_maintainer(cache_project='jqdata', cache_type='tec_analysis')
    def get_MACD(self, check_date=CHECK_DATE,  SHORT=12, LONG=26, MID=9, unit='1d'):
        security_list = list(BasicRepo().get_all_securities('stocks', check_date=check_date).index[:])
        res = jqd.technical_analysis.MACD(security_list, check_date, SHORT=SHORT, LONG=LONG, MID=MID, unit=unit, include_now=True)
        res = pd.DataFrame(res).T.rename(columns={0: 'dif', 1: 'dea', 2: 'macd'})
        return res

    @local_cache_maintainer(cache_project='jqdata', cache_type='tec_analysis')
    def get_QACD(self, check_date=CHECK_DATE,  N1=12, N2=26, M=9, unit='1d'):
        security_list = list(BasicRepo().get_all_securities('stocks', check_date=check_date).index[:])
        res = jqd.technical_analysis.QACD(security_list, check_date, N1=N1, N2=N2, M=M, unit=unit, include_now=True)
        res = pd.DataFrame(res).T.rename(columns={0: 'dif', 1: 'macd', 2: 'ddif'})
        return res

    @local_cache_maintainer(cache_project='jqdata', cache_type='tec_analysis')
    def get_QR(self, index_stock='000001.XSHE', check_date=CHECK_DATE,  N=21, unit='1d'):
        security_list = list(BasicRepo().get_all_securities('stocks', check_date=check_date).index[:])
        res = jqd.technical_analysis.QR(index_stock, security_list, check_date, N=N, unit=unit, include_now=True)
        res = pd.DataFrame(res).T.rename(columns={0: 'gg', 1: 'dp', 2: 'qrz'})
        return res

    @local_cache_maintainer(cache_project='jqdata', cache_type='tec_analysis')
    def get_TRIX(self, check_date=CHECK_DATE,  N=12, M=9, unit='1d'):
        security_list = list(BasicRepo().get_all_securities('stocks', check_date=check_date).index[:])
        res = jqd.technical_analysis.TRIX(security_list, check_date, N=N, M=M, unit=unit, include_now=True)
        res = pd.DataFrame(res).T.rename(columns={0: 'TRIX', 1: 'MATRIX'})
        return res

    @local_cache_maintainer(cache_project='jqdata', cache_type='tec_analysis')
    def get_UOS(self, check_date=CHECK_DATE,  N1=7, N2=14, N3=28, M=6, unit='1d'):
        security_list = list(BasicRepo().get_all_securities('stocks', check_date=check_date).index[:])
        res = jqd.technical_analysis.UOS(security_list, check_date, N1=N1, N2=N2, N3=N3, M=M, unit=unit, include_now=True)
        res = pd.DataFrame(res).T.rename(columns={0: 'ultiInc', 1: 'mauos'})
        return res

    @local_cache_maintainer(cache_project='jqdata', cache_type='tec_analysis')
    def get_VMACD(self, check_date=CHECK_DATE,  SHORT=12, LONG=26, MID=9, unit='1d', ):
        security_list = list(BasicRepo().get_all_securities('stocks', check_date=check_date).index[:])
        res = jqd.technical_analysis.VMACD(security_list, check_date, SHORT=SHORT, LONG=LONG, MID=MID, unit=unit, include_now=True)
        res = pd.DataFrame(res).T.rename(columns={0: 'DIF', 1: 'DEA', 2: 'MACD'})
        return res

    @local_cache_maintainer(cache_project='jqdata', cache_type='tec_analysis')
    def get_VPT(self, check_date=CHECK_DATE,  N=51, M=6, unit='1d'):
        security_list = list(BasicRepo().get_all_securities('stocks', check_date=check_date).index[:])
        res = jqd.technical_analysis.VPT(security_list, check_date, N=N, M=M, unit=unit, include_now=True)
        res = pd.DataFrame(res).T.rename(columns={0: 'VPT'})
        return res

    @local_cache_maintainer(cache_project='jqdata', cache_type='tec_analysis')
    def get_WVAD(self, check_date=CHECK_DATE,  N=24, M=6, unit='1d'):
        security_list = list(BasicRepo().get_all_securities('stocks', check_date=check_date).index[:])
        res = jqd.technical_analysis.WVAD(security_list, check_date, N=N, M=M, unit=unit, include_now=True)
        res = pd.DataFrame(res).T.rename(columns={0: 'wvad', 1: 'mawvad'})
        return res

    # @local_cache_maintainer(cache_project='jqdata', cache_type='tec_analysis')
    # def get_general(self, security_list=None, check_date=CHECK_DATE, factor=None):
    #     res = eval(f"jqd.technical_analysis.{factor}")(security_list, check_date)
    #     return res


if __name__ == '__main__':
    from script.infrastructure.repository.join_quant_repo.basic_repo import BasicRepo
    days = BasicRepo().get_trade_days('2020-04-30','2021-12-09')
    for i in days:
        try:
            print(i)
            print(jqd.get_query_count())
            # all_securities_df = BasicRepo().get_all_securities('stocks', check_date=i)
            # code_list = list(all_securities_df.index[:])

            res = Trends().get_GDX(check_date=i)
            res2 = Trends().get_CHO(check_date=i)
            res3 = Trends().get_CYE(check_date=i)
            res4 = Trends().get_DBQR(check_date=i)
            res5 = Trends().get_DMA(check_date=i)
            res6 = Trends().get_DMI(check_date=i)
            res7 = Trends().get_DPO(check_date=i)
            res8 = Trends().get_EMV(check_date=i)
            res9 = Trends().get_JLHB(check_date=i)
            res10 = Trends().get_JS(check_date=i)
            res11 = Trends().get_MACD(check_date=i)
            res12 = Trends().get_QACD(check_date=i)
            res13 = Trends().get_QR(check_date=i)
            res14 = Trends().get_TRIX(check_date=i)
            res15 = Trends().get_UOS(check_date=i)
            res16 = Trends().get_VMACD(check_date=i)
            res17 = Trends().get_VPT(check_date=i)
            res18 = Trends().get_WVAD(check_date=i)
        except:
            pass
    ...
