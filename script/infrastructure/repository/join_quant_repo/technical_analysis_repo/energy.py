import datetime
from script.infrastructure.repository.base.base_repo import BaseRepo
from script.infrastructure.utility.handler.local_cache_maintainer import local_cache_maintainer
from script.infrastructure.utility.handler.singleton import Singleton
from script.infrastructure.repository.join_quant_repo.basic_repo import BasicRepo
import pandas as pd
import jqdatasdk as jqd

jqd.auth('13401689393', 'Lujiaweizuishuai001')
print(jqd.get_query_count())
CHECK_DATE = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d')


@Singleton
class Energy(BaseRepo):
    _aggregate_root_storage = {}

    @local_cache_maintainer(cache_project='jqdata', cache_type='tec_analysis')
    def get_BRAR(self, check_date=CHECK_DATE, N=26, unit='1d'):
        security_list = list(BasicRepo().get_all_securities('stocks', check_date=check_date).index[:])
        res = jqd.technical_analysis.BRAR(security_list, check_date, N=N, unit=unit, include_now=True)
        res = pd.DataFrame(res).T.rename(columns={0: 'BR', 1: 'AR'})
        return res

    @local_cache_maintainer(cache_project='jqdata', cache_type='tec_analysis')
    def get_CR(self, check_date=CHECK_DATE, N=26, M1=10, M2=20, M3=40, M4=62, unit='1d'):
        security_list = list(BasicRepo().get_all_securities('stocks', check_date=check_date).index[:])
        res = jqd.technical_analysis.CR(security_list, check_date, N=N, M1=M1, M2=M2, M3=M3, M4=M4, unit=unit, include_now=True)
        res = pd.DataFrame(res).T.rename(columns={0: 'CR1', 1: 'MA1', 2: 'MA2', 3: 'MA3', 4: 'MA4'})
        return res

    @local_cache_maintainer(cache_project='jqdata', cache_type='tec_analysis')
    def get_CYR(self, check_date=CHECK_DATE, N=13, M=5, unit='1d'):
        security_list = list(BasicRepo().get_all_securities('stocks', check_date=check_date).index[:])
        res = jqd.technical_analysis.CYR(security_list, check_date, N=N, M=M, unit=unit, include_now=True)
        res = pd.DataFrame(res).T.rename(columns={0: 'CYR1', 1: 'MACYR1'})
        return res

    @local_cache_maintainer(cache_project='jqdata', cache_type='tec_analysis')
    def get_MASS(self, check_date=CHECK_DATE, N1=9, N2=25, M=6, unit='1d'):
        security_list = list(BasicRepo().get_all_securities('stocks', check_date=check_date).index[:])
        res = jqd.technical_analysis.MASS(security_list, check_date, N1=N1, N2=N2, M=M, unit=unit, include_now=True)
        res = pd.DataFrame(res).T.rename(columns={0: 'MASS', 1: 'MAMASS'})
        return res

    @local_cache_maintainer(cache_project='jqdata', cache_type='tec_analysis')
    def get_PCNT(self,check_date=CHECK_DATE, M=5, unit='1d', ):
        security_list = list(BasicRepo().get_all_securities('stocks', check_date=check_date).index[:])
        res = jqd.technical_analysis.PCNT(security_list, check_date, M=M, unit=unit, include_now=True)
        res = pd.DataFrame(res).T.rename(columns={0: 'PCNT1', 1: 'MAPCNT1'})
        return res

    @local_cache_maintainer(cache_project='jqdata', cache_type='tec_analysis')
    def get_PSY(self, check_date=CHECK_DATE, timeperiod=14, unit='1d'):
        security_list = list(BasicRepo().get_all_securities('stocks', check_date=check_date).index[:])
        res = jqd.technical_analysis.PSY(security_list, check_date, timeperiod=timeperiod, unit=unit, include_now=True)
        res = pd.DataFrame(pd.Series(res)).rename(columns={0: 'PSY'})
        return res

    @local_cache_maintainer(cache_project='jqdata', cache_type='tec_analysis')
    def get_VR(self, check_date=CHECK_DATE, N=26, M=6, unit='1d'):
        security_list = list(BasicRepo().get_all_securities('stocks', check_date=check_date).index[:])
        res = jqd.technical_analysis.VR(security_list, check_date, N=N, M=M, unit=unit, include_now=True)
        res = pd.DataFrame(res).T.rename(columns={0: 'VR', 1: 'MAVR'})
        return res



if __name__ == '__main__':
    from script.infrastructure.repository.join_quant_repo.basic_repo import BasicRepo
    days = BasicRepo().get_trade_days('2021-12-01','2021-12-09')
    for i in days:
        try:
            print(i)
            print(jqd.get_query_count())

            res = Energy().get_BRAR(check_date=i)
            res2 = Energy().get_CR(check_date=i)
            res3 = Energy().get_CYR(check_date=i)
            res4 = Energy().get_MASS(check_date=i)
            res5 = Energy().get_PCNT(check_date=i)
            res6 = Energy().get_PSY(check_date=i)
            res7 = Energy().get_VR(check_date=i)
        except:
            pass
    ...
