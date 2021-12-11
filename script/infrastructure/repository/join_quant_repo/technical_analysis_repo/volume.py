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
class Volume(BaseRepo):
    _aggregate_root_storage = {}

    @local_cache_maintainer(cache_project='jqdata', cache_type='tec_analysis')
    def get_AMO(self, check_date=CHECK_DATE, M1=5, M2=10, unit='1d'):
        security_list = list(BasicRepo().get_all_securities('stocks', check_date=check_date).index[:])
        res = jqd.technical_analysis.AMO(security_list, check_date, M1=M1, M2=M2, unit=unit, include_now=True)
        res = pd.DataFrame(res).T.rename(columns={0: 'AMOW', 1: 'AMO1', 2: 'AMO2'})
        return res

    @local_cache_maintainer(cache_project='jqdata', cache_type='tec_analysis')
    def get_DBLB(self, index_stock='399106.XSHE', check_date=CHECK_DATE, N=5, M=5, unit='1d'):
        security_list = list(BasicRepo().get_all_securities('stocks', check_date=check_date).index[:])
        res = jqd.technical_analysis.DBLB(index_stock, security_list, check_date, N=N, M=M, unit=unit, include_now=True)
        res = pd.DataFrame(res).T.rename(columns={0: 'DBLB', 1: 'MADBLB'})
        return res

    @local_cache_maintainer(cache_project='jqdata', cache_type='tec_analysis')
    def get_DBQRV(self, index_stock='399106.XSHE', check_date=CHECK_DATE, N=5, unit='1d'):
        security_list = list(BasicRepo().get_all_securities('stocks', check_date=check_date).index[:])
        res = jqd.technical_analysis.DBQRV(index_stock, security_list, check_date, N=N, unit=unit, include_now=True)
        res = pd.DataFrame(res).T.rename(columns={0: 'ZS', 1: 'GG'})
        return res

    @local_cache_maintainer(cache_project='jqdata', cache_type='tec_analysis')
    def get_HSL(self, check_date=CHECK_DATE, N=5, unit='1d'):
        security_list = list(BasicRepo().get_all_securities('stocks', check_date=check_date).index[:])
        res = jqd.technical_analysis.HSL(security_list, check_date, N=N, unit=unit, include_now=True)
        res = pd.DataFrame(res).T.rename(columns={0: 'HSL', 1: 'MAHSL'})
        return res

    @local_cache_maintainer(cache_project='jqdata', cache_type='tec_analysis')
    def get_OBV(self, check_date=CHECK_DATE, timeperiod=30, unit='1d'):
        security_list = list(BasicRepo().get_all_securities('stocks', check_date=check_date).index[:])
        res = jqd.technical_analysis.OBV(security_list, check_date, timeperiod=timeperiod, unit=unit, include_now=True)
        res = pd.DataFrame(pd.Series(res)).rename(columns={0: 'OBV'})
        return res

    @local_cache_maintainer(cache_project='jqdata', cache_type='tec_analysis')
    def get_VOL(self, check_date=CHECK_DATE, M1=5, M2=10, unit='1d'):
        security_list = list(BasicRepo().get_all_securities('stocks', check_date=check_date).index[:])
        res = jqd.technical_analysis.VOL(security_list, check_date, M1=M1, M2=M2, unit=unit, include_now=True)
        res = pd.DataFrame(res).T.rename(columns={0: 'VOL', 1: 'MAVOL1', 2: 'MAVOL2'})
        return res

    @local_cache_maintainer(cache_project='jqdata', cache_type='tec_analysis')
    def get_VRSI(self, check_date=CHECK_DATE, N1=6, N2=12, N3=24, unit='1d'):
        security_list = list(BasicRepo().get_all_securities('stocks', check_date=check_date).index[:])
        res = jqd.technical_analysis.VRSI(security_list, check_date, N1=N1, N2=N2, N3=N3, unit=unit, include_now=True)
        res = pd.DataFrame(res).T.rename(columns={0: 'VRSI1', 1: 'VRSI2', 2: 'VRSI3'})
        return res

    @local_cache_maintainer(cache_project='jqdata', cache_type='tec_analysis')
    def get_LB(self, check_date=CHECK_DATE, N=5):
        security_list = list(BasicRepo().get_all_securities('stocks', check_date=check_date).index[:])
        res = jqd.technical_analysis.LB(security_list, check_date, N=N)
        res = pd.DataFrame(pd.Series(res)).rename(columns={0: 'lb'})
        return res



if __name__ == '__main__':
    from script.infrastructure.repository.join_quant_repo.basic_repo import BasicRepo
    days = BasicRepo().get_trade_days('2021-12-01','2021-12-09')
    for i in days:
        try:
            print(i)
            print(jqd.get_query_count())

            res = Volume().get_AMO(check_date=i)
            res2 = Volume().get_DBLB(check_date=i)
            res3 = Volume().get_DBQRV(check_date=i)
            res4 = Volume().get_HSL(check_date=i)
            res5 = Volume().get_OBV(check_date=i)
            res6 = Volume().get_VOL(check_date=i)
            res7 = Volume().get_VRSI(check_date=i)
            res8 = Volume().get_LB(check_date=i)
        except:
            pass
    ...
