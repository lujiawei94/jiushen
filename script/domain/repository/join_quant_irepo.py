import json

from script.infrastructure.repository.join_quant_repo.basic_repo import BasicRepo
from script.infrastructure.repository.join_quant_repo.technical_analysis_repo.energy import Energy
from script.infrastructure.repository.join_quant_repo.technical_analysis_repo.overbought_and_oversold import \
    OverboughtOversold
import jqdatasdk as jqd
import pandas as pd

from script.infrastructure.repository.join_quant_repo.technical_analysis_repo.volume import Volume

OVERBS_TECH_FACTORS = ['ACCER', 'ADTM', 'ATR', 'BIAS', 'BIAS_QL', 'BIAS_36', 'CCI', 'CYF', 'DKX', 'KD', 'KDJ', 'SKDJ',
                       'MFI', 'MTM', 'ROC', 'RSI', 'MARSI', 'OSC', 'UDL', 'WR', 'LWR', 'TAPI', 'FSL']

ENERGY_TECH_FACTORS = ['BRAR', 'CR', 'CYR', 'MASS', 'PCNT', 'PSY', 'VR']

VOLUME_TECH_FACTORS = ['AMO', 'DBLB', 'DBQRV', 'HSL', 'OBV', 'VOL', 'VRSI', 'LB']

TRENDS_TECH_FACTORS = ['GDX', 'CHO', 'CYE', 'DBQR', 'DMA', 'DMI', 'DPO', 'EMV', 'JLHB', 'JS', 'MACD', 'QACD', 'QR',
                       'TRIX', 'UOS', 'VMACD', 'VPT', 'WVAD']

class JoinQuantIrepo:


    @staticmethod
    def get_overbought_and_oversold_indicators(target_code_list, check_date=None):
        df_res = pd.DataFrame()
        for factor in OVERBS_TECH_FACTORS:
            df = OverboughtOversold().__getattribute__(f'get_{factor}')(check_date=check_date)
            df.rename(columns={old_name: f'{factor}_' + old_name for old_name in df.columns}, inplace=True)
            df_res = pd.concat([df_res, df], axis=1)
        return df_res.loc[df_res.index.isin(jqd.normalize_code(target_code_list))]

    @staticmethod
    def get_energy_indicators(target_code_list, check_date=None):
        df_res = pd.DataFrame()
        for factor in ENERGY_TECH_FACTORS:
            df = Energy().__getattribute__(f'get_{factor}')(check_date=check_date)
            df.rename(columns={old_name: f'{factor}_' + old_name for old_name in df.columns}, inplace=True)
            df_res = pd.concat([df_res, df], axis=1)
        return df_res.loc[df_res.index.isin(jqd.normalize_code(target_code_list))]

    @staticmethod
    def get_volume_indicators(target_code_list, check_date=None):
        df_res = pd.DataFrame()
        for factor in VOLUME_TECH_FACTORS:
            df = Volume().__getattribute__(f'get_{factor}')(check_date=check_date)
            df.rename(columns={old_name: f'{factor}_' + old_name for old_name in df.columns}, inplace=True)
            df_res = pd.concat([df_res, df], axis=1)
        return df_res.loc[df_res.index.isin(jqd.normalize_code(target_code_list))]

    @staticmethod
    def get_trends_indicators(target_code_list, check_date=None):
        df_res = pd.DataFrame()
        for factor in TRENDS_TECH_FACTORS:
            df = Volume().__getattribute__(f'get_{factor}')(check_date=check_date)
            df.rename(columns={old_name: f'{factor}_' + old_name for old_name in df.columns}, inplace=True)
            df_res = pd.concat([df_res, df], axis=1)
        return df_res.loc[df_res.index.isin(jqd.normalize_code(target_code_list))]

    @staticmethod
    def get_jq_factor_value_dic(start, end, json_factors_key='focused_factors', json_index_codes_key='hs300'):
        with open('json/self_index.json', 'r') as ifile:
            index_code = json.load(ifile)[json_index_codes_key]
        with open('json/self_factors.json', 'r') as ffile:
            factors = []
            for category, factor_lst in json.load(ffile)[json_factors_key].items():
                factors.extend(factor_lst)
        res_dic = {}
        for factor in factors:
            df = BasicRepo().get_jq_specific_factor_value_for_index_stocks(factor, start_date=start, end_date=end, code=index_code)
            res_dic[factor] = df
        return res_dic

    @staticmethod
    def get_overbought_oversold_object(code_id):
        return OverboughtOversold()._aggregate_root_storage.get(code_id)

if __name__ == '__main__':
    # res1 = JoinQuantIrepo.get_overbought_and_oversold_indicators(['601919', '601899', '601020', '600737', '002309'], check_date='2021-03-25')
    # res2 = JoinQuantIrepo.get_energy_indicators(['601919', '601899', '601020', '600737', '002309'], check_date='2021-03-25')
    # res3 = JoinQuantIrepo.get_volume_indicators(['601919', '601899', '601020', '600737', '002309'], check_date='2021-03-25')

    ...
