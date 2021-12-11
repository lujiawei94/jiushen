import shutil
import time
from functools import wraps
import os
import datetime
import re

import pandas as pd
from loguru import logger

CACHE_PATH = '/Users/apple/PycharmProjects/jiushen/cache'
EXPIRY_DAYS = 3


def local_cache_maintainer(cache_project=None, cache_type=None):

    def maintain_cache(func):
        @wraps(func)
        def wrapper(*args, **kwargs):

            cache_conf = {
                "lhb": {
                    'recent_data': _check_date,
                    'lhb_history': _check_date,
                    'indicators': _check_date},
                "stock_quote": {
                    'stock_history': _stock_history_range,
                    'current_closing':_current_date},
                "jqdata":{
                    "current_data": _current_date,
                    "current_factor": _current_date,
                    "basic_data": _check_date,
                    "tec_analysis": _check_date,
                    "static_data": _static_data,
                    'stock_history_factor': _stock_history_range,
                }
            }

            cache_root = cache_conf[cache_project][cache_type](cache_project=cache_project,
                                                               cache_type=cache_type,
                                                               para_dic=kwargs,
                                                               raw_func=func)
            cache_file_name = f"""{func.__name__}-{'_'.join([str(i) for i in args[1:]])}"""
            # range history
            if isinstance(cache_root, list):
                # 先交集再 根据start end date求子集
                pd_res = pd.DataFrame()
                raw_start, raw_end = kwargs['start_date'],kwargs['end_date']
                for single_cache_root in cache_root:
                    # _run_func(func, args, kwargs, pd_res, single_cache_root, cache_file_name)
                    cache_file_path = f'{single_cache_root}/{cache_file_name}.gzip.pickle'
                    if os.path.isfile(cache_file_path):
                        logger.debug("######### read from cache #########", cache_file_path)
                        df_exist = pd.read_pickle(cache_file_path, compression="gzip")
                        pd_res = pd.concat([pd_res, df_exist])
                    else:
                        target_start, target_end = [date for date in
                                                    re.findall('_between_(\d*)_and_(\d*)', cache_file_path)[0]]

                        kwargs['start_date'] = datetime.datetime.strftime(
                            datetime.datetime.strptime(target_start, '%Y%m%d'), '%Y-%m-%d')
                        kwargs['end_date'] = datetime.datetime.strftime(
                            datetime.datetime.strptime(target_end, '%Y%m%d'), '%Y-%m-%d')
                        res = func(*args, **kwargs)

                        if isinstance(res, pd.DataFrame) or isinstance(res, pd.Series):
                            if res.empty:
                                logger.warning(f"{target_start} ~ {target_end}，ak无数据可获取")
                                continue
                            actual_start, actual_end = sorted(
                                [date.replace('-', '') for date in
                                 re.findall('\d{4}-\d{2}-\d{2}', str(res.index[0]) + str(res.index[-1]))])
                            actual_cache_root = single_cache_root.replace(target_start, actual_start).replace(
                                target_end, actual_end)
                            cache_file_path = f'{actual_cache_root}/{cache_file_name}.gzip.pickle'
                            if not os.path.exists(actual_cache_root):
                                os.makedirs(actual_cache_root)
                            res.to_pickle(cache_file_path, compression="gzip")
                            pd_res = pd.concat([pd_res, res])

                            #
                            dif_date_range = sorted(
                                list(set(date_range(target_start, target_end)) - set(
                                    date_range(actual_start, actual_end))))
                            if not dif_date_range:
                                continue
                            dif_start, dif_end = dif_date_range[0], dif_date_range[-1]
                            logger.warning(f"{dif_start} ~ {dif_end}，ak无数据可获取")
                            # next_single_cache_root = single_cache_root.replace(target_start, dif_start).replace(target_end, dif_end)
                            # _run_func(func, args, kwargs, pd_res, next_single_cache_root, cache_file_name)
                        else:
                            logger.warning('非dateframe或series，to_pickle 异常！')

                pd_res = pd_res.loc[lambda x: (x.index <= raw_end) & (x.index >= raw_start)]
                pd_res = pd_res.sort_index()
                return pd_res
            else:
                if not os.path.exists(cache_root):
                    os.makedirs(cache_root)
                cache_file_path = f'{cache_root}/{cache_file_name}.gzip.pickle'
                if os.path.isfile(cache_file_path):
                    logger.debug("######### read from cache #########", cache_file_path)
                    return pd.read_pickle(cache_file_path, compression="gzip")
                else:
                    res = func(*args, **kwargs)
                    _inspect(res, cache_file_path)
                    return res

        return wrapper

    return maintain_cache


def _run_func(func, args, kwargs, pd_res, single_cache_root, cache_file_name):
    cache_file_path = f'{single_cache_root}/{cache_file_name}.gzip.pickle'
    if os.path.isfile(cache_file_path):
        logger.debug("######### read from cache #########", cache_file_path)
        df_exist = pd.read_pickle(cache_file_path, compression="gzip")
        pd_res = pd.concat([pd_res, df_exist])
    else:
        target_start, target_end = [date for date in re.findall('_between_(\d*)_and_(\d*)', cache_file_path)[0]]

        kwargs['start_date'] = datetime.datetime.strftime(datetime.datetime.strptime(target_start, '%Y%m%d'), '%Y-%m-%d')
        kwargs['end_date'] = datetime.datetime.strftime(datetime.datetime.strptime(target_end, '%Y%m%d'), '%Y-%m-%d')
        res = func(*args, **kwargs)

        if isinstance(res, pd.DataFrame) or isinstance(res, pd.Series):
            if res.empty:
                logger.warning(f"{target_start} ~ {target_end}，ak无数据可获取")
                return
            actual_start, actual_end = sorted(
                [date.replace('-', '') for date in re.findall('\d{4}-\d{2}-\d{2}', res.index[0] + res.index[-1])])
            actual_cache_root = single_cache_root.replace(target_start, actual_start).replace(target_end, actual_end)
            cache_file_path = f'{actual_cache_root}/{cache_file_name}.gzip.pickle'
            if not os.path.exists(actual_cache_root):
                os.makedirs(actual_cache_root)
            res.to_pickle(cache_file_path, compression="gzip")
            pd_res = pd.concat([pd_res, res])

            #
            dif_date_range = sorted(
                list(set(date_range(target_start, target_end)) - set(date_range(actual_start, actual_end))))
            if not dif_date_range:
                return
            dif_start, dif_end = dif_date_range[0], dif_date_range[-1]
            logger.warning(f"{dif_start} ~ {dif_end}，ak无数据可获取")
            # next_single_cache_root = single_cache_root.replace(target_start, dif_start).replace(target_end, dif_end)
            # _run_func(func, args, kwargs, pd_res, next_single_cache_root, cache_file_name)
        else:
            logger.warning('非dateframe或series，to_pickle 异常！')


def _inspect(res,cache_file_path):
    if isinstance(res, pd.DataFrame) or isinstance(res, pd.Series):
        res.to_pickle(cache_file_path, compression="gzip")
    else:
        logger.warning('非dateframe或series，to_pickle 异常！')


def _static_data(**kwargs):
    cache_project = kwargs.get('cache_project')
    cache_type = kwargs.get('cache_type')
    topic_path = f"{cache_type}"

    if not os.path.exists(cache_root := f'{CACHE_PATH}/{cache_project}/{topic_path}/'):
        os.makedirs(cache_root)
    return cache_root


def _current_date(**kwargs):
    cache_project = kwargs.get('cache_project')
    cache_type = kwargs.get('cache_type')
    topic_path = f"{datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d')}/{cache_type}"

    if not os.path.exists(cache_root := f'{CACHE_PATH}/{cache_project}/{topic_path}/'):
        os.makedirs(cache_root)
    return cache_root

def _check_date(**kwargs):
    para_dic = kwargs.get('para_dic')
    cache_project = kwargs.get('cache_project')
    cache_type = kwargs.get('cache_type')
    check_date = para_dic.get('check_date')
    if not check_date:
        for i in kwargs.get('raw_func').__defaults__:
            if re.findall('\d{4}-\d{2}-\d{2}', str(i)):
                check_date = i
    topic_path = f"{check_date}/{cache_type}"

    if not os.path.exists(cache_root := f'{CACHE_PATH}/{cache_project}/{topic_path}/'):
        os.makedirs(cache_root)
    return cache_root

def _stock_history_range(**kwargs):
    para_dic = kwargs.get('para_dic')
    cache_project = kwargs.get('cache_project')
    cache_type = kwargs.get('cache_type')
    code = para_dic.get('code')
    period = para_dic.get('period') if para_dic.get('period') else ''
    end = para_dic.get('end_date').replace('-','')
    start = para_dic.get('start_date').replace('-','')


    cache_root_existed = []
    targeted_range = set(date_range(start, end))
    existed_range = []
    if not os.path.exists(code_path:=f'{CACHE_PATH}/{cache_project}/{cache_type}/{code}/'):
        os.makedirs(code_path)
    for string in os.listdir(code_path):
        if string.startswith(f'{period}') and os.listdir(code_path+string):
            _start, _end = [i for i in re.findall('_(\d+)', string)]
            cache_root_existed.append(f"{CACHE_PATH}/{cache_project}/{cache_type}/{code}/{period}_between_{_start}_and_{_end}")
            existed_range.extend(date_range(_start, _end))

    existed_range = set(existed_range)
    dif_range = sorted(list(targeted_range - existed_range))

    dif_res = set()
    start_date, end_date = None, None
    for date in dif_range:
        if not start_date:
            start_date = date
        if end_date and (datetime.datetime.strptime(date, '%Y%m%d') - datetime.datetime.strptime(end_date, '%Y%m%d')) > datetime.timedelta(days=10):
            dif_res.add((start_date, end_date))
            start_date = date
        end_date = date
    else:
        if end_date and (datetime.datetime.strptime(end_date, '%Y%m%d') - datetime.datetime.strptime(start_date, '%Y%m%d')) > datetime.timedelta(days=3):
            dif_res.add((start_date, end_date)) if start_date and end_date else ...

    cache_root_lst = cache_root_existed
    for dif_range in dif_res:
        cache_root = f"{CACHE_PATH}/{cache_project}/{cache_type}/{code}/{period}_between_{dif_range[0]}_and_{dif_range[1]}"
        cache_root_lst.append(cache_root)
        # if not os.path.exists(cache_root):
        #     os.makedirs(cache_root)

    return cache_root_lst


def date_range(beginDate, endDate):
    dates = []
    dt = datetime.datetime.strptime(beginDate, "%Y%m%d")
    date = beginDate[:]
    while date <= endDate:
        dates.append(date)
        dt = dt + datetime.timedelta(1)
        date = dt.strftime("%Y%m%d")
    return dates

