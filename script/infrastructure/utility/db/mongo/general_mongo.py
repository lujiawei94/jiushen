
from loguru import logger


def cetify_res_first(queryres, data_func_name):
    if not queryres:
        logger.warning(f'{data_func_name} mongo查询无结果！')
        return {}
    else:
        # logger.info(f'{data_func_name}: {queryres[0]}')
        return queryres[0]

def cetify_res_all(queryres, data_func_name):
    if not queryres:
        logger.warning(f'{data_func_name} mongo查询无结果！')
        return {}
    else:
        # logger.info(f'{data_func_name}: {queryres}')
        return queryres


#
# def _deep_search_mongo_data(mapped_field, mongo_data, search_dic):
#     for search_field, determine_field in search_dic.items():
#         if type(determine_field) == dict:
#             return _deep_search_mongo_data(mapped_field, mongo_data[search_field], determine_field)
#         elif determine_field not in mongo_data[search_field]:
#             logger.error(f'map中的{determine_field}，在数据源中缺失, {mapped_field}被置为None')
#             return None
#         else:
#             if mongo_data[search_field][determine_field] is None:
#                 logger.warning(f'map中的{determine_field},在数据源中为空值(None),特征逻辑可能未生效')
#             return mongo_data[search_field][determine_field]
#
#
# @logger.catch
# def get_mapped_data_for_public(mapped_data, mongo_data, map):
#     """
#
#     :param mapped_data: 被更新的字典
#     :param mongo_data: 数据源
#     :param map: 映射表
#     :return:
#     """
#     for mapped_field, mongo_field_list in map.items():
#         flag = 0
#         for mongo_field in mongo_field_list:
#             if type(mongo_field) == dict:
#                 mapped_data[mapped_field] = _deep_search_mongo_data(mapped_field, mongo_data, mongo_field)
#                 break
#             elif mongo_field not in mongo_data:
#                 flag += 1
#                 if flag == len(mongo_field_list):
#                     logger.error(f'map中的{mongo_field_list},在数据源中缺失，{mapped_field}被置为None')
#                     mapped_data[mapped_field] = None
#             else:
#                 mapped_data[mapped_field] = mongo_data[mongo_field]
#                 if mongo_data[mongo_field] is None:
#                     logger.warning(f'map中的{mongo_field},在数据源中为空值(None),特征逻辑可能未生效')
#                 break


def get_mapped_data_for_public(mapped_data, mongo_data, map):
    """

    :param mapped_data: 被更新的字典
    :param mongo_data: 数据源
    :param map: 映射表
    :return:
    """
    for mapped_field, mongo_field_list in map.items():
        flag = 0
        for mongo_field in mongo_field_list:
            if type(mongo_field) == dict:
                deep_res = _deep_search_mongo_data(mapped_field, mongo_data, mongo_field)
                if deep_res == 'not exist in data':
                    flag += 1
                    if flag == len(mongo_field_list):
                        logger.critical(f"map中的{mongo_field_list},在数据源中缺失!被置为None")
                        mapped_data[mapped_field] = None
                else:
                    mapped_data[mapped_field] = deep_res
                    if deep_res is None:
                        logger.warning(f'map中的{mongo_field},在数据源中为空值(None),特征逻辑可能未生效')
                    break
            elif mongo_field not in mongo_data:
                logger.critical(f"map中的{mongo_field_list},在数据源中缺失!被置为None")
                mapped_data[mapped_field] = None
            else:
                mapped_data[mapped_field] = mongo_data[mongo_field]
                if mongo_data[mongo_field] is None:  # 0 [] () {} 等认为是特征工程生效的
                    logger.warning(f'map中的{mongo_field},在数据源中为空值(None),特征逻辑可能未生效')
                break

def _deep_search_mongo_data(mapped_field, mongo_data, search_dic):
    for search_field, determine_field in search_dic.items():
        if type(determine_field) == dict:
            # return _deep_search_mongo_data(mapped_field, mongo_data[search_field], determine_field)
            return _deep_search_mongo_data(mapped_field, mongo_data.get(search_field,{}), determine_field)
        # elif determine_field not in mongo_data[search_field]:
        elif determine_field not in mongo_data.get(search_field,{}):
            return 'not exist in data'
        else:
            return mongo_data[search_field][determine_field]

def get_mapped_data_from_mongodb_for_specific(mapped_data, mongo_data, map, country):
    full_specific_fields = _get_specific_fields(map)
    for field in full_specific_fields:
        mapped_data[field] = None
    specific_map = map.get(country)
    get_mapped_data_for_public(mapped_data, mongo_data, specific_map)


def _get_specific_fields(map):
    specific_fields = set()
    for app_code_map in map.values():
        for key in app_code_map:
            specific_fields.add(key)
    return specific_fields

