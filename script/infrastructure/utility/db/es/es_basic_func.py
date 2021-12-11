
def filter_with_term(info_dict):
    lst = []
    for term_key, term_value in info_dict.items():
        if term_key == 'terms_set':
            for terms_set_key, terms_set_value in term_value.items():
                lst.append({"terms_set": {
                    terms_set_key: {"terms": terms_set_value, "minimum_should_match_script": {"source": "5"}}}})
        elif type(term_value) == list:
            lst.append({"terms": {term_key: term_value}})
        else:
            lst.append({"term": {term_key: term_value}})
    return lst

def filter_by_geo_distance(geo_dict, es_location_field='location.data'):
    # geo_dict = {'2km':{'lat':12,'lon':32}}
    lst = []
    for distance, geo_info_dic in geo_dict.items():
        lst.append({"geo_distance": {"distance": distance, es_location_field: geo_info_dic}})
    return lst

def filter_by_fuzzy(fuzzy_dict):
    # fuzzy_dict = {es_field_name:{"value": 'asd', "fuzziness": 4}}
    lst = []
    for es_field_name, fuzzy_info_dic in fuzzy_dict.items():
        lst.append({"fuzzy":{es_field_name:fuzzy_info_dic}})
    return lst

def filter_by_range(range_dict):
    # range_dict = {es_field: {"lt": value}}
    lst = []
    for es_field, range_info_dic in range_dict.items():
        lst.append({"range": {es_field: range_info_dic}})
    return lst

def filter_by_exists(exists_lst):
    lst = []
    for es_field in exists_lst:
        lst.append({"exists": {"field": es_field}})
    return lst

def filter_by_script(script_lst):
    lst = []
    for script in script_lst:
        lst.append({"script": {"script": script}})
    return lst

def filter_by_should(*lst, need_bool=True):
    should_lst = []
    for con in lst:
        if type(con) == list:
            should_lst.extend(con)
        else:
            should_lst.append(con)
    should_syntax = {"should": should_lst, "minimum_should_match": 1}
    should_syntax_with_bool = {"bool": should_syntax}
    return should_syntax_with_bool if need_bool else should_syntax

def filter_by_must(*lst, need_bool=True):
    must_lst = []
    for con in lst:
        if type(con) == list:
            must_lst.extend(con)
        else:
            must_lst.append(con)
    must_syntax = {"must": must_lst}
    must_syntax_with_bool = {"bool": must_syntax}
    return must_syntax_with_bool if need_bool else must_syntax

def filter_by_must_not(*lst, need_bool=True):
    must_not_lst = []
    for con in lst:
        if type(con) == list:
            must_not_lst.extend(con)
        else:
            must_not_lst.append(con)
    must_not_syntax = {"must_not": must_not_lst}
    must_not_syntax_with_bool = {"bool": must_not_syntax}
    return must_not_syntax_with_bool if need_bool else must_not_syntax


def combinate_by_bool(*condition_lst):
    con_dic = {}
    for _ in condition_lst:
        con_dic.update(_)
    bool_syntax = {"bool": con_dic}
    return bool_syntax