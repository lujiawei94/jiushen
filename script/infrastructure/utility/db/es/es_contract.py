# 其实这里的contract也要可以拆分（basic 和 feature）

es_get_all_documents = {
    'es': {'query': {'match_all': {}}}
}

es_get_doc_by_ID = {
    'es': '''{
            "_source": {
                "include": include_list
            },
            "query": {
                "match": {
                    "_id": id
                }
            }
    }''',
    'params': ['id', 'include_list']
}

es_update_mapping = {
    'es': '''{
            "properties": {
                prop_name: {
                    "type": prop_type
                }
            }
    }''',
    'params': ['prop_name', 'prop_type']
}

es_update_by_ID_with_script = {
    'es': '''{
        "query": {
            "term": {"_id": id}
        },
        "script": {
            "lang": "painless",
            "inline": f"ctx._source.{name}=params.{name};",
            "params": params_dic
        }
    }''',
    'params': ['id', 'name', 'params_dic']
}

es_bulk_update_by_ID = {
    'es': '''{
            "_op_type": "update",
            "_index": index_name,
            "_id": id,
            "script": {
                "lang": "painless",
                "inline": f"ctx._source.{name}=params.{name};",
                "params": {name: insert_value}
            }
    }''',
    'params': ['index_name', 'id', 'name', 'insert_value']
}

