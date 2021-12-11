# coding:utf8
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from loguru import logger

from script.infrastructure.utility.db.es import es_contract


class GeneralES:
    def __init__(self, es_config):
        self._session = Elasticsearch([es_config['ip']], http_auth=es_config['user_pw'], port=es_config['port'])
        self._index = es_config.get('index', None)
        # self._index_type = es_config.get('index_type', None)

    @property
    def session(self):
        return self._session

    @property
    def index(self):
        return self._index

    @index.setter
    def index(self, index):
        self._index = index

    def general_search(self, body):
        logger.info(f'es_查询body：{body}')
        res = self._session.search(index=self._index, body=body)
        # logger.info(f'es_查询res：{res}')
        return res

    def get_all_doc(self):
        body = eval(es_contract.es_get_all_documents['es'])
        return self._session.search(index=self._index, body=body)

    def insert_data(self, id, body):
        res = self._session.create(index=self._index, id=id, body=body)

    def update_data(self, id, body):
        res = self._session.update(index=self._index, id=id, body=body)

    def get_doc_by_ID(self, params=None):
        """
        根据ID获取文档中的include_list里的字段
        :param params: {'id':id,'include_list':include_list}
        """
        body = eval(es_contract.es_get_doc_by_ID['es'], params)
        return self._session.search(index=self._index, body=body)

    def update_mapping(self, update_dic=None):
        """
        批量更新某index 的 _mapping
        :param update_dic: {字段名[str]：类型[str], ...}
        """
        for prop_name, prop_type in update_dic.items():
            params = {'prop_name': prop_name, 'prop_type': prop_type}
            body = eval(es_contract.es_update_mapping['es'], params)
            self._session.indices.put_mapping(body=body, index=self._index)

    def update_by_ID_with_script(self, name=None, id_value_dic=None):
        """
        根据_id 单条插入/更新 某单一字段的数据
        :param name: 字段名
        :param id_value_dic: {id[str]:insert_value[str],...}
        """
        params = {'id': list(id_value_dic.keys())[0], 'name': name, 'params_dic': {name: id_value_dic.get(id)}}
        body = eval(es_contract.es_update_by_ID_with_script['es'], params)
        return self._session.update_by_query(index=self._index, body=body)

    def bulk_update_by_ID(self, name=None, id_value_dic=None):
        """
        根据_id 创建批量更新/插入同一index下的某单一字段的bulk列表
        :param name: 字段名
        :param id_value_dic: {id[str]:insert_value[str],...}
        """
        bulk_actions = []
        for id, insert_value in id_value_dic.items():
            params = {'index_name': self._index, 'id': id, 'name': name, 'insert_value': insert_value}
            action = eval(es_contract.es_bulk_update_by_ID['es'], params)
            bulk_actions.append(action)
        bulk(self._session, actions=bulk_actions)

if __name__ == '__main__':
    from script.appservice.config import config

    es_riskdata_ban = GeneralES(config.es_db_conf['ind'])
    session = es_riskdata_ban.session
    logger.add(f"location_ind.log")
    for i in range(1,30):
        day = f'{i}' if i > 9 else f'0{i}'
        day1 = f'{i+1}' if i > 8 else f'0{i+1}'
        params = {"query":{"range":{"create_time":{"gte":f"2021-09-{day} 00:00:00", "lte":f"2021-09-{day1} 00:00:00"}}}, "from":0,"size":4000}
        temp = es_riskdata_ban.general_search(params)
        res = temp['hits']['hits']
        logger.info(f"2021-09-{day}: 共：{temp['hits']['total']['value']}, 取:{len(res)}")
        for record in res:
            id = record['_id']
            location = record['_source'].get('location')
            if location:
                if not location.get('data'):
                    record['_source']['location'] = {'data':{'lat':record['_source']['location'].get('latitude'),'lon':record['_source']['location'].get('longitude')},'altitude':record['_source']['location'].get('altitude'), 'createTime':record['_source']['location'].get('createTime')}
                    logger.critical(f"{id}, location 已经修改")
                    es_riskdata_ban.update_data(id=id,body={"doc": record['_source']})
            else:
                logger.warning(f"{id},找不到location")
    import time
    time.sleep(2000)

