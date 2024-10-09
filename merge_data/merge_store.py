# coding: utf-8

from config import snowflake_prd_config
from create_table_sql import store
from create_session import create_session
from dynamic_merge import dynamic_merge
if __name__ == '__main__':

    # 获取哪个API
    method = '/api/store/v1/queryStore'
    method_mode = 'MODIFY'
    # 创建一个snowflake session
    session = create_session(snowflake_prd_config)
    store_sql = store(method=method, method_mode=method_mode)
    # 执行创建临时表
    session.sql(store_sql).collect()
    # dynamic_merge(session=session, target_table_name='ODS.CRM.ODS_T_STORE',
    #               source_table_name='ODS.CRM.ODS_T_STORE_TMP')
