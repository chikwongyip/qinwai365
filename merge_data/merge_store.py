# coding: utf-8

from config import snowflake_prd_config
from create_table_sql import store
from create_session import create_session
from dynamic_merge import dynamic_merge
if __name__ == '__main__':

    method = ''
    method_mode = ''
    session = create_session(snowflake_prd_config)
    store_sql = store(method=method, method_mode=method_mode)
    dynamic_merge(session=session, target_table_name='ODS.CRM.ODS_T_STORE',source_table_name='ODS.CRM.ODS_T_STORE_TMP',data)