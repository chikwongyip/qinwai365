# coding: utf-8

from config import snowflake_prd_config
from create_table_sql import store, store_dealers, store_ext, store_receiver
from create_session import create_session
from dynamic_merge import dynamic_merge
if __name__ == '__main__':

    # 获取哪个API
    method = '/api/store/v1/queryStore'
    method_mode = 'MODIFY'
    # 创建一个snowflake session
    print("正在处理接口{0} - {1}".format(method, method_mode))
    session = create_session(snowflake_prd_config)
    print('正在更新ODS.CRM.ODS_T_STORE')
    store_sql = store(method=method, method_mode=method_mode)
    # 执行创建临时表
    session.sql(store_sql).collect()
    dynamic_merge(session=session, target_table_name='ODS.CRM.ODS_T_STORE',
                  source_table_name='ODS.CRM.ODS_T_STORE_TMP', keys=['ID'])
    print('更新门店数据完成')

    print('更新门店ODS.CRM.ODS_T_STORE_EXTS')
    store_ext_sql = store_ext(method=method, method_mode=method_mode)
    session.sql(store_ext_sql).collect()
    dynamic_merge(session=session, target_table_name='ODS.CRM.ODS_T_STORE_EXTS',
                  source_table_name='ODS.CRM.ODS_T_STORE_EXTS_TMP', keys=['ID', 'STORE_EXT_COLUMN'])
    print('更新门店扩展数据完成')

    store_receiver_sql = store_receiver(method=method, method_mode=method_mode)
    session.sql(store_receiver_sql).collect()
    dynamic_merge(session=session, target_table_name='ODS.CRM.ODS_T_STORE_MAP_RECEIVE_INFO',
                  source_table_name='ODS.CRM.ODS_T_STORE_MAP_RECEIVE_INFO_TMP', keys=['ID', 'DEALER_ID'])

    store_dealers_sql = store_dealers(method=method, method_mode=method_mode)
    session.sql(store_dealers_sql).collect()
    dynamic_merge(session=session, target_table_name='ODS.CRM.ODS_T_STORE_MAP_DEALER',
                  source_table_name='ODS.CRM.ODS_T_STORE_MAP_DEALER_TMP', keys=['ID', 'WAIQIN365_DEALER_ID'])
    # 更新json 记录表
    t = session.table("ODS.CRM.ODS_T_CRM_EXTRACT_ORIGINAL_DATA")
    t.update(
        {"IS_PROCCESSED": True},
        (t["METHOD"] == method)
        & (t["METHOD_MODE"] == method_mode)
        & (t["IS_PROCCESSED"] == False),
    )
    session.close()
