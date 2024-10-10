# coding:utf-8

from config import snowflake_prd_config
from create_table_sql import dealer_info
from create_session import create_session
from dynamic_merge import dynamic_merge

if __name__ == '__main__':
    session = create_session(snowflake_prd_config)
    # 获取哪个API
    #  '/api/dealer/v1/queryDealer', 'MODIFY'
    method = '/api/dealer/v1/queryDealer'
    method_mode = 'MODIFY'
    # 创建一个snowflake session
    print("正在处理接口{0} - {1}".format(method, method_mode))
    session = create_session(snowflake_prd_config)
    print('正在更新ODS.CRM.ODS_T_DEALER')
    dealer_sql = dealer_info(method=method, method_mode=method_mode)
    session.sql(dealer_sql).collect()
    dynamic_merge(session=session, target_table_name='ODS.CRM.ODS_T_DEALER',
                  source_table_name='ODS.CRM.ODS_T_DEALER_TMP', keys=['ID'])
    print('更新门店数据完成')
    t = session.table("ODS.CRM.ODS_T_CRM_EXTRACT_ORIGINAL_DATA")
    t.update(
        {"IS_PROCCESSED": True},
        (t["METHOD"] == method)
        & (t["METHOD_MODE"] == method_mode)
        & (t["IS_PROCCESSED"] == False),
    )
