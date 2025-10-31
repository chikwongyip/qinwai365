# from request_qince_api import Qince_API
# from config import snowflake_prd_config

# if __name__ == '__main__':
#     dict_data = {
#         "create_start": "2025-09-01 00:00:00",
#         "create_end": "2025-09-11 00:00:00"
#     }
#     res = Qince_API('/api/cuxiao/v1/queryRegularSale',
#                     snowflake_prd_config, dict_data).request_data()
#     print(res)

# coding: utf-8
from config import snowflake_prd_config
from extract_handler import extract_handler
from create_session import create_session
from create_dataframe import sql_select
from dynamic_merge_data import dynamic_merge_data
from create_table import CreateTable
SQL_STR = """
select
    data.value:id::STRING as id,
    data.value:item_names::STRING as item_names,
    data.value:form_id::STRING as form_id,
    data.value:flow_status::STRING as flow_status,
    data.value:next_user_names::STRING as next_user_names,
    data.value:assign_budget::STRING as assign_budget,
    data.value:auditor_name::STRING as auditor_name,
    data.value:budget_freeze_node::STRING as budget_freeze_node,
    data.value:budget_name::STRING as budget_name,
    data.value:code::STRING as code,
    data.value:description::STRING as description,
    data.value:execute_cycle::STRING as execute_cycle,
    data.value:exts::STRING as exts,
    data.value:pay_type::STRING as pay_type,
    data.value:pd_range_type::STRING as pd_range_type,
    data.value:plan_amount::STRING as plan_amount,
    data.value:plan_name::STRING as plan_name,
    data.value:plan_status::STRING as plan_status,
    data.value:plan_type::STRING as plan_type,
    data.value:predict_effective_ratio::STRING as predict_effective_ratio,
    data.value:predict_sale_amount::STRING as predict_sale_amount,
    data.value:project_code::STRING as project_code,
    data.value:project_name::STRING as project_name,
    data.value:project_status::STRING as project_status
from
    ods.crm.ods_t_crm_extract_original_data,
    lateral FLATTEN(input => PARSE_JSON(extracted_result)) as data
where
    method = '/api/cuxiao/v1/queryRegularSale'
    and is_proccessed = false;
"""
if __name__ == '__main__':
    # path = '/api/store/v1/queryStore'
    #     method_mode = 'MODIFY'
    # keys = ['ID']
    # session = create_session(snowflake_prd_config)
    # data_df = sql_select(snowflake_session=session, query_str=SQL_STR)
    # data_df.drop_duplicates(
    #     subset=keys, keep='last', inplace=True)
    # res = CreateTable(
    #     table_name='ODS.CRM.ODS_T_CRM_PROMOTIONS').create_table(df=data_df)

    keys = ['ID']
    extract_handler(path='/api/cuxiao/v1/queryRegularSale',
                    method_mode='CREATE')
    session = create_session()
    data_df = sql_select(snowflake_session=session, query_str=SQL_STR)
    if data_df.empty == False:

        data_df.drop_duplicates(
            subset=keys, keep='last', inplace=True)
        try:
            source_df = session.create_dataframe(data_df)
            dynamic_merge_data(
                session=session,
                source_df=data_df,
                table_name='ODS.CRM.ODS_T_CRM_PROMOTIONS',
                merge_keys=keys
            )

        except Exception as e:
            print("merge table failed")
        finally:
            session.close()
    # extract_handler(path='/api/store/v1/queryStore', method_mode='CREATE')
