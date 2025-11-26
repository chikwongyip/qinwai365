# coding:utf-8
from config import snowflake_prd_config
from extract_handler import extract_handler
from create_session import create_session
from create_dataframe import sql_select
from dynamic_merge_data import dynamic_merge_data
from datetime import datetime
# from create_table import CreateTable


def get_sql_str(method: str):

    sql_str = """
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
        and is_proccessed = false
        and method_mode = '{0}';
    """.format(method)
    return sql_str


def exec(meth_mode: str):
    keys = ['ID']
    path = '/api/cuxiao/v1/queryRegularSale'
    method_mode = meth_mode
    extract_handler(path=path,
                    method_mode=method_mode)
    session = create_session(snowflake_prd_config)
    sql_str = get_sql_str(method=meth_mode)
    data_df = sql_select(snowflake_session=session, query_str=sql_str)
    # print(data_df)
    if data_df.empty == False:

        data_df.drop_duplicates(
            subset=keys, keep='last', inplace=True)
        data_df['UPDATE_TIME'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            source_df = session.create_dataframe(data_df)
            dynamic_merge_data(
                session=session,
                source_df=source_df,
                table_name='ODS.CRM.ODS_T_CRM_PROMOTIONS',
                merge_keys=keys
            )
            t = session.table("ODS.CRM.ODS_T_CRM_EXTRACT_ORIGINAL_DATA")
            t.update(
                {"IS_PROCCESSED": True},
                (t["METHOD"] == path)
                & (t["IS_PROCCESSED"] == False)
                & (t["METHOD_MODE"] == method_mode),
            )
        except Exception as e:
            print("merge table failed")
        finally:
            session.close()


if __name__ == '__main__':

    method_mode = ['CREATE', 'MODIFY']
    for i in method_mode:
        exec(i)
