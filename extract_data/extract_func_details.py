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
from datetime import datetime
SQL_STR = """
select
    -- Extract fields from the 'data' array inside the JSON response
    v.value:id::string as id,
    v.value:activityStatus::string as activity_status,
    v.value:applicant::string as applicant,
    v.value:applicantCode::string as applicant_code,
    v.value:applicantId::string as applicant_id,
    v.value:applicantSourceCode::string as applicant_source_code,
    v.value:applyCode::string as apply_code,
    v.value:applyId::string as apply_id,
    v.value:applyName::string as apply_name,
    v.value:applyTime::string as apply_time, -- Better type
    v.value:auditTime::string as audit_time,
    v.value:budgetId::string as budget_id,
    v.value:extColumns::string as ext_columns,
    v.value:formDataId::string as form_data_id,
    v.value:formDefinedId::string as form_defined_id,
    v.value:modifyTime::string as modify_time,
    v.value:parentApplyCode::string as parent_apply_code,
    v.value:planCode::string as plan_code,
    v.value:planId::string as plan_id,
    v.value:planName::string as plan_name,
    v.value:projectApplyCode::string as project_apply_code,
    v.value:projectCode::string as project_code,
    v.value:projectName::string as project_name,
    v.value:status::string as status,
    v.value:subList::string as sub_list -- Keep as VARIANT if nested
from
    ods.crm.ods_t_crm_extract_original_data t,
    lateral FLATTEN(
        input => PARSE_JSON(t.extracted_result) -- Key fix: access .data array
    ) v
where
    t.method = '/api/cuxiao/v1/queryRegularSaleActivities'
    and t.is_proccessed = false;

"""
if __name__ == '__main__':
    # path = '/api/store/v1/queryStore'
    #     method_mode = 'MODIFY'
    # keys = ['ID']
    # session = create_session(snowflake_prd_config)
    # data_df = sql_select(snowflake_session=session, query_str=SQL_STR)
    # data_df.drop_duplicates(
    #     subset=keys, keep='last', inplace=True)
    # data_df['UPDATE_TIME'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    # res = CreateTable(
    #     table_name='ODS.CRM.ODS_T_CRM_PROMOTIONS_DETAILS').create_table(df=data_df)

    keys = ['ID']
    extract_handler(path='/api/cuxiao/v1/queryRegularSaleActivities',
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
                table_name='ODS.CRM.ODS_T_CRM_PROMOTIONS_DETAILS',
                merge_keys=keys
            )

        except Exception as e:
            print("merge table failed")
        finally:
            session.close()
    
