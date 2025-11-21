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
    data.value:id::string as id,
    data.value:comment_time::string as comment_time,
    data.value:code::string as code,
    data.value:re_comment_status::string as re_comment_status,
    data.value:form_define_id::string as form_define_id,
    data.value:report_cm_code::string as report_cm_code,
    data.value:first_comment_data_id::string as first_comment_data_id,
    data.value:record_sys_id::string as record_sys_id,
    data.value:re_record_sys_name::string as re_record_sys_name,
    data.value:re_comment_data_id::string as re_comment_data_id,
    data.value:sye_name::string as sye_name,
    data.value:project_name::string as project_name,
    data.value:sye_id::string as sye_id,
    data.value:cm_code::string as cm_code,
    data.value:re_record_sys_code::string as re_record_sys_code,
    data.value:sye_code::string as sye_code,
    data.value:record_sys_name::string as record_sys_name,
    data.value:data_id::string as data_id,
    data.value:re_pre_verify_amount::string as re_pre_verify_amount,
    data.value:re_comment_text::string as re_comment_text,
    data.value:cm_source_code::string as cm_source_code,
    data.value:arrive_lla::string as arrive_lla,
    data.value:report_cm_name::string as report_cm_name,
    data.value:comment_text::string as comment_text,
    data.value:re_record_sys_id::string as re_record_sys_id,
    data.value:project_code::string as project_code,
    data.value:activity_code::string as activity_code,
    data.value:re_record_source_code::string as re_record_source_code,
    data.value:visit_implement_id::string as visit_implement_id,
    data.value:activity_cus_id::string as activity_cus_id,
    data.value:plan_name::string as plan_name,
    data.value:ai_status::string as ai_status,
    data.value:record_id::string as record_id,
    data.value:record_source_code::string as record_source_code,
    data.value:pre_verify_amount::string as pre_verify_amount,
    data.value:arrive_pos_offset::string as arrive_pos_offset,
    data.value:report_cm_source_code::string as report_cm_source_code,
    data.value:record_audit_form_id::string as record_audit_form_id,
    data.value:cm_id::string as cm_id,
    data.value:cm_name::string as cm_name,
    data.value:pos_is_normal::string as pos_is_normal,
    data.value:re_comment_time::string as re_comment_time,
    data.value:record_sys_code::string as record_sys_code,
    data.value:report_cm_id::string as report_cm_id,
    data.value:source_code::string as source_code,
    data.value:record_time::string as record_time,
    data.value:status::string as status,
    data.value:comment_status::string as comment_status
from
    ods.crm.ods_t_crm_extract_original_data as t,
    lateral flatten(input => (parse_json(t.extracted_result))) as data
where
    method = '/api/cuxiao/v1/queryRegularReport';

"""


def exec(method: str):
    path = '/api/cuxiao/v1/queryRegularReport'
    # method_mode = 'CREATE'
    keys = ['DATA_ID']
    extract_handler(path=path,
                    method_mode=method)
    session = create_session(snowflake_prd_config)
    data_df = sql_select(snowflake_session=session, query_str=SQL_STR)
    if data_df.empty == False:

        data_df.drop_duplicates(
            subset=keys, keep='last', inplace=True)
        try:
            source_df = session.create_dataframe(data_df)
            dynamic_merge_data(
                session=session,
                source_df=source_df,
                table_name='ODS.CRM.ODS_T_CRM_PROMOTIONS_COMMENT',
                merge_keys=keys
            )

        except Exception as e:
            print("merge table failed")
        finally:
            session.close()


if __name__ == '__main__':
    method_mode = ['CREATE', 'MODIFY']
    for i in method_mode:
        exec(i)
    # path = '/api/cuxiao/v1/queryRegularReport'
    # method_mode = 'CREATE'
    # keys = ['DATA_ID']
    # session = create_session(snowflake_prd_config)
    # data_df = sql_select(snowflake_session=session, query_str=SQL_STR)
    # data_df.drop_duplicates(
    #     subset=keys, keep='last', inplace=True)
    # data_df['UPDATE_TIME'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    # res = CreateTable(
    #     table_name='ODS.CRM.ODS_T_CRM_PROMOTIONS_COMMENT').create_table(df=data_df)

    # keys = ['DATA_ID']
    # extract_handler(path=path,
    #                 method_mode=method_mode)
    # session = create_session(snowflake_prd_config)
    # data_df = sql_select(snowflake_session=session, query_str=SQL_STR)
    # if data_df.empty == False:

    #     data_df.drop_duplicates(
    #         subset=keys, keep='last', inplace=True)
    #     try:
    #         source_df = session.create_dataframe(data_df)
    #         dynamic_merge_data(
    #             session=session,
    #             source_df=source_df,
    #             table_name='ODS.CRM.ODS_T_CRM_PROMOTIONS_DETAILS',
    #             merge_keys=keys
    #         )

    #     except Exception as e:
    #         print("merge table failed")
    #     finally:
    #         session.close()
