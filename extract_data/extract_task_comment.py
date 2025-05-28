from extract_handler import extract_handler
from create_session import create_session
from config import snowflake_prd_config
# import snowflake.connector


def get_function_list(session):
    query = """
                select distinct
                    function_id,
                    table_name
                from
                    ods.crm.ods_t_crm_subtask_settings;"""
    res = session.sql(query).collect()
    return res


if __name__ == '__main__':
    # path = '/api/store/v1/queryStore'
    #     method_mode = 'MODIFY'
    session = create_session(snowflake_prd_config)
    function_lists = get_function_list(session=session)
    for function_list in function_lists:
        function_id, table_name = function_list
        extract_handler(path='/api/cusVisit/v1/getVisitRecordApprovalData',
                        method_mode='CREATE', function_id=function_id)
