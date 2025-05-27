
from config import snowflake_prd_config
from create_table_sql import task_comment
from snowflake.snowpark.functions import col
from create_session import create_session
from dynamic_merge import dynamic_merge
if __name__ == '__main__':
    method = '/api/cusVisit/v1/getVisitRecordApprovalData'
    method_mode = "CREATE"
    target_table = 'ODS_T_CRM_TASK_COMMENT'
    source_table = target_table+'_TMP'
    keys = ['FUNCTION_ID', 'ID', 'VISIT_ID']
    session = create_session(snowflake_prd_config)
    sql_str = task_comment(method, method_mode)
    df_data = session.sql(sql_str).to_pandas()
    df_data.drop_duplicates(
        subset=keys, inplace=True, keep='last')
    # session.write_pandas(
    #     df_data, table_name=target_table,  auto_create_table=True, overwrite=True)
    session.write_pandas(
        df_data, table_name=source_table,  auto_create_table=True, overwrite=True, table_type="transient")

    dynamic_merge(session=session, target_table_name=target_table,
                  source_table_name=source_table, keys=keys)

    t = session.table("ODS.CRM.ODS_T_CRM_EXTRACT_ORIGINAL_DATA")
    condition = (col("IS_PROCCESSED") == "false") & (
        col("METHOD").like(f"{method}%")) & (col("METHOD_MODE") == method_mode)
    t.update(
        {"IS_PROCCESSED": True},
        condition=condition,
    )
