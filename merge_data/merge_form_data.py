
from config import snowflake_prd_config
from create_table_sql import form_data
from snowflake.snowpark.functions import col
from create_session import create_session
from dynamic_merge import dynamic_merge


def table_exists(session, table_name: str) -> bool:
    """
    检查表是否存在
    :param session: Snowpark Session 对象
    :param table_name: 表名（区分大小写）
    :param schema_name: Schema 名（可选，默认为当前 Session 的 Schema）
    :param database_name: 数据库名（可选，默认为当前 Session 的 Database）
    :return: True 表示存在，False 表示不存在
    """
    database_name = 'ODS'
    schema_name = 'CRM'
    table_name = table_name
    # 动态构造查询条件
    conditions = []
    if database_name:
        conditions.append(f"table_catalog = '{database_name.upper()}'")
    if schema_name:
        conditions.append(f"table_schema = '{schema_name.upper()}'")
    conditions.append(f"table_name = '{table_name.upper()}'")

    query = f"""
            SELECT COUNT(*) AS CNT
            FROM INFORMATION_SCHEMA.TABLES
            WHERE {" AND ".join(conditions)}
        """

    result = session.sql(query).collect()
    # print(result[0]['CNT'] > 0)
    return result[0]['CNT'] > 0


if __name__ == '__main__':
    session = create_session(snowflake_prd_config)
    query_str = """
      select distinct
            form_id,
            table_name
        from
            ods.crm.ods_t_crm_form_config
        
    """
    form_config = session.sql(query_str).collect()
    for forms in form_config:
        form_id, table_name = forms
        print('正在执行', form_id, table_name)
        method = '/api/userDefined/v1/queryUserDefined'
        method_mode = "CREATE"
        target_table = 'ODS_T_CRM_FORM_DATA' + '_' + table_name
        target_table = target_table.upper()
        source_table = target_table+'_TMP'
        keys = ['ID']
        sql_str = form_data(session, form_id, method, method_mode)
        # print(sql_str)
        df_data = session.sql(sql_str).to_pandas()
        df_data.drop_duplicates(
            subset=keys, inplace=True, keep='last')
        if df_data.empty == False:

            res = table_exists(session, table_name=target_table)
            if res:
                print('更新表：', target_table)
                session.write_pandas(
                    df_data, table_name=source_table,  auto_create_table=True, overwrite=True, table_type="transient")
                dynamic_merge(session=session, target_table_name=target_table,
                              source_table_name=source_table, keys=keys)
            else:
                print('创建表：', target_table)
                session.write_pandas(
                    df_data, table_name=target_table,  auto_create_table=True, overwrite=True)

            t = session.table("ODS.CRM.ODS_T_CRM_EXTRACT_ORIGINAL_DATA")
            condition = (col("IS_PROCCESSED") == "false") & (
                col("METHOD") == method+'-'+str(form_id)) & (col("METHOD_MODE") == method_mode)
            t.update(
                {"IS_PROCCESSED": True},
                condition=condition,
            )
