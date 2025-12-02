
import pandas as pd
from mysql_handler import create_mysql_engine, select_mysql_sql, execute_sql
from config import snowflake_prd_config
from create_session import create_session
from sqlalchemy.types import VARCHAR, TEXT, BIGINT, DATE, DATETIME

# -------------------------------------------------
# 2. 自动类型转换函数（推荐封装）
# -------------------------------------------------


def convert_df_to_mysql_types(df, type_mapping):
    """
    将 DataFrame 列转换为 SQLAlchemy 支持的 MySQL 类型
    """
    df = df.copy()  # 不修改原数据

    for col, sql_type in type_mapping.items():
        if col not in df.columns:
            print(f"警告: 列 '{col}' 不存在，已跳过")
            continue

        if sql_type in [BIGINT]:
            df[col] = pd.to_numeric(
                df[col], errors='coerce', downcast=None)  # 保留大 int
            df[col] = df[col].where(df[col].notna(), None)  # NULL 处理

        elif sql_type == DATE:
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.date

        elif sql_type == DATETIME:
            df[col] = pd.to_datetime(df[col], errors='coerce')

        elif isinstance(sql_type, VARCHAR):
            length = sql_type.length
            df[col] = df[col].astype(str).str.slice(0, length)  # 截断超长
            df[col] = df[col].replace({'nan': None, 'None': None})  # 清理
            df[col] = df[col].astype('string')  # pandas 可空字符串

        elif sql_type == TEXT:
            df[col] = df[col].astype('string')

    return df


if __name__ == '__main__':
    snowflake_session = create_session(snowflake_prd_config)
    engine = create_mysql_engine()
    sql_str = "select max(update_time ) as extract_time from weis.store_activity sa;"
    res = select_mysql_sql(egine=engine, mysql_str=sql_str)
    if not res[0][0]:
        last_extract_time = '2025-10-01 14:00:00'
    else:
        last_extract_time = res[0][0]
    # last_extract_time = '2025-10-01 14:00:00'
    print(last_extract_time)
    fetch_data_sql = """
    select
        item.id,
        item.plan_code as activity_code,
        item.plan_name as activity_name,
        header.description as activity_description,
        item.cus_id as store_id,
        item.cus_Code as store_code,
        item.cus_name as store_name,
        item.applicant_code as dsr_code,
        item.applicant as dsr_name,
        item.start_date as start_date,
        item.end_date as end_date,
        header.plan_type as promotion_activity_type,
        header.form_id as activity_form,
        item.exe_require as execution_requirements,
        item.remark ,
        'BI_SYSTEM' as create_by,
        'BI_SYSTEM' as update_by,
        item.apply_code,
        item.item_name,
        CURRENT_TIMESTAMP() as create_time,
        item.update_time,
        activity_status,
        item.item_status
        
    from
        ods.crm.ods_v_crm_promotion_details as item
        inner join ods.crm.ods_t_crm_promotions as header on item.plan_id = header.id
    where
        store_id <> ''
        and item.plan_name <> 'null'
            ;
    """.format(last_extract_time)
    # and item.item_status = '1'
    # and activity_status = '1'
    result = snowflake_session.sql(fetch_data_sql).to_pandas()

    result.columns = [i.lower() for i in result.columns]
    # print(result.columns)
    column_type_mapping = {

        'activity_code': VARCHAR(50, collation='utf8mb4_general_ci'),
        'activity_name': VARCHAR(100, collation='utf8mb4_general_ci'),
        'activity_description': TEXT(collation='utf8mb4_general_ci'),
        'store_id': VARCHAR(20, collation='utf8mb4_general_ci'),
        'store_code': VARCHAR(50, collation='utf8mb4_general_ci'),
        'store_name': VARCHAR(255, collation='utf8mb4_general_ci'),
        'dsr_code': VARCHAR(50, collation='utf8mb4_general_ci'),
        'dsr_name': VARCHAR(100, collation='utf8mb4_general_ci'),
        'start_date': DATE,
        'end_date': DATE,
        'promotion_activity_type': VARCHAR(100, collation='utf8mb4_general_ci'),
        'activity_form': VARCHAR(100, collation='utf8mb4_general_ci'),
        'execution_requirements': TEXT(collation='utf8mb4_general_ci'),
        'remark': TEXT(collation='utf8mb4_general_ci'),
        'create_by': VARCHAR(50, collation='utf8mb4_general_ci'),
        'update_by': VARCHAR(50, collation='utf8mb4_general_ci'),
        'apply_code': VARCHAR(100, collation='utf8mb4_general_ci'),
        'item_name': VARCHAR(100, collation='utf8mb4_general_ci'),
        'create_time': DATETIME,
        'update_time': DATETIME,
        'activity_status': VARCHAR(1, collation='utf8mb4_general_ci'),
        'item_status': VARCHAR(1, collation='utf8mb4_general_ci'),
        # 'apply_code': VARCHAR(255, collation='utf8mb4_general_ci'),
    }
    df_converted = convert_df_to_mysql_types(result, column_type_mapping)
    # print(df_converted)
    df_converted.to_sql(
        name='store_activity_tmp',
        con=engine,
        if_exists='replace',

        index=False,
        dtype=column_type_mapping  # 关键！指定类型
    )
    print("数据写入成功，类型完全匹配！")

    # MySQL 模拟 Snowflake MERGE（根据 activity_code + store_id + store_code 任意匹配）
    # 目标表：weis.store_activity
    # 源表：   weis.store_activity_tmp

    # 第一步：匹配上了就 UPDATE（包括更新 update_time）
    update_sql = """
        UPDATE weis.store_activity AS target
            INNER JOIN weis.store_activity_tmp AS source
            ON target.activity_code = source.activity_code
            AND target.store_id      = source.store_id
            
            SET 
                target.activity_name           = source.activity_name,
                target.activity_description    = source.activity_description,
                target.store_name              = source.store_name,
                target.dsr_code                = source.dsr_code,
                target.dsr_name                = source.dsr_name,
                target.start_date              = source.start_date,
                target.end_date                = source.end_date,
                target.promotion_activity_type = source.promotion_activity_type,
                target.activity_form           = source.activity_form,
                target.execution_requirements  = source.execution_requirements,
                target.remark                  = source.remark,
                target.update_by               = source.update_by,
                target.update_time             = source.update_time,
                target.apply_code              = source.apply_code,
                target.store_code              = source.store_code,
                target.item_name               = source.item_name,
                target.item_status             = source.item_status,
                target.activity_status         = source.activity_status
    """
    insert_sql = """
    INSERT INTO weis.store_activity 
        (
            activity_code, activity_name, activity_description, store_id, store_code, store_name,
            dsr_code, dsr_name, start_date, end_date, promotion_activity_type, activity_form,
            execution_requirements, remark, create_by, update_by, create_time, update_time,apply_code,item_name,
            item_status,activity_status
        )
        SELECT 
            source.activity_code, source.activity_name, source.activity_description, source.store_id, 
            source.store_code, source.store_name, source.dsr_code, source.dsr_name, source.start_date, 
            source.end_date, source.promotion_activity_type, source.activity_form,
            source.execution_requirements, source.remark, source.create_by, source.update_by, 
            source.create_time, source.update_time,source.apply_code,source.item_name,
            source.item_status,source.activity_status
        FROM weis.store_activity_tmp AS source
        LEFT JOIN weis.store_activity AS target
            ON target.activity_code = source.activity_code
            AND target.store_id      = source.store_id
            
        WHERE target.activity_code IS NULL;
    """
    res = execute_sql(egine=engine, mysql_str=update_sql)
    res = execute_sql(egine=engine, mysql_str=insert_sql)
    # print(res)
