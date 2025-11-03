
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
        last_extract_time = '2025-11-01 14:00:00'
    else:
        last_extract_time = res[0][0]

    fetch_data_sql = """
        select
            item.plan_code as activity_code,
            item.plan_name as activity_name,
            '' as activity_description,
            item.cm_receive_id as store_id,
            item.cm_receive_code as store_code,
            item.cm_receive_name as store_name,
            item.applicant_code as dsr_code,
            item.applicant as dsr_name,
            item.start_date as start_date,
            item.end_date as end_date,
            header.plan_type as promotion_activity_type,
            header.form_id as activity_form,
            item.exe_require as execution_requirements,
            item.remark,
            'BI_SYSTEM' as create_by,
            'BI_SYSTEM' as update_by,
            CURRENT_TIMESTAMP() as create_time,
            item.update_time
        from
            ods.crm.ods_v_crm_promotion_details as item
            inner join ods.crm.ods_t_crm_promotions as header on item.plan_id = header.id
        where
            store_id <> ''
            and item.update_time >= '{0}';
    """.format(last_extract_time)
    result = snowflake_session.sql(fetch_data_sql).to_pandas()

    result.columns = [i.lower() for i in result.columns]
    # print(result.columns)
    column_type_mapping = {
        'id': BIGINT,
        'activity_code': VARCHAR(50),
        'activity_name': VARCHAR(100),
        'activity_description': TEXT,
        'store_id': VARCHAR(20),
        'store_code': VARCHAR(50),
        'store_name': VARCHAR(255),
        'dsr_code': VARCHAR(50),
        'dsr_name': VARCHAR(100),
        'start_date': DATE,
        'end_date': DATE,
        'promotion_activity_type': VARCHAR(100),
        'activity_form': VARCHAR(100),
        'execution_requirements': TEXT,
        'remark': TEXT,
        'create_by': VARCHAR(50),
        'update_by': VARCHAR(50),
        'create_time': DATETIME,
        'update_time': DATETIME,
    }
    df_converted = convert_df_to_mysql_types(result, column_type_mapping)
    print(df_converted)
    df_converted.to_sql(
        name='store_activity_tmp',
        con=engine,
        if_exists='replace',
        index=False,
        dtype=column_type_mapping  # 关键！指定类型
    )
    print("数据写入成功，类型完全匹配！")

    insert_mysql_str = """
        INSERT INTO weis.store_activity 
    (
        activity_code, activity_name, activity_description, store_id, store_code, store_name,
        dsr_code, dsr_name, start_date, end_date, promotion_activity_type, activity_form,
        execution_requirements, remark, create_by, update_by, create_time, update_time
    )
    SELECT 
        activity_code, activity_name, activity_description, store_id, store_code, store_name,
        dsr_code, dsr_name, start_date, end_date, promotion_activity_type, activity_form,
        execution_requirements, remark, create_by, update_by, create_time, update_time
    FROM weis.store_activity_tmp AS tmp
    ON DUPLICATE KEY UPDATE
        activity_name = tmp.activity_name,
        activity_description = tmp.activity_description,
        store_code = tmp.store_code,
        store_name = tmp.store_name,
        dsr_code = tmp.dsr_code,
        dsr_name = tmp.dsr_name,
        start_date = tmp.start_date,
        end_date = tmp.end_date,
        promotion_activity_type = tmp.promotion_activity_type,
        activity_form = tmp.activity_form,
        execution_requirements = tmp.execution_requirements,
        remark = tmp.remark,
        update_by = tmp.update_by,
        update_time = tmp.update_time;
    """
    res = execute_sql(egine=engine, mysql_str=insert_mysql_str)
    print(res)
