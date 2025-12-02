
import pandas as pd
from mysql_handler import create_mysql_engine, select_mysql_sql, execute_sql
from config import snowflake_prd_config
from create_session import create_session
from sqlalchemy.types import VARCHAR, TEXT, BIGINT, DATE, DATETIME, DECIMAL

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

    fetch_data_sql = """
    select
        *
    from
        api.crm.api_v_crm_mini_program_store
    where
        store_id is not null;
    """
    # and item.item_status = '1'
    # and activity_status = '1'
    result = snowflake_session.sql(fetch_data_sql).to_pandas()

    result.columns = [i.lower() for i in result.columns]
    # print(result.columns)
    column_type_mapping = {

        'store_id': VARCHAR(20, collation='utf8mb4_general_ci'),
        'store_code': VARCHAR(50, collation='utf8mb4_general_ci'),
        'store_name': VARCHAR(255, collation='utf8mb4_general_ci'),
        'store_photos': TEXT(collation='utf8mb4_general_ci'),
        'store_type_code': VARCHAR(20, collation='utf8mb4_general_ci'),
        'store_type_name_cn_en': VARCHAR(255, collation='utf8mb4_general_ci'),
        'store_type_name_cn': VARCHAR(255, collation='utf8mb4_general_ci'),
        'store_type_name_en': VARCHAR(255, collation='utf8mb4_general_ci'),
        'l1_code': VARCHAR(50, collation='utf8mb4_general_ci'),
        'l1_name': VARCHAR(255, collation='utf8mb4_general_ci'),
        'l2_code': VARCHAR(50, collation='utf8mb4_general_ci'),
        'l2_name': VARCHAR(255, collation='utf8mb4_general_ci'),
        'l3_code': VARCHAR(50, collation='utf8mb4_general_ci'),
        'l3_name': VARCHAR(255, collation='utf8mb4_general_ci'),
        'l4_code': VARCHAR(50, collation='utf8mb4_general_ci'),
        'l4_name': VARCHAR(255, collation='utf8mb4_general_ci'),
        'dsr_name': VARCHAR(100, collation='utf8mb4_general_ci'),
        'dsr_type': VARCHAR(50, collation='utf8mb4_general_ci'),
        'dsr_id':  BIGINT,
        'dsr_code': VARCHAR(50, collation='utf8mb4_general_ci'),
        'customer_system_code': VARCHAR(50, collation='utf8mb4_general_ci'),
        'customer_system_name': VARCHAR(255, collation='utf8mb4_general_ci'),
        'province': VARCHAR(100, collation='utf8mb4_general_ci'),
        'city': VARCHAR(100, collation='utf8mb4_general_ci'),
        'address': TEXT(collation='utf8mb4_general_ci'),
        'cooperation_status': VARCHAR(50, collation='utf8mb4_general_ci'),
        'darlie_area': VARCHAR(50, collation='utf8mb4_general_ci'),
        'dealer_code': VARCHAR(50, collation='utf8mb4_general_ci'),
        'store_category': VARCHAR(100, collation='utf8mb4_general_ci'),
        'latitude': DECIMAL(10, 6),
        'longitude': DECIMAL(10, 6),
        'marked_location': TEXT(collation='utf8mb4_general_ci'),
        'marked_district': VARCHAR(100, collation='utf8mb4_general_ci'),
        'marked_city': VARCHAR(100, collation='utf8mb4_general_ci'),
        'marked_province': VARCHAR(100, collation='utf8mb4_general_ci'),
        'is_marked': VARCHAR(20, collation='utf8mb4_general_ci'),
        'nielsen_store_code': VARCHAR(50, collation='utf8mb4_general_ci'),
        'store_channel': VARCHAR(100, collation='utf8mb4_general_ci'),
        'first_sales_month': VARCHAR(1, collation='utf8mb4_general_ci'),
        'avg_sales_half_year': DECIMAL(15, 2),
        'visit_tag': VARCHAR(100, collation='utf8mb4_general_ci'),
        'visit_tag2': VARCHAR(100, collation='utf8mb4_general_ci'),
        'pg_name': VARCHAR(100, collation='utf8mb4_general_ci'),
        'is_assigned_dsr': VARCHAR(10, collation='utf8mb4_general_ci'),
        'has_pg': VARCHAR(10, collation='utf8mb4_general_ci'),
        'create_by': VARCHAR(50, collation='utf8mb4_general_ci'),
        'update_by': VARCHAR(50, collation='utf8mb4_general_ci'),
    }
    df_converted = convert_df_to_mysql_types(result, column_type_mapping)
    # print(df_converted)
    df_converted.to_sql(
        name='store_info_tmp',
        con=engine,
        if_exists='replace',
        chunksize=20000,
        index=False,
        dtype=column_type_mapping  # 关键！指定类型
    )
    print("数据写入成功，类型完全匹配！")

    insert_sql_str = """
        INSERT INTO weis.store_info (   -- ← 改成你的目标表名
            store_id, store_code, store_name, store_photos, store_type_code,
            store_type_name_cn_en, store_type_name_cn, store_type_name_en,
            l1_code, l1_name, l2_code, l2_name, l3_code, l3_name, l4_code, l4_name,
            dsr_name, dsr_type, dsr_id, dsr_code,
            customer_system_code, customer_system_name,
            province, city, address, cooperation_status, darlie_area,
            dealer_code, store_category,
            latitude, longitude, marked_location, marked_district, marked_city, marked_province,
            is_marked, nielsen_store_code, store_channel,
            first_sales_month, avg_sales_half_year,
            visit_tag, visit_tag2, pg_name, is_assigned_dsr, has_pg,
            create_by, update_by, create_time, update_time
        )
        SELECT
            s.store_id, s.store_code, s.store_name, s.store_photos, s.store_type_code,
            s.store_type_name_cn_en, s.store_type_name_cn, s.store_type_name_en,
            s.l1_code, s.l1_name, s.l2_code, s.l2_name, s.l3_code, s.l3_name, s.l4_code, s.l4_name,
            s.dsr_name, s.dsr_type, s.dsr_id, s.dsr_code,
            s.customer_system_code, s.customer_system_name,
            s.province, s.city, s.address, s.cooperation_status, s.darlie_area,
            s.dealer_code, s.store_category,
            s.latitude, s.longitude, s.marked_location, s.marked_district, s.marked_city, s.marked_province,
            s.is_marked, s.nielsen_store_code, s.store_channel,
            s.first_sales_month, s.avg_sales_half_year,
            s.visit_tag, s.visit_tag2, s.pg_name, s.is_assigned_dsr, s.has_pg,
            s.create_by, s.update_by, NOW(), NOW()
        FROM weis.store_info_tmp AS s   -- ← 改成你的源表名
        LEFT JOIN weis.store_info AS t ON t.store_id = s.store_id
        WHERE t.store_id IS NULL;
    """
    update_sql_str = """
        UPDATE weis.store_info AS t
        INNER JOIN weis.store_info_tmp AS s ON t.store_id = s.store_id
        SET
            t.store_code              = s.store_code,
            t.store_name              = s.store_name,
            t.store_photos            = s.store_photos,
            t.store_type_code         = s.store_type_code,
            t.store_type_name_cn_en   = s.store_type_name_cn_en,
            t.store_type_name_cn      = s.store_type_name_cn,
            t.store_type_name_en      = s.store_type_name_en,
            t.l1_code                 = s.l1_code,
            t.l1_name                 = s.l1_name,
            t.l2_code                 = s.l2_code,
            t.l2_name                 = s.l2_name,
            t.l3_code                 = s.l3_code,
            t.l3_name                 = s.l3_name,
            t.l4_code                 = s.l4_code,
            t.l4_name                 = s.l4_name,
            t.dsr_name                = s.dsr_name,
            t.dsr_type                = s.dsr_type,
            t.dsr_id                  = s.dsr_id,
            t.dsr_code                = s.dsr_code,
            t.customer_system_code    = s.customer_system_code,
            t.customer_system_name    = s.customer_system_name,
            t.province                = s.province,
            t.city                    = s.city,
            t.address                 = s.address,
            t.cooperation_status      = s.cooperation_status,
            t.darlie_area             = s.darlie_area,
            t.dealer_code             = s.dealer_code,
            t.store_category          = s.store_category,
            t.latitude                = s.latitude,
            t.longitude               = s.longitude,
            t.marked_location         = s.marked_location,
            t.marked_district         = s.marked_district,
            t.marked_city             = s.marked_city,
            t.marked_province         = s.marked_province,
            t.is_marked               = s.is_marked,
            t.nielsen_store_code      = s.nielsen_store_code,
            t.store_channel           = s.store_channel,
            t.first_sales_month       = s.first_sales_month,
            t.avg_sales_half_year     = s.avg_sales_half_year,
            t.visit_tag               = s.visit_tag,
            t.visit_tag2              = s.visit_tag2,
            t.pg_name                 = s.pg_name,
            t.is_assigned_dsr         = s.is_assigned_dsr,
            t.has_pg                  = s.has_pg,
            t.update_by               = s.update_by,
            t.update_time             = NOW();
    """
    res = execute_sql(egine=engine, mysql_str=insert_sql_str)
    res = execute_sql(egine=engine, mysql_str=update_sql_str)
    # print(res)
