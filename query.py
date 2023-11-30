# coding:utf-8

import snowflake.connector

from config.config import snowflake_config

def execute_snowflake_query(query):
    try:
        # 建立连接
        conn = snowflake.connector.connect(
            user=snowflake_config['user'],
            password=snowflake_config['password'],
            account=snowflake_config['account'],
            database=snowflake_config['database']
        )

        # 创建游标
        cur = conn.cursor()

        # 执行查询
        cur.execute(query)

        # 获取查询结果
        results = cur.fetchall()

        # 打印结果或进行其他处理
        return results

    except snowflake.connector.errors.OperationalError as e:
        print(f"Snowflake Operational Error: {e}")
        # 可以在这里处理连接失败的情况

    finally:
        # 关闭连接
        if conn:
            conn.close()


