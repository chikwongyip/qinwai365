# coding:utf-8

def dynamic_merge(**kwargs):
    """
    动态生成 merge SQL 并执行
    """
    session = kwargs.get('session')  # param session: Snowflake session
    target_table_name = kwargs.get(
        'target_table_name')  # target_table_name: 目标表名称
    source_table_name = kwargs.get('source_table_name')
    data = kwargs.get('data')  # data: 待合并的 Snowpark DataFrame
    keys = kwargs.get('keys')  # keys: 用于 join 的列名列表

    # 获取目标表的 Snowpark DataFrame
    target_table = session.table(target_table_name)

    # 列出待合并数据的所有列
    data_columns = data.columns
    # print(data_columns)
    # 生成合并条件 (ON 条件)
    merge_condition = " AND ".join(
        [f"target.{col} = source.{col}" for col in keys])
    # print(merge_condition)
    # 生成更新部分 (UPDATE SET)
    update_set_clause = ", ".join(
        [f"target.{col} = source.{col}" for col in data_columns])

    # 生成插入部分 (INSERT INTO)
    insert_columns = ", ".join(data_columns)
    insert_values = ", ".join([f"source.{col}" for col in data_columns])

    # 生成完整的 MERGE INTO SQL
    merge_sql = f"""
    MERGE INTO {target_table_name} AS target
    USING {source_table_name} AS source
    ON {merge_condition}
    WHEN MATCHED THEN
      UPDATE SET {update_set_clause}
    WHEN NOT MATCHED THEN
      INSERT ({insert_columns})
      VALUES ({insert_values})
    """
    # print(merge_sql)

    # 执行 MERGE SQL
    session.sql(merge_sql).collect()
