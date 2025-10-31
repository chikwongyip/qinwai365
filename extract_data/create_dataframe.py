def sql_select(snowflake_session, query_str):
    result = snowflake_session.sql(query_str).to_pandas()
    return result
