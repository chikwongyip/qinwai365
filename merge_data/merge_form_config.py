from config import snowflake_prd_config
from create_table_sql import form_config
from create_session import create_session
if __name__ == '__main__':
    path = '/api/userDefined/v1/getUserDefined'
    method_mode = 'CREATE'
    session = create_session(snowflake_prd_config)
    sql_str = form_config(method=path, method_mode=method_mode)
    session.sql(sql_str).collect()
    session.close()
