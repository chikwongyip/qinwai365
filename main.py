# coding:utf-8

import pandas as pd
from query import execute_snowflake_query

if __name__ == '__main__':
    query = 'select * from ODS.CRM.ODS_T_DEALER'
    result = execute_snowflake_query(query)
    dealers = pd.DataFrame(result)
    print(dealers)

