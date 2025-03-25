from request_qince_api import Qince_API
from create_session import create_session
from config import snowflake_prd_config
from select_mtd import sql_str
# import json


def insert_form(row):
    print(type(row[0]))
    dict_data = eval(row[0])
    # print(dict_data)
    res = Qince_API('/api/userDefined/v1/modifyUserDefined',
                    snowflake_prd_config, dict_data).request_data()
    print(res)


if __name__ == '__main__':

    session = create_session(snowflake_prd_config)
    df = session.sql(sql_str).to_pandas()
    # print(df)
    # print(df)
    df.apply(insert_form, axis=1)
