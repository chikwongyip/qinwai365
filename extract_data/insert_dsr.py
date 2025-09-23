from request_qince_api import Qince_API
from create_session import create_session
from config import snowflake_prd_config
from select_data import select_dsr


def update_form(row):
    dict_data = eval(row[0])
    res = Qince_API('/api/userDefined/v1/addUserDefined',
                    snowflake_prd_config, dict_data).request_data()

    if res.get('return_code', None) == None:
        res = Qince_API('/api/userDefined/v1/addUserDefined',
                        snowflake_prd_config, dict_data).request_data()
    print(res)


if __name__ == '__main__':

    session = create_session(snowflake_prd_config)
    df = session.sql(select_dsr).to_pandas()
    df.apply(update_form, axis=1)
