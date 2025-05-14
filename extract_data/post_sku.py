from request_qince_api import Qince_API
from create_session import create_session
from config import snowflake_prd_config
from select_data import select_sku


def insert_update_sku(row):
    print('正在执行SKU:', row['prd_code'])
    dict_data = eval(row['J'])
    res = Qince_API('/api/product/v1/modifyProduct',
                    snowflake_prd_config, dict_data).request_data()
    return_code = res.get('return_code', None)
    if return_code == None or return_code != '0':
        res = Qince_API('/api/product/v1/addProduct',
                        snowflake_prd_config, dict_data).request_data()
    # if res.get('return_code', None) == None:
    #     res = Qince_API('/api/product/v1/modifyProduct',
    #                     snowflake_prd_config, dict_data).request_data()
    print(res)


if __name__ == '__main__':

    session = create_session(snowflake_prd_config)
    df = session.sql(select_sku()).to_pandas()
    # print(df)
    df.apply(insert_update_sku, axis=1)
