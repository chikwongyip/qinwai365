from request_qince_api import Qince_API
from config import snowflake_prd_config

if __name__ == '__main__':
    dict_data = {
        "activity_code": "ZHHTMP001_09_23_01"
    }
    res = Qince_API('/api/cuxiao/v1/queryRegularSaleActivities',
                    snowflake_prd_config, dict_data).request_data()
    print(res)

# coding: utf-8
# from extract_handler import extract_handler

# if __name__ == '__main__':
#     # path = '/api/store/v1/queryStore'
#     #     method_mode = 'MODIFY'
#     extract_handler(path='/api/cuxiao/v1/queryRegularSale',
#                     method_mode='CREATE')
#     # extract_handler(path='/api/store/v1/queryStore', method_mode='CREATE')
