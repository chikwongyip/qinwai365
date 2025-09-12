# from request_qince_api import Qince_API
# from config import snowflake_prd_config

# if __name__ == '__main__':
#     dict_data = {
#         "create_start": "2025-09-01 00:00:00",
#         "create_end": "2025-09-11 00:00:00"
#     }
#     res = Qince_API('/api/cuxiao/v1/queryRegularSale',
#                     snowflake_prd_config, dict_data).request_data()
#     print(res)

# coding: utf-8
from extract_handler import extract_handler

if __name__ == '__main__':
    # path = '/api/store/v1/queryStore'
    #     method_mode = 'MODIFY'
    extract_handler(path='/api/cuxiao/v1/queryRegularSale',
                    method_mode='CREATE')
    # extract_handler(path='/api/store/v1/queryStore', method_mode='CREATE')
