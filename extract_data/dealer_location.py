from request_qince_api import Qince_API
from config import snowflake_prd_config
import json
import pandas as pd
import time
if __name__ == '__main__':
    dict_data = {
        "cm_codes": "9102003831"

    }
    res = Qince_API('/api/customer/v1/queryCustomerLocation',
                    snowflake_prd_config, dict_data).request_data()
    print(res)
    # ReturnData = json.loads(res)
    # df = pd.DataFrame(ReturnData)
    # df['UPDATE_TIMESTAMP'] = time.strftime("%Y%m%d%H%M%S")
    # df['UPDATE_DATE'] = time.strftime("%Y-%m-%d", time.localtime())
    # df = df[['cm_code',
    #          'location_a',
    #          'mss_province',
    #          'cm_id',
    #          'cm_name',
    #          'id',
    #          'mss_area',
    #          'mss_city',
    #          'location_c',
    #          'UPDATE_TIMESTAMP',
    #          'UPDATE_DATE',
    #          'poi_address',
    #          'poi_name',
    #          'poi_id',
    #          'poi_location']]
    # print(df)
