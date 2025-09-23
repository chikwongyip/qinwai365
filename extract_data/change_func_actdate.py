from request_qince_api import Qince_API
from config import snowflake_prd_config

if __name__ == '__main__':
    dict_data = {
        "scene": "regular",
        "id": "7263730607095094120",
        "start_date": "2025-10-19",
        "end_date": "2025-10-30"
    }
    res = Qince_API('/api/cuxiao/v1/modifyExeCycle',
                    snowflake_prd_config, dict_data).request_data()
    print(res)
