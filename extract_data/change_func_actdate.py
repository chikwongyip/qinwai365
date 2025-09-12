from request_qince_api import Qince_API
from config import snowflake_prd_config

if __name__ == '__main__':
    dict_data = {
        "scene": "regular",
        "id": "7007658823334359063",
        "start_date": "2025-10-07",
        "end_date": "2025-10-19"
    }
    res = Qince_API('/api/cuxiao/v1/modifyExeCycle',
                    snowflake_prd_config, dict_data).request_data()
    print(res)
