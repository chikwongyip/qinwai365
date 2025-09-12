from request_qince_api import Qince_API
from config import snowflake_prd_config

if __name__ == '__main__':
    dict_data = {
        "scene": "9",
        "code": "TPM003",
        "name": "测试放",
        "description": "开始测试",
        "start_date": "2025-10-01",
        "end_date": "2025-10-30",
        "input_items": "端架陈列,海报DM,包柱,海报,堆头,货架陈列",
        "emp_code": "365cs"
    }

    res = Qince_API('/api/cuxiao/v1/modifyPlan',
                    snowflake_prd_config, dict_data).request_data()
    print(res)
