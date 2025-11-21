from request_qince_api import Qince_API
from config import snowflake_prd_config

if __name__ == '__main__':
    dict_data = {
        "scene": "1",
        "code": "HDHQ-20251112-000006",
        "name": "test二次陈列",
        "description": "test二次陈列",
        "start_date": "2025-10-01",
        "end_date": "2025-12-31",
        "input_items": "包柱,主货架陈列,堆头,挂网挂条,端架陈列",
        "emp_code": "tpm_acct"
    }

    res = Qince_API('/api/cuxiao/v1/modifyPlan',
                    snowflake_prd_config, dict_data).request_data()
    print(res)
