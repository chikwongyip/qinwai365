from request_qince_api import Qince_API
from config import snowflake_prd_config

if __name__ == '__main__':
    dict_data = {
        "activityCode": "TPM_FUNC_20250912_2",
        "applicantCode": "365cs"
    }
    res = Qince_API('/api/cuxiaoTpm/v1/tpDelActivity',
                    snowflake_prd_config, dict_data).request_data()
    print(res)
