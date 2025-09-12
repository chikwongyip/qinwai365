from request_qince_api import Qince_API
from config import snowflake_prd_config

if __name__ == '__main__':
    dict_data = {
        "activityCode": "TPM_FUNC_20250912_1",
        "cusCode": "POS016818",
        "empCode": "365cs"
    }
    res = Qince_API('/api/cuxiao/v1/removeRegularCus',
                    snowflake_prd_config, dict_data).request_data()
    print(res)
