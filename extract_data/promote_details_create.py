from request_qince_api import Qince_API
from config import snowflake_prd_config

if __name__ == '__main__':
    dict_data = {
        "planCode": "HDHQ-20251111-000018",
        "projectApplyCode": "",
        "activityCode": "HDHQ-20251111-000018-001_9100002345",
        "applicantCode": "tpm_acct",
        "applyName": "test二次陈列_HDHQ-20251111-000018-001_9100002345",
        "subList": [
            {
                    "cusCode": "9100002345",
                    "startDate": "2025-11-01",
                    "endDate": "2025-12-31",
                    "itemName": "挂网挂条",
                    "budgetAmount": "1000.0000",
                    "payMode": "",
                    "remark": "测试"
            },
            {
                "cusCode": "9100002345",
                "startDate": "2025-11-01",
                "endDate": "2025-12-31",
                "itemName": "堆头",
                "budgetAmount": "1000.0000",
                "payMode": "",
                "remark": "测试"
            },
            {
                "cusCode": "9100002345",
                "startDate": "2025-11-01",
                "endDate": "2025-12-31",
                "itemName": "端架陈列",
                "budgetAmount": "1000.0000",
                "payMode": "",
                "remark": "测试"
            }
        ]
    }

    # dict_data = {
    #     "planCode": "TPM003",
    #     "projectApplyCode": "",
    #     "activityCode": "TPM_FUNC_20250912_2",
    #     "applicantCode": "365cs",
    #     "applyName": "好来化工速销活动",

    #     "subList": [{"cusCode": "POS000002",

    #                  "startDate": "2025-10-02",
    #                  "endDate": "2025-10-10",
    #                  "itemName": "堆头",

    #                  "budgetAmount": "10",
    #                  "payMode": "",
    #                  "remark": "hey man"

    #                  }]
    # }
    res = Qince_API('/api/cuxiao/v1/addRegularActivity',
                    snowflake_prd_config, dict_data).request_data()
    print(res)
