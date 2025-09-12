from request_qince_api import Qince_API
from config import snowflake_prd_config

if __name__ == '__main__':
    # dict_data = {
    #     "planCode": "TPM003",
    #     "projectApplyCode": "",
    #     "activityCode": "TPM_FUNC_20250912_1",
    #     "applicantCode": "365cs",
    #     "applyName": "好来化工速销活动",

    #     "subList": [{"cusCode": "POS024314",

    #                  "startDate": "2025-10-02",
    #                  "endDate": "2025-10-10",
    #                  "itemName": "端架陈列",

    #                  "budgetAmount": "10",
    #                  "payMode": "",
    #                  "remark": "hey man"

    #                  }, {"cusCode": "POS016818",

    #                      "startDate": "2025-10-11",
    #                      "endDate": "2025-10-25",
    #                      "itemName": "海报DM",

    #                      "budgetAmount": "20",
    #                      "payMode": "",
    #                      "remark": "TPM"

    #                      }]
    # }
    dict_data = {
        "planCode": "TPM003",
        "projectApplyCode": "",
        "activityCode": "TPM_FUNC_20250912_2",
        "applicantCode": "365cs",
        "applyName": "好来化工速销活动",

        "subList": [{"cusCode": "POS000002",

                     "startDate": "2025-10-02",
                     "endDate": "2025-10-10",
                     "itemName": "堆头",

                     "budgetAmount": "10",
                     "payMode": "",
                     "remark": "hey man"

                     }]
    }
    res = Qince_API('/api/cuxiao/v1/addRegularActivity',
                    snowflake_prd_config, dict_data).request_data()
    print(res)
