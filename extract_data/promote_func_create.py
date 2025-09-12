from request_qince_api import Qince_API
from config import snowflake_prd_config

if __name__ == '__main__':
    dict_data = {
        "code": "TPM003",
        "name": "TPM_测试20250912",
        "planTypeName": "陈列活动",
        "description": "这是方案说明",
        "startDate": "2025-09-11",
        "endDate": "2025-09-20",
        "recordFormName": "陈列拍照",
        "visitExeFlag": "0",
        "recordAuditFormName": "陈列评价",
        "planBudgetAmount": 70.2,
        "empCode": "365cs",
        "applyRangeDeptCode": "好来化工",
        "planItemMode": "1",
        "itemNames": "端架陈列,海报DM,包柱,海报,堆头,货架陈列"
        # "itemCostStdList": [
        #     {
        #         "itemName": "端架陈列",
        #         "costStdName": "端架陈列",
        #         "redemptionAmount": "50.33"
        #     }
        # ]
    }
    res = Qince_API('/api/cuxiao/v1/addRegularPlan',
                    snowflake_prd_config, dict_data).request_data()
    print(res)
