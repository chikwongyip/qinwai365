from request_qince_api import Qince_API
from config import snowflake_prd_config

if __name__ == '__main__':
    dict_data = {
        "code": "ZHHTMP001",
        "name": "好来化工速销活动-0923_1",
        "planTypeName": "陈列活动",
        "description": "好来化工速销活动-0923_1",
        "startDate": "2025-10-01",
        "endDate": "2025-10-30",
        "recordFormName": "陈列拍照",
        "visitExeFlag": "0",
        "recordAuditFormName": "陈列评价",
        "planBudgetAmount": 100.2,
        "empCode": "365cs",
        "applyRangeDeptCode": "1",
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
