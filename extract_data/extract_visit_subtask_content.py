from request_qince_api import Qince_API
from create_session import create_session
from config import snowflake_prd_config, function_list
import pandas as pd
import json
from create_table import CreateTable

if __name__ == '__main__':

    functions = function_list.get('function_id', None)
    if functions:
        for id in functions:
            request_param = {
                'function_id': id,
                'date_start': "2025-03-01",
                'date_end': "2025-03-02",
                'page': '1',
                'rows': '1000'
            }
            res = Qince_API('/api/cusVisit/v1/queryCusVisitDetail',
                            snowflake_prd_config, request_param).request_data()
            if res.get('response_data', None):

                data = json.loads(res.get('response_data'))
                df_data = pd.json_normalize(data)
                print(df_data)

    # if res.get('return_code', None):
    #     # data = json.loads()

    #     data = json.loads(res.get('response_data'))
    #     df_data = pd.json_normalize(data)

    #     res = CreateTable(
    #         "ODS.CRM.ODS_T_CRM_SUBTASK_SETTINGS").create_table(df_data)
