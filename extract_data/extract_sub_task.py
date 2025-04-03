from request_qince_api import Qince_API
from create_session import create_session
from config import snowflake_prd_config
import pandas as pd
import json
from create_table import CreateTable

if __name__ == '__main__':

    res = Qince_API('/api/cusVisit/v1/queryFunction',
                    snowflake_prd_config, '{}').request_data()
    # res = res.json()
    # print(res)
    if res.get('return_code', None):
        # data = json.loads()

        data = json.loads(res.get('response_data'))
        df_data = pd.json_normalize(data)

        res = CreateTable(
            "ODS.CRM.ODS_T_CRM_SUBTASK_SETTINGS").create_table(df_data)
