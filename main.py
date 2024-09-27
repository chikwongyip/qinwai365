# coding:utf-8
from config.config import snowflake_prd_config
from request_qince_api import Qince_API
import datetime
import time
import json
import pandas as pd
from write_data import SaveData
if __name__ == '__main__':

    path = '/api/store/v1/queryStore'
    request_body = dict(page_number=1)
    # 获取当前时间戳
    extract_start_timestamp = int(time.time())
    extract_start_date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    res = Qince_API(path,
                    snowflake_prd_config, request_body).request_data()

    extract_end_timestamp = int(time.time())
    extract_end_date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    extract_condition = json.dumps(request_body)

    extract_result = dict(
        method=path,
        method_mode='',
        extract_start_timestamp=extract_start_timestamp,
        extract_end_timestamp=extract_end_timestamp,
        extract_start_date=extract_start_date,
        extract_end_date=extract_end_date,
        extract_condition=extract_condition,
        page_no=request_body.get("page_number"),
        cost_time=extract_end_timestamp - extract_start_timestamp,
        extracted_result=res,
        is_proccessed=False,
        is_success=True
    )
    df = pd.DataFrame([extract_result])
    # 将df column 转换成大写
    df.columns = df.columns.str.upper()
    SaveData(config=snowflake_prd_config).insert_data(
        df, "ODS.CRM.ODS_T_CRM_EXTRACT_ORIGINAL_DATA")
    # print(df)
