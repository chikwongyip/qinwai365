from config import snowflake_prd_config
from request_qince_api import Qince_API
import datetime
import time
import json
import pandas as pd
from write_data import SaveData


def extract_data(**kwargs):

    # 获取请求参数
    path = kwargs.get('path')
    method_mode = kwargs.get('method_mode')
    page_number = kwargs.get('page_number')
    after_modify_date = kwargs.get('after_modify_date')
    before_modify_date = kwargs.get('before_modify_date')

    # 设置请求参数
    request_body = dict(page_number=page_number)

    if after_modify_date:
        request_body['after_modify_date'] = after_modify_date
    if before_modify_date:
        request_body['before_modify_date'] = before_modify_date
    # print(request_body)
    extract_start_timestamp = int(time.time())
    extract_start_date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    res = Qince_API(path,
                    snowflake_prd_config, request_body).request_data()

    # print(res.get('response_data'))
    if res.get('response_data') == '[]':
        print('第{0}页无数据'.format(page_number))
        if before_modify_date:
            # 把before_modify_date转为timestamp
            last_extract_timestamp = int(time.mktime(
                time.strptime(before_modify_date, "%Y-%m-%d %H:%M:%S")))

            delta_result = dict(
                method=path,
                method_mode=method_mode,
                last_extract_date=before_modify_date,
                last_extract_timestamp=last_extract_timestamp,
            )
            df_data = pd.DataFrame([delta_result])
            # df.columns = df.columns.str.upper()
            SaveData(config=snowflake_prd_config, data=df_data).insert_data(
                "COMMON.UTILS.COMMON_T_CRM_DELTA_TABLE")

        return False
    else:
        extract_end_timestamp = int(time.time())
        extract_end_date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        extract_condition = json.dumps(request_body)
        extract_result = dict(
            method=path,
            method_mode=method_mode,
            extract_start_timestamp=extract_start_timestamp,
            extract_end_timestamp=extract_end_timestamp,
            extract_start_date=extract_start_date,
            extract_end_date=extract_end_date,
            extract_condition=extract_condition,
            page_no=request_body.get("page_number"),
            cost_time=extract_end_timestamp - extract_start_timestamp,
            extracted_result=res.get('response_data'),
            is_proccessed=False,
            is_success=True
        )
        df = pd.DataFrame([extract_result])
        # 将df column 转换成大写
        # df.columns = df.columns.str.upper()
        SaveData(config=snowflake_prd_config, data=df).insert_data(
            "ODS.CRM.ODS_T_CRM_EXTRACT_ORIGINAL_DATA")
        print('第{0}页数据提取成功'.format(page_number))
        return True
