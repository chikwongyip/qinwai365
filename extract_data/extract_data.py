from config import snowflake_prd_config
from request_qince_api import Qince_API
import datetime
import time
import json
import pandas as pd
from write_data import SaveData
from extract_strategies import get_strategy


def extract_data(**kwargs) -> bool:
    """
    Extract data from Qince API based on provided parameters and save to Snowflake.

    Args:
        **kwargs: Arbitrary keyword arguments.
            path (str): API endpoint path.
            method_mode (str): 'CREATE' or 'MODIFY'.
            page_number (int): Page number to fetch.
            after_modify_date (str): Start date for modification/creation range.
            before_modify_date (str): End date for modification/creation range.
            form_id (str, optional): Form ID for user defined forms.
            function_id (str, optional): Function ID for visit records.

    Returns:
        bool: True if data was extracted and saved, False if no data was found.
    """

    # 获取请求参数
    path = kwargs.get('path')
    method_mode = kwargs.get('method_mode')
    page_number = kwargs.get('page_number')
    after_modify_date = kwargs.get('after_modify_date')
    before_modify_date = kwargs.get('before_modify_date')
    form_id = kwargs.get('form_id')
    function_id = kwargs.get('function_id')
    method = path
    method = path
    # 设置请求参数
    strategy = get_strategy(path)
    request_body = strategy.construct_body(**kwargs)

    # Special handling for method name modification (preserved from original logic)
    if path == '/api/userDefined/v1/queryUserDefined' and form_id:
        method = method + '-' + form_id
    elif path == '/api/cusVisit/v1/getVisitRecordApprovalData' and function_id:
        method = method + '-' + function_id
    extract_start_timestamp = int(time.time())
    extract_start_date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    # print(request_body)
    res = Qince_API(path,
                    snowflake_prd_config, request_body).request_data()

    # print(res.get('response_data'))
    if res.get('response_data') == '[]':
        print('第{0}页无数据'.format(page_number))
        if before_modify_date:
            if path == '/api/userDefined/v1/queryUserDefined' or path == '/api/cusVisit/v1/getVisitRecordApprovalData':
                tmp = before_modify_date + ' 00:00:00'
            else:
                tmp = before_modify_date
            # 把before_modify_date转为timestamp
            last_extract_timestamp = int(time.mktime(
                time.strptime(tmp, "%Y-%m-%d %H:%M:%S")))

            delta_result = dict(
                method=method,
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
            method=method,
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


def extract_data_visit(**kwargs) -> bool:
    """
    Extract visit or approval data from Qince API.

    Args:
        **kwargs: Arbitrary keyword arguments.
            path (str): API endpoint path.
            method_mode (str): 'VISIT' or other modes.
            page_number (int): Page number to fetch.
            start_date (str): Start date for filtering.
            end_date (str): End date for filtering.

    Returns:
        bool: True if data was extracted and saved, False if no data was found.
    """

    # 获取请求参数
    path = kwargs.get('path')
    method_mode = kwargs.get('method_mode')
    page_number = kwargs.get('page_number')
    start_date = kwargs.get('start_date')
    end_date = kwargs.get('end_date')

    # 设置请求参数
    request_body = dict(page=page_number)

    if method_mode == 'VISIT':
        if start_date:
            request_body['visit_start_date'] = start_date.split(' ')[0]
        if end_date:
            request_body['visit_end_date'] = end_date.split(' ')[0]
    else:
        if start_date:
            request_body['approval_start_date'] = start_date.split(' ')[0]
        if end_date:
            request_body['approval_end_date'] = end_date.split(' ')[0]

    request_body['rows'] = '900'

    extract_start_timestamp = int(time.time())
    extract_start_date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    # print(request_body)

    res = Qince_API(path,
                    snowflake_prd_config, request_body).request_data()
    # print(res)
    if res.get('response_data') == '[]':
        print('第{0}页无数据'.format(page_number))
        if end_date:
            # 把before_modify_date转为timestamp
            last_extract_timestamp = int(time.mktime(
                time.strptime(end_date, "%Y-%m-%d %H:%M:%S")))

            delta_result = dict(
                method=path,
                method_mode=method_mode,
                last_extract_date=end_date,
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


def extract_data_form(**kwargs) -> bool:
    """
    Extract form configuration data from Qince API.

    Args:
        **kwargs: Arbitrary keyword arguments.
            path (str): API endpoint path.
            method_mode (str): Method mode.
            form_id (str): Form ID to fetch configuration for.

    Returns:
        bool: True if data was extracted and saved, False otherwise.
    """

    # 获取请求参数
    path = kwargs.get('path')
    method_mode = kwargs.get('method_mode')
    form_id = kwargs.get('form_id')

    # 设置请求参数
    request_body = dict(form_id=form_id)

    extract_start_timestamp = int(time.time())
    extract_start_date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    # print(request_body)

    res = Qince_API(path,
                    snowflake_prd_config, request_body).request_data()
    # print(res)
    if res.get('response_data') == '[]':
        return False
    else:
        extract_end_timestamp = int(time.time())
        extract_end_date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        extract_condition = json.dumps(request_body)
        extract_result = dict(
            method=path+'-'+form_id,
            method_mode=method_mode,
            extract_start_timestamp=extract_start_timestamp,
            extract_end_timestamp=extract_end_timestamp,
            extract_start_date=extract_start_date,
            extract_end_date=extract_end_date,
            extract_condition=extract_condition,
            page_no=1,
            cost_time=1,
            extracted_result=res.get('response_data'),
            is_proccessed=False,
            is_success=True
        )
        df = pd.DataFrame([extract_result])
        # 将df column 转换成大写
        # df.columns = df.columns.str.upper()
        SaveData(config=snowflake_prd_config, data=df).insert_data(
            "ODS.CRM.ODS_T_CRM_EXTRACT_ORIGINAL_DATA")
        print('获取表单配置完成！')
        return True
