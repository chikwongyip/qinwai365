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
    form_id = kwargs.get('form_id')
    function_id = kwargs.get('function_id')
    method = path
    # 设置请求参数
    if path == '/api/userDefined/v1/queryUserDefined':
        request_body = dict(page=page_number)
        request_body['rows'] = 1000
        if form_id:
            request_body['form_id'] = form_id
            method = method + '-' + form_id
        if method_mode == 'CREATE':
            if after_modify_date:
                request_body['date_start'] = after_modify_date
            if before_modify_date:
                request_body['date_end'] = before_modify_date
        else:
            if after_modify_date:
                request_body['modify_date_start'] = after_modify_date
            if before_modify_date:
                request_body['modify_date_end'] = before_modify_date
    elif path == '/api/cusVisit/v1/getVisitRecordApprovalData':
        request_body = dict(page=page_number)
        request_body['rows'] = 1000
        if function_id:
            request_body['function_id'] = function_id
            method = method + '-' + function_id
        if method_mode == 'CREATE':
            if after_modify_date:
                request_body['create_date_start'] = after_modify_date
            if before_modify_date:
                request_body['create_date_end'] = before_modify_date
        else:
            if after_modify_date:
                request_body['approve_date_start'] = after_modify_date
            if before_modify_date:
                request_body['approve_date_end'] = before_modify_date
    elif path == '/api/cuxiao/v1/queryRegularSale':
        request_body = dict(page_number=page_number)
        if method_mode == 'CREATE':
            request_body['create_start'] = after_modify_date
            request_body['create_end'] = before_modify_date
        else:
            request_body['modify_start'] = after_modify_date
            request_body['modify_end'] = before_modify_date
    elif path == '/api/employee/v3/queryEmployee':
        request_body = dict(page_number=page_number)
        if method_mode == 'CREATE':
            request_body['create_date'] = kwargs.get('create_date')

        else:
            request_body['modify_date'] = kwargs.get('modify_date')

    else:
        request_body = dict(page_number=page_number)
        if method_mode == 'CREATE':
            if after_modify_date:
                request_body['after_create_date'] = after_modify_date
            if before_modify_date:
                request_body['before_create_date'] = before_modify_date
        else:
            if after_modify_date:
                request_body['after_modify_date'] = after_modify_date
            if before_modify_date:
                request_body['before_modify_date'] = before_modify_date
        # print(request_body)
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


def extract_data_visit(**kwargs):

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


def extract_data_form(**kwargs):

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
