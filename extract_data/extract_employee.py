# coding:utf-8
import json
import datetime
import pandas as pd
from datetime import timedelta
from request_qince_api import Qince_API
from create_session import create_session
from config import snowflake_prd_config
from write_data import SaveData


def get_employee_code(id):
    dict_data = {"id": id}
    print(id)
    res = Qince_API('/api/employee/v3/queryEmployee',
                    snowflake_prd_config, dict_data).request_data()
    # eval(res.get('response_data'))
    if res.get('response_data'):
        # print(type(res.get('response_data')))
        result = json.loads(res.get('response_data'))
        # result = eval(res.get('response_data'))
        employee_code = result[0].get('employee_code')

        return employee_code


def get_employee_data(create_date):
    create_date_str = create_date.strftime("%Y-%m-%d")
    print(create_date_str)
    dict_data = {"create_date": create_date_str}
    # print(id)
    res = Qince_API('/api/employee/v3/queryEmployee',
                    snowflake_prd_config, dict_data).request_data()
    return res.get('response_data')


if __name__ == '__main__':

    # session = create_session(snowflake_prd_config)
    keep = True
    day_str = '2023-11-10'
    lv_day = datetime.datetime.strptime(day_str, "%Y-%m-%d").date()
    date_now = datetime.datetime.today().date()
    # res = get_employee_data(lv_day)
    # data_json = json.loads(res)
    # df = pd.DataFrame(data_json)
    # print(df)
    data = pd.DataFrame()
    while keep:
        res = get_employee_data(lv_day)
        if res == '[]':
            lv_day = lv_day + timedelta(days=1)
            continue
        # print(res)
        df = pd.DataFrame(json.loads(res))
        data = pd.concat([data, df], axis=0)
        if lv_day > date_now:
            keep = False
        lv_day = lv_day + timedelta(days=1)

    if data.empty == False:
        data.columns = [i.upper() for i in data.columns]
        SaveData(config=snowflake_prd_config, data=data).insert_data(
            "ODS.CRM.")
