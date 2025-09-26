# coding:utf-8
from request_qince_api import Qince_API
from create_session import create_session
from config import snowflake_prd_config
import json

sql_str = """
    select
    dealer_code,
    dealer_name,
    dealer_status,
    dealer_manager,
    dealer_manager_code,
    dealer_manager_waiqin365_id
from
    ods.crm.ods_t_dealer
    WHERE DEALER_CODE <> '000000001'
    and dealer_manager_waiqin365_id <> ''
    ;
"""


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


if __name__ == '__main__':

    session = create_session(snowflake_prd_config)
    df = session.sql(sql_str).to_pandas()
    df['EMPLOYEE_CODE'] = df['DEALER_MANAGER_WAIQIN365_ID'].apply(
        get_employee_code)
    df.to_excel('经销商负责人对应关系.xlsx')
