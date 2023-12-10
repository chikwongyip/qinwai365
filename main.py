# coding:utf-8
import json

# import pandas as pd
from query import execute_snowflake_query
from requestData import request_data

if __name__ == '__main__':
    # sql_string = """
    #     SELECT
    #     ODS_T_DEALER.ID,
    #     VALUE:dealer_ext_column::string as dealer_ext_column,
    #     VALUE:dealer_ext_key::string as dealer_ext_key,
    #     VALUE:dealer_ext_value::string as dealer_ext_value
    # FROM
    #     ODS.CRM.ODS_T_DEALER,
    #     LATERAL FLATTEN(input => PARSE_JSON(VARIANT)) AS json_data
    # WHERE ODS.CRM.ODS_T_DEALER.ID = '6745402878445459855'
    # """
    # result = execute_snowflake_query(sql_string)
    # df = pd.DataFrame(result, columns=['ID', 'dealer_ext_column', 'dealer_ext_key', 'dealer_ext_value'])
    # tmp = df.set_index(['ID']).stack()
    # tmp2 = tmp.rename_axis(index=['dealer_ext_column', 'dealer_ext_key'])
    # tmp2.name = 'dealer_ext_value'
    # tmp2.reset_index()
    # print(tmp2)

    # url = 'https://openapi.waiqin365.com/api/store/v1/queryStore'
    headers = {"Content-Type": "application/json"}
    page_number = 1
    response = request_data('https://openapi.waiqin365.com/api/store/v1/queryStore', headers=headers,
                            page_number=page_number)
    sql_string = f"""
        INSERT INTO KETTLE_TEST.PUBLIC.ODS_T_STORE_JSON (PAGE_NUMBER, RESPONSE_DATA) VALUES ( ' {page_number}', '{json_string}')
    """
    result = execute_snowflake_query(sql_string)
