from request_qince_api import Qince_API
from create_session import create_session
from config import snowflake_prd_config, function_list
import pandas as pd
import json
import datetime
from create_table import CreateTable
from dynamic_merge import dynamic_merge


class Extract_Subtaks_Content:
    def __init__(self, function_list, date_start, config):
        self.function_list = function_list
        self.date_start = date_start
        self.date_end = datetime.datetime.today().strftime('%Y-%m-%d')
        self.page = 1
        self.rows = 1000
        self.session = create_session(config)

    def extract_data(self):
        keep = True
        for function_id in self.function_list.get('function_id', []):
            print('正在获取function_id为{0}的数据'.format(function_id))
            while keep:

                print('正在获取第{0}页数据'.format(self.page))
                request_param = {
                    'function_id': function_id,
                    'date_start': self.date_start,
                    'date_end': datetime.datetime.today().strftime('%Y-%m-%d'),
                    # 'date_end': '2025-03-02',
                    'page': self.page,
                    'rows': self.rows
                }

                res = Qince_API('/api/cusVisit/v1/queryCusVisitDetail',
                                snowflake_prd_config, request_param).request_data()

                res = res.get('response_data', None)
                # print(res)
                if res:

                    if res != '[]':
                        print('第{0}页数据获取成功'.format(self.page))
                        df_data = pd.json_normalize(json.loads(res))
                        df_data['function_id'] = function_id
                        df_data.columns = [col.upper()
                                           for col in df_data.columns]
                        CreateTable('ODS.CRM.ODS_T_CRM_SUBTASK_CONTENT_' +
                                    str(function_id)+'_TMP').create_table(df_data)
                        dynamic_merge(session=self.session, target_table_name='ODS.CRM.ODS_T_CRM_SUBTASK_CONTENT'+'_'+function_id,
                                      source_table_name='ODS.CRM.ODS_T_CRM_SUBTASK_CONTENT_'+str(function_id)+'_TMP', keys=['CREATE_TIME', 'VISIT_IMPLEMENT_ID', 'FUNCTION_ID', 'ID'])

                        self.page += 1

                    else:
                        print('第{0}页没有数据'.format(self.page))
                        keep = False
                else:
                    print('第{0}页数据获取失败'.format(self.page))
                    keep = False


if __name__ == '__main__':

    extract_handle = Extract_Subtaks_Content(
        function_list=function_list, date_start='2025-03-01', config=snowflake_prd_config)
    extract_handle.extract_data()
