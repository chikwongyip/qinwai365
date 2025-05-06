from get_last_update_time import get_last_extract_time
from request_qince_api import Qince_API
from create_session import create_session
from config import snowflake_prd_config, function_list
import pandas as pd
import json
import datetime
from create_table import CreateTable
from dynamic_merge import dynamic_merge
import numpy as np
from snowflake.connector.pandas_tools import write_pandas
import snowflake.connector
PREFIX_TABLE_NAME = 'ODS.CRM.ODS_T_CRM_SUBTASK_CONTENT'


class Extract_Subtaks_Content:
    def __init__(self,  date_start, config):
        # self.function_list = function_list
        self.date_start = date_start
        self.date_end = datetime.datetime.today().strftime('%Y-%m-%d')
        # self.page = 1
        self.rows = 1000
        self.session = create_session(config)
        self.conn = self.create_snowflake_conn(config)

    def create_snowflake_conn(self, config):

        return snowflake.connector.connect(
            user=config.get('user'), password=config.get('password'), account=config.get('account'))

    def get_function_list(self):
        query = """
                select distinct
                    function_id,
                    table_name
                from
                    ods.crm.ods_t_crm_subtask_settings;"""
        res = self.session.sql(query).collect()
        return res

    def table_exists(self, table_name: str) -> bool:
        """
        检查表是否存在
        :param session: Snowpark Session 对象
        :param table_name: 表名（区分大小写）
        :param schema_name: Schema 名（可选，默认为当前 Session 的 Schema）
        :param database_name: 数据库名（可选，默认为当前 Session 的 Database）
        :return: True 表示存在，False 表示不存在
        """
        database_name = table_name.split('.')[0]
        schema_name = table_name.split('.')[1]
        table_name = table_name.split('.')[2]
        # 动态构造查询条件
        conditions = []
        if database_name:
            conditions.append(f"table_catalog = '{database_name.upper()}'")
        if schema_name:
            conditions.append(f"table_schema = '{schema_name.upper()}'")
        conditions.append(f"table_name = '{table_name.upper()}'")

        query = f"""
            SELECT COUNT(*) AS CNT
            FROM INFORMATION_SCHEMA.TABLES
            WHERE {" AND ".join(conditions)}
        """

        result = self.session.sql(query).collect()
        return result[0]['CNT'] > 0

    def extract_data(self):

        function_lists = self.get_function_list()
        print(function_lists)
        for function_list in function_lists:
            page = 1
            function_id, table_name = function_list
            keep = True
            print('正在获取function_id为{0}的数据'.format(function_id))
            print('正在获取table_name为{0}的数据'.format(table_name))
            full_table_name = PREFIX_TABLE_NAME + '_' + table_name.upper()
            full_table_name_tmp = PREFIX_TABLE_NAME + '_' + table_name.upper() + '_TMP'
            while keep:

                print('正在获取第{0}页数据'.format(page))
                request_param = {
                    'function_id': function_id,
                    'date_start': self.date_start,
                    'date_end': datetime.datetime.today().strftime('%Y-%m-%d'),
                    # 'date_end': self.date_start,
                    'page': page,
                    'rows': self.rows
                }
                print(request_param)
                res = Qince_API('/api/cusVisit/v1/queryCusVisitDetail',
                                snowflake_prd_config, request_param).request_data()

                res = res.get('response_data', None)

                if res:

                    if res != '[]':
                        print('第{0}页数据获取成功'.format(page))
                        df_data = pd.json_normalize(json.loads(res))
                        df_data['function_id'] = function_id
                        df_data.columns = [col.upper()
                                           for col in df_data.columns]
                        snow_cols = self.get_column(full_table_name)
                        # print(df_data)
                        for col in snow_cols:
                            if col not in df_data.columns:
                                df_data[col] = ''
                        # print(df_data)
                        # 合并数据前检查table 是否存在
                        if self.table_exists(full_table_name):

                            CreateTable(
                                full_table_name_tmp).create_table(df_data)
                            dynamic_merge(session=self.session, target_table_name=full_table_name,
                                          source_table_name=full_table_name_tmp, keys=['CREATE_TIME', 'VISIT_IMPLEMENT_ID', 'FUNCTION_ID', 'ID'])
                        else:
                            CreateTable(full_table_name).create_table(df_data)
                        page += 1

                    else:
                        print('第{0}页没有数据'.format(page))
                        keep = False
                else:
                    print('第{0}页数据获取失败'.format(page))
                    keep = False

    def insert_data(self, data, table):
        database, schema, table_name = table.split(".")
        write_pandas(
            conn=self.conn,
            df=data,
            overwrite=False,
            table_name=table_name,
            database=database,
            schema=schema,
        )

    def get_column(self, full_table_name):
        query = """
                select *
                from
                    {0} limit 1;""".format(full_table_name)
        res = self.session.sql(query).to_pandas()
        return res.columns


if __name__ == '__main__':
    method = '/api/cusVisit/v1/queryCusVisitDetail'
    method_mode = 'VISIT'
    last_extract_date = get_last_extract_time(config=snowflake_prd_config,
                                              method='/api/cusVisit/v1/queryCusVisitDetail',
                                              method_mode='VISIT')

    last_extract_date_str = last_extract_date.strftime('%Y-%m-%d %H:%M:%S')
    last_extract_date_str = last_extract_date_str.split(' ')[0]
    delta_extract_date_str = datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')
    delta_extract_timestamp = datetime.datetime.today().timestamp()
    extract_handle = Extract_Subtaks_Content(
        date_start=last_extract_date_str, config=snowflake_prd_config)
    extract_handle.extract_data()
    data = [method, method_mode, '', delta_extract_date_str,
            int(delta_extract_timestamp), '', 0]
    df_delta_data = pd.DataFrame(
        np.array(
            data
        ).reshape(1, 7),
        columns=[
            "METHOD",
            "METHOD_MODE",
            "TOKEN",
            "LAST_EXTRACT_DATE",
            "LAST_EXTRACT_TIMESTAMP",
            "EXTRACT_FUNCTION",
            "COST_TIME",
        ],
    )
    extract_handle.insert_data(
        df_delta_data, 'COMMON.UTILS.COMMON_T_CRM_DELTA_TABLE')
