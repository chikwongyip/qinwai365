# coding:utf-8
from snowflake.snowpark import Session
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas


class CreateTable:

    def __init__(self, table_name):
        self.database, self.schema, self.table_name = table_name.split('.')

    #   print('database:',self.database,'schema:',self.schema,'table_name:',self.table_name)
    def create_session(self):
        connection_parameters = {
            "user": "ETL_USER",
            "password": "WD6gvmPY",
            "account": "ul51368.ap-southeast-1",
            "database": self.database,
            "schema": self.schema,
        }
        snowflake_session = Session.builder.configs(
            connection_parameters).create()
        return snowflake_session

    def sql_select(self, snowflake_session, query_str):
        result = snowflake_session.sql(query_str).to_pandas()
        snowflake_session.close()
        return result

    def create_connect(self):
        db_conn = snowflake.connector.connect(
            user="ETL_USER", password="WD6gvmPY", account="ul51368.ap-southeast-1"
        )
        return db_conn

    def create_table(self, df):
        df.columns = [k.upper() for k in df.columns]
        conn = self.create_connect()
        write_pandas(
            conn=conn,
            df=df,
            overwrite=True,
            auto_create_table=True,
            table_name=self.table_name,
            database=self.database,
            schema=self.schema,
        )
        conn.close()
