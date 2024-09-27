# coding:utf-8
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas


class SaveData:
    def __init__(self, config):
        self.conn = snowflake.connector.connect(
            user=config.get('user'), password=config.get('password'), account=config.get('account'))

    def insert_data(self, df, table):
        database, schema, table_name = table.split(".")
        write_pandas(
            conn=self.conn,
            df=df,
            overwrite=False,
            table_name=table_name,
            database=database,
            schema=schema,
        )
