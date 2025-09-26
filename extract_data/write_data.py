# coding:utf-8
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas


class SaveData:
    def __init__(self, config, data):
        self.conn = snowflake.connector.connect(
            user=config.get('user'), password=config.get('password'), account=config.get('account'))

        self.data = data
        self.data.columns = self.data.columns.str.upper()

    def insert_data(self, table, overwrite=False, auto_create=False):
        database, schema, table_name = table.split(".")
        write_pandas(
            conn=self.conn,
            df=self.data,
            overwrite=overwrite,
            table_name=table_name,
            database=database,
            schema=schema,
            auto_create_table=auto_create
        )
