# coding:utf-8
from snowflake.snowpark import Session


def create_session(params):
    config = {
        "user": params.get("user"),
        "password": params.get("password"),
        "account": params.get("account"),
        "database": params.get("database"),
        "schema": params.get("schema"),
    }
    snowflake_session = Session.builder.configs(config).create()
    return snowflake_session
