# coding:utf-8
from snowflake.snowpark import Session


def create_session(params):
    snowflake_session = Session.builder.configs(params).create()
    return snowflake_session
