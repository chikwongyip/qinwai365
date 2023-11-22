# coding:utf-8
from datetime import datetime


def generate_timestamp():
    now = datetime.now()
    date_time = now.strftime("%Y%m%d%H%M%S")
    return date_time
