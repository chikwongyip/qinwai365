# coding:utf-8
from datetime import datetime
import time


def generate_timestamp():
    now = datetime.now()
    date_time = now.strftime("%Y%m%d%H%M%S")
    return date_time


def timestamp_to_datetime(ts):
    """
    时间戳转日
    """
    dt = datetime.strptime(time.strftime(
        '%Y-%m-%d %H:%M:%S', time.localtime(ts)))
    return dt


def timestamp_to_datetime(timestamp):
    """
    时间戳转datetime
    :param timestamp:
    :return: datetime
    """
    result = datetime.fromtimestamp(timestamp)
    return result


def datetime_to_date(dt):
    """
    日期时间转字符串日期
    :param dt: datetime 日期时间
    :return:
    """
    d = dt.strftime("%Y-%m-%d")
    return d


def datetimestr_to_datetime(str_time):
    """
    日期转字符串
    """
    datetime = datetime.strptime(str_time, "%Y-%m-%d %H:%M:%S")
    return datetime
