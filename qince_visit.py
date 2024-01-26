# coding:utf-8
from requestData import request_data

headers = {"Content-Type": "application/json"}


def get_visit_record(params, url):
    # 获取拜访主线记录
    return request_data(headers, params, url)


def get_sub_visit_record(params, url):
    # 获取拜访子线记录
    return request_data(headers, params, url)


def get_visit_record_detail(params, url):
    # 获取拜访记录详情
    return request_data(headers, params, url)
