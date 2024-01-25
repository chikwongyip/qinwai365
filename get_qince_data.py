# coding:utf-8
from requestData import request_data
from api_params import CusVisitRecord

def get_visit_record:
    params = CusVisitRecord({
    "date_start": "2017-01-10",
    "date_end": "2017-01-20",
    "modify_start": "2017-01-10",
    "modify_end": "2017-01-20",
    "visitor_code": "",
    "page":"1",
    "rows":"1000"
    })
    request_data('')
