# coding:utf-8

class Params:
    # 初始化
    def __init__(self, page_number):
        self.page_number = page_number


#   拜访记录传入参数
class CusVisitRecord:

    def __init__(self, **kwargs):
        self.page = kwargs['id']
        self.date_start = kwargs['date_start']
        self.date_end = kwargs['date_end']
        self.visitor_code = kwargs['visitor_code']
        self.rows = 1000
