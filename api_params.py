# coding:utf-8

class Params:
    # 初始化
    def __init__(self, **kwargs):
        for key in kwargs:
            self['key'] = kwargs[key]


#   拜访记录传入参数
