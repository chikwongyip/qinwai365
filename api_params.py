# coding:utf-8
# 请求传参
class Params:
    # 初始化
    def __init__(self, **kwargs):
        for key in kwargs:
            self['key'] = kwargs[key]
