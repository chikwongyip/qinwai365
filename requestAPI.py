# coding:utf-8
import requests


class RequestHandler:
    def get(self, url, **kwargs):
        """封装get方法"""
        params = kwargs.get("params")
        headers = kwargs.get("headers")
        try:
            result = requests.get(url, params=params, headers=headers)
            return result
        except Exception as e:
            print("get 请求错误：%s" % e)

    def post(self, url, **kwargs):
        """封装post请求"""
        params = kwargs.get("params")
        data = kwargs.get("data")
        json = kwargs.get("json")
        try:
            result = requests.post(url, params=params, data=data, json=json)
            return result
        except Exception as e:
            print("post 请求错误： %s" % e)

    def run_main(self, method, **kwargs):
        """判断请求方法"""
        if method == 'get':
            result = self.get(**kwargs)
            return result
        elif method == 'post':
            result = self.post(**kwargs)
            return result
        else:
            print('请求方法错误')
