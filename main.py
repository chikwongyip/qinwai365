# coding:utf-8
from requestAPI import RequestHandler
from make_api_url import  make_api_url
if __name__ == '__main__':
    # 以下是测试代码
    # get请求接口
    url = 'https://api.apiopen.top/getJoke?page=1&count=2&type=video'
    res = RequestHandler().get(url)
    # post请求接口
    url2 = 'http://127.0.0.1:8000/user/login/'
    payload = {
        "username": "vivi",
        "password": "123456"
    }
    res2 = RequestHandler().post(url2, json=payload)
    print(res.json())
    print(res2.json())
