# coding:utf-8
from requestAPI import RequestHandler
from make_api_url import make_api_url
from api_params import Params
import json
import pandas as pd
if __name__ == '__main__':
    # 以下是测试代码
    # get请求接口
    # url = 'https://openapi.waiqin365.com/api/store/v1/queryStore/'
    # res = RequestHandler().get(url)
    # post请求接口
    # openid:5641776398931134667
    # appkey:DMVtcNFzbZgFqK03_Y

    url = 'https://openapi.waiqin365.com/api/store/v1/queryStore'
    headers = {
        "Content-Type": "application/json"
    }
    json_object = Params(1)
    json_dict = json_object.__dict__
    data = json.dumps(json_dict)
    api_url = make_api_url(url, data, '5641776398931134667', 'DMVtcNFzbZgFqK03_Y')
    res = RequestHandler().post(api_url, data=data, headers=headers)
    result = json.loads(res.text)
    response_data = result["response_data"]
    df = pd.DataFrame(response_data)
    print(df)
    # df = pd.DataFrame(result)
    # print(df)
    # df = pd.read_json(result)
    # print(df)
