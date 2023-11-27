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
    params = Params(1)
    params_dict = params.__dict__
    param_data = json.dumps(params_dict)
    api_url = make_api_url(url, param_data, '5641776398931134667', 'DMVtcNFzbZgFqK03_Y')
    res = RequestHandler().post(api_url, data=param_data, headers=headers)
    if res.status_code == 200:
        result = res.json()
    else:
        print(f"Error {res.status_code}: Unable to fetch data from the API")
        exit()

    response_data = json.loads(result["response_data"])
    df = pd.DataFrame(response_data)
    print(df.loc[[0, 1]].dealers)


    # with open("text.txt", "w") as file:
    #     file.write(result)

    # df = pd.read_json(res.text)
    # print(df.to_string())
    # df = pd.DataFrame(result)
    # print(df)
    # df = pd.read_json(result)
    # print(df)
