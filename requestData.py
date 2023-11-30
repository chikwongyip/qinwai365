from requestAPI import RequestHandler
from make_api_url import make_api_url
from api_params import Params
import json


def request_data(url, headers, page_number):
    # url = 'https://openapi.waiqin365.com/api/store/v1/queryStore'
    # headers = {
    #     "Content-Type": "application/json"
    # }
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
    return response_data
