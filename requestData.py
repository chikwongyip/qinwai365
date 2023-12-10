from requestAPI import RequestHandler
from make_api_url import make_api_url
from api_params import Params
import json


def request_data(url, headers, page_number):

    params = Params(page_number)
    params_dict = params.__dict__
    param_data = json.dumps(params_dict)
    api_url = make_api_url(url, param_data, '5641776398931134667', 'DMVtcNFzbZgFqK03_Y')
    res = RequestHandler().post(api_url, data=param_data, headers=headers)
    if res.status_code == 200:
        result = json.loads(res.text)
    else:
        print(f"Error {res.status_code}: Unable to fetch data from the API")
        exit()
    print(type(result))
    return result
