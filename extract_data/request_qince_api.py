# coding:utf-8
from requestAPI import RequestHandler
import json
from md5 import md5_string
from datetime import datetime
import random


class Qince_API:

    def __init__(self, path, config, body: dict):
        self.uri = 'https://openapi.waiqin365.com'
        self.url = self.uri + path
        self.headers = {"Content-Type": "application/json"}
        self.openid = config.get('openid')
        self.appkey = config.get('appkey')
        self.body = json.dumps(body)

    def generate_timestamp(self):
        now = datetime.now()
        date_time = now.strftime("%Y%m%d%H%M%S")
        return date_time

    def make_api_url(self):
        timestamp = self.generate_timestamp()
        json_string = self.body + '|' + self.appkey + '|' + timestamp
        digest = md5_string(json_string)
        msg_id = str(random.randint(10000000000, 99999999999))
        api_url = self.url + '/' + self.openid + '/' + \
            timestamp + '/' + digest + '/' + msg_id
        return api_url

    def request_data(self):

        api_url = self.make_api_url()
        print(api_url)
        res = RequestHandler().post(api_url, data=self.body, headers=self.headers)
        if res.status_code == 200:
            return res.json()
        else:
            print(
                f"Error {res.status_code}: Unable to fetch data from the API")
            exit()
