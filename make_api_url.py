# coding: utf-8
import uuid
from md5 import md5_string
from timestamp import generate_timestamp
import random


def make_api_url(url, json, openid, appkey):
    timestamp = generate_timestamp()
    json_string = json + '|' + appkey + '|' + timestamp
    digest = md5_string(json_string)
    msg_id = str(random.randint(10000000000, 99999999999))
    api_url = url + '/' + openid + '/' + timestamp + '/' + digest + '/' + msg_id
    return api_url
