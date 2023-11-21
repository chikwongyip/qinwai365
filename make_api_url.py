# coding: utf-8
import uuid
from md5 import md5_string
from timestamp import generate_timestamp


def make_api_url(url, json, openid):
    digest = md5_string(json)
    timestamp = generate_timestamp()
    msg_id = str(uuid.uuid4())
    api_url = url + '/' + openid + '/' + timestamp + '/' + digest + '/' + msg_id
    return api_url