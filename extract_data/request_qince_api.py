# coding:utf-8
from requestAPI import RequestHandler
import json
from md5 import md5_string
from datetime import datetime
import random
from typing import Dict, Any, Optional


class Qince_API:
    """
    Handles API requests to the Qince (Waiqin365) platform.

    Attributes:
        uri (str): The base URI for the API.
        url (str): The full URL for the specific API endpoint.
        headers (Dict[str, str]): HTTP headers for the request.
        openid (str): The openid from the configuration.
        appkey (str): The appkey from the configuration.
        body (str): The JSON-encoded request body.
    """

    def __init__(self, path: str, config: Dict[str, Any], body: Dict[str, Any]):
        """
        Initialize the Qince_API instance.

        Args:
            path (str): The API endpoint path (e.g., '/api/userDefined/v1/queryUserDefined').
            config (Dict[str, Any]): Configuration dictionary containing 'openid' and 'appkey'.
            body (Dict[str, Any]): The request body as a dictionary.
        """
        self.uri = 'https://openapi.waiqin365.com'
        self.url = self.uri + path
        self.headers = {"Content-Type": "application/json"}
        self.openid = config.get('openid')
        self.appkey = config.get('appkey')
        self.body = json.dumps(body)

    def generate_timestamp(self) -> str:
        """
        Generate a timestamp string in the format 'YYYYMMDDHHMMSS'.

        Returns:
            str: The current timestamp.
        """
        now = datetime.now()
        date_time = now.strftime("%Y%m%d%H%M%S")
        return date_time

    def make_api_url(self) -> str:
        """
        Construct the full API URL with signature and authentication parameters.

        The URL structure includes timestamp, digest (MD5 signature), and a random message ID.

        Returns:
            str: The fully constructed API URL.
        """
        timestamp = self.generate_timestamp()
        json_string = self.body + '|' + self.appkey + '|' + timestamp
        digest = md5_string(json_string)
        msg_id = str(random.randint(10000000000, 99999999999))
        api_url = self.url + '/' + self.openid + '/' + \
            timestamp + '/' + digest + '/' + msg_id
        return api_url

    def request_data(self) -> Dict[str, Any]:
        """
        Send the POST request to the API and return the JSON response.

        Returns:
            Dict[str, Any]: The JSON response from the API.

        Raises:
            SystemExit: If the API request fails (status code != 200).
            # TODO: Consider raising a custom exception instead of exit() for better robustness.
        """
        api_url = self.make_api_url()
        # print(api_url)
        res = RequestHandler().post(api_url, data=self.body, headers=self.headers)
        if res.status_code == 200:
            return res.json()
        else:
            print(
                f"Error {res.status_code}: Unable to fetch data from the API")
            exit()
