from unittest import TestCase

from ..api_example import ApiExample
from pdr_python_sdk.tools import mock_tools


# TODO: implement your own unit test case
class TestApiExample(TestCase):

    def test_get(self):
        http_code, body = mock_tools.mock_api_request(ApiExample, "GET")
        self.assertEqual(http_code, 200)
        self.assertEqual(body, {'status': 'ok'})

    def test_post(self):
        http_code, body = mock_tools.mock_api_request(ApiExample, "POST")
        self.assertEqual(http_code, 405)
        self.assertEqual(body, {'status': 'method is not get'})
