import sys
import unittest

from pdr_python_sdk.api.on_demand_api import OnDemandApi
from pdr_python_sdk.api.response import Response
from pdr_python_sdk.on_demand_action import run


class HelloWorldApi(OnDemandApi):

    def do_handle_data(self, data):
        if not data.contains_request():
            raise Exception('api data should contain request details')
        request = data.request()
        if 'GET' != str.upper(request.method()):
            return Response(405, 'unsupported method [{}]'.format(str.upper(request.method()))).to_string()
        return Response(204, [{"hello": "world"}]).to_string()


class TestClientMethods(unittest.TestCase):

    def test_runner(self):
        """
        Check the runner method
        """
        run(HelloWorldApi, ["/bin/test_api.py app_root/bin/libs", "app_root/bin/libs"], sys.stdin.buffer,
            sys.__stdout__.buffer)


if __name__ == "__main__":
    unittest.main()
