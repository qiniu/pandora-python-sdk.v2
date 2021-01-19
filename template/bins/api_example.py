import sys
import os

# add library to python path , don't forget it
lib_name = 'libs'
sys.path.insert(0, os.path.sep.join([os.path.dirname(os.path.abspath(__file__)), lib_name]))

from pdr_python_sdk.api.on_demand_api import OnDemandApi
from pdr_python_sdk.api.response import Response
from pdr_python_sdk.on_demand_action import run


class ApiExample(OnDemandApi):

    def do_handle_data(self, data):
        """
        TODO: Implement your own business logic

        :param data: http request
        :return:
        """
        if not data.contains_request():
            raise Exception('api data should contain request details')

        request = data.request()
        if 'GET' != str.upper(request.method()):
            return Response(405, {"status": "method is not get"}).to_string()

        return Response(200, {"status": "ok"}).to_string()


if __name__ == '__main__':
    run(ApiExample, sys.argv, sys.stdin.buffer, sys.stdout.buffer)
