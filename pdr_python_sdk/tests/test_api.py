import sys
import unittest
import json
import io

from pdr_python_sdk.api import ApiRequestPacket
from pdr_python_sdk.api.on_demand_api import OnDemandApi
from pdr_python_sdk.api.response import Response
from pdr_python_sdk.on_demand_action import run
from pdr_python_sdk.tools.mock_tools import mock_api_request, gen_api_request_packet


class HelloWorldApi(OnDemandApi):

    def do_handle_data(self, data):
        if not data.contains_request():
            raise Exception('api data should contain request details')
        if data.contains_request():
            request = data.request()
        if data.contains_metadata():
            metadata = data.metadata()
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

    def test_mock_api(self):
        http_code, response_body = mock_api_request(HelloWorldApi, method="GET")
        self.assertEqual(http_code, 204)
        self.assertEqual(response_body[0]['hello'], "world")

    def test_mock_api_post(self):
        http_code, response_body = mock_api_request(HelloWorldApi, method="POST")
        self.assertEqual(http_code, 405)

    def test_api_request_packet(self):
        arp = ApiRequestPacket()

        request = {
            "method": "GET",
            "path": "path",
            "requestBody": "{'hello':'world'}",
            "uuid": "uuid",
            "param": {"a": 1},
            "header": {"Content-Type": "application/json"}
        }
        metadata = {
            "server_uri": "localhost",
            "session_key": "kkkkkkkk",
        }
        opcode = 7
        body = json.dumps({"metadata": metadata, "request": request})
        body_len = len(body)
        packet = gen_api_request_packet(opcode, body)
        in_stream = io.BytesIO(packet.encode("utf-8"))
        arp.read(in_stream)
        self.assertEqual(arp.allow_stream(), False)
        self.assertEqual(arp.opcode(), opcode)
        self.assertEqual(arp.body_length(), body_len)
        self.assertEqual(arp.body_length(), body_len)
        self.assertEqual(arp.body().metadata()["server_uri"], "localhost")
        reqresult = arp.body().request()
        self.assertEqual(reqresult.param(), {"a": 1})
        self.assertEqual(reqresult.uuid(), "uuid")
        self.assertEqual(reqresult.header(), {"Content-Type": "application/json"})
        self.assertEqual(reqresult.method(), "GET")
        self.assertEqual(reqresult.path(), "path")
        self.assertEqual(reqresult.request_body(), "{'hello':'world'}")


if __name__ == "__main__":
    unittest.main()
