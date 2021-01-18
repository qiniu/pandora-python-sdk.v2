"""
Copyright 2020 Qiniu Cloud (qiniu.com)
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
 http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import json


class ApiRequestPacket(object):
    """
    packet used to hold api request data
    """

    OPCODE_REQUEST_INIT = 0x01
    OPCODE_REQUEST_DATA = 0x02
    OPCODE_REQUEST_END = 0x04
    OPCODE_REQUEST_ALLOW_STREAM = 0x08

    def __init__(self):
        self.__opcode = None
        self.__body_length = None
        self.__body = None

    def is_init(self):
        """
        returns whether a packet is an init packet
        @rtype: bool
        @return: returns whether a packet is an init packet
        """
        return (self.__opcode & ApiRequestPacket.OPCODE_REQUEST_INIT) != 0

    def is_end(self):
        """
        returns whether a packet is an end packet
        @rtype: bool
        @return: returns whether a packet is an end packet
        """
        return (self.__opcode & ApiRequestPacket.OPCODE_REQUEST_END) != 0

    def contains_data(self):
        """
        returns whether a packet contains data
        @rtype: bool
        @return: returns whether a packet contains data
        """
        return (self.__opcode & ApiRequestPacket.OPCODE_REQUEST_DATA) != 0

    def allow_stream(self):
        """
        returns whether streaming is allowed
        @rtype: bool
        @return: returns whether a packet allows streaming
        """
        return (self.__opcode & ApiRequestPacket.OPCODE_REQUEST_ALLOW_STREAM) != 0

    def opcode(self):
        """
        returns opcode of the packet
        @rtype: int
        @return: packet opcode
        """
        return self.__opcode

    def body_length(self):
        """
        returns packet body length
        @rtype: int
        @return: packet body length
        """
        return self.__body_length

    def body(self):
        """
        returns request received
        @rtype: ApiPacketBody
        @return: request
        """
        return self.__body

    def read(self, input_stream):
        """
        read request packet from input stream
        @param input_stream: input stream that request data will be read from
        """
        opcode_str = input_stream.readline().decode('utf-8')
        if len(opcode_str) <= 0:
            return False
        self.__opcode = int(opcode_str)
        len_str = input_stream.readline().decode("utf-8")
        self.__body_length = int(len_str)
        request_string = input_stream.read(self.__body_length).decode("utf-8")
        self.__body = _parse_body(request_string)

        return True


class ApiResponsePacket(object):
    """
    api response packet contains response opcode and response body
    """

    def __init__(self, opcode, body):
        self.__opcode = opcode
        self.__body = body

    def opcode(self):
        return self.__opcode

    def body_length(self):
        return len(self.__body)

    def body(self):
        return self.__body

    def to_string(self):
        """
        convert a response packet to a string, which
        @return:
        """
        return str(self.opcode()) + '\n' + str(self.body_length()) + '\n' + self.body()


def _parse_body(body):
    params = json.loads(body)
    return ApiPacketBody(**params)


class ApiPacketBody(object):
    def __init__(self, metadata=None, request=None):
        if metadata is None:
            self.__metadata = None
        else:
            self.__metadata = metadata

        if request is None:
            self.__request = None
        else:
            self.__request = ApiRequest(**request)

    def contains_metadata(self):
        return self.__metadata is not None

    def contains_request(self):
        return self.__request is not None

    def metadata(self):
        return self.__metadata

    def request(self):
        return self.__request


class ApiRequest(object):
    def __init__(self, method=None, header=None, param=None, path=None, requestBody=None, uuid=None):
        self.__method = method
        self.__header = header
        self.__param = param
        self.__path = path
        self.__request_body = requestBody
        self.__uuid = uuid

    def method(self):
        return self.__method

    def header(self):
        return self.__header

    def param(self):
        return self.__param

    def path(self):
        return self.__path

    def request_body(self):
        return self.__request_body

    def uuid(self):
        return self.__uuid
