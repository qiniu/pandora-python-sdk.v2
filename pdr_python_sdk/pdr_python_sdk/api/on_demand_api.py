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

import sys

from .packet import ApiPacketBody
from .packet import ApiRequestPacket
from .packet import ApiResponsePacket
from ..on_demand_action import OnDemandAction


class OnDemandApi(OnDemandAction):
    """
    One time api which process starts on new request and ends after handling it.
    """

    def __init__(self):
        self.__is_inited = False
        self.__input_stream = sys.stdin.buffer
        self.__output_stream = sys.__stdout__.buffer

    def do_handle_init(self, packet):
        """
        actual initialization actions
        @param ApiRequestPacket packet: packet received from the request
        """
        pass

    def do_handle_data(self, data):
        """
        actual data processing actions
        @param ApiPacketBody data: data block in request
        @rtype: str
        @return: response as a string
        """
        raise NotImplemented('method [do_handle_data(data)] is not yet implemented')

    def do_handle_end(self):
        """
        perform terminating processes
        @return:
        """
        pass

    def handle_init(self, packet):
        """
        load api according to arrived request, for example, setup session, assign env variables, etc.
        @param ApiRequestPacket packet: packet received from the request
        """
        self.do_handle_init(packet)
        self.__is_inited = True

    def handle_data(self, data):
        """
        actions to handle packet body and generate response
        @param ApiPacketBody data: data block in request
        @rtype: str
        @return: response as a string
        """
        return self.do_handle_data(data)

    def handle_end(self):
        """
        perform actions needed after request is handled, for example, release resources, clear thread, etc.
        """
        self.do_handle_end()

    def write(self, data):
        """
        write response to output stream
        @param str data: response string
        """
        self.__output_stream.write(data.encode('utf-8'))

    def write_packet(self, packet):
        """
        write packet to output stream
        @param packet: packet to be writen to output stream
        """
        self.write(packet.to_string())

    def on_request(self, argv=None, input_stream=sys.stdin.buffer, output_stream=sys.__stdout__.buffer):
        """
        default method called when new request arrives.
        in default, this method uses sys.stdin as input stream and sys.stdout as ouput stream
        """
        self.handle_request(argv, input_stream, output_stream)

    def handle_request(self, argv=None, input_stream=sys.stdin.buffer, output_stream=sys.__stdout__.buffer):
        """
        handle request from given input_stream, and response through given output_stream
        @param argv: cmd arguments
        @param input_stream: input_stream to read request
        @param output_stream: output_stream to write response
        """
        self.__input_stream = input_stream
        self.__output_stream = output_stream

        packet = ApiRequestPacket()
        packet.read(self.__input_stream)

        if packet.is_init():
            try:
                if self.__is_inited:
                    raise ApiAlreadyInitedException('current api has already been initialized')
                self.handle_init(packet)
            except:
                self.write('error when handling init request')
                raise
            # write empty message, which means initialization succeeded
            self.write('')

        if packet.contains_data():
            try:
                response = self.handle_data(packet.body())
                self.write_packet(ApiResponsePacket(1, response))
                pass
            except Exception as e:
                self.write('error when handling data : {}'.format(e))
                raise

        if packet.is_end():
            self.handle_end()


class OnDemandApiException(Exception):
    pass


class ApiAlreadyInitedException(OnDemandApiException):
    pass
