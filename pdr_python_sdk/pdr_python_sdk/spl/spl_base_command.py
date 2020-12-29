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


import logging
import sys
import traceback

from .spl_packet_utils import *
from ..on_demand_action import OnDemandAction


class SplBaseCommand(OnDemandAction):
    def __init__(self):
        self.uri = 'http://127.0.0.1:9999'
        self.session = ''
        self.metainfo = None
        self.is_finish = False
        self.require_fields = ['*']
        self.export_fields = []
        self.lines = []
        self.spl_args = []

    def on_request(self, argv=None, input_stream=sys.stdin.buffer, output_stream=sys.__stdout__.buffer):
        self.process_protocol(argv, input_stream, output_stream)

    def process_protocol(self, argv=None, input_stream=sys.stdin.buffer, output_stream=sys.__stdout__.buffer):
        try:
            if argv is None:
                argv = sys.argv
            logging.debug('execute script command: {}'.format(argv))

            self.process_protocol_info(input_stream)
            self.init_env_by_getinfo()
            self.metainfo['require_fields'] = self.config_require_fields()
            self.metainfo['export_fields'] = self.config_export_fields()
            send_packet(output_stream, self.metainfo, [])
            self.after_getinfo()
            self.process_data(argv, input_stream, output_stream)
        except Exception as error:
            logging.exception(error)
            self.metainfo['error_message'] = "{}".format(error)
            self.metainfo['error_traceback'] = "{}".format(traceback.format_exc())
            send_packet(output_stream, self.metainfo, [])

    def process_data(self, argv=None, input_stream=sys.stdin.buffer, output_stream=sys.__stdout__.buffer):
        """
        To be implemented
        """
        return

    def process_protocol_info(self, input_stream):
        meta_length, body_length = parse_head(input_stream)
        if meta_length <= 0:
            raise RuntimeError('GetInfo Protocol metaLength is invalid: {}'.format(meta_length))

        self.metainfo = parse_meta(input_stream, meta_length)

        # discard body in getinfo
        parse_body(input_stream, body_length)

    def process_protocol_execute(self, input_stream):
        meta_length, body_length = parse_head(input_stream)
        if meta_length <= 0:
            raise RuntimeError('Execute Protocol metaLength is invalid: {}'.format(meta_length))

        execute_meta = parse_meta(input_stream, meta_length)

        if execute_meta['action'] != "execute":
            raise RuntimeError('Execute Protocol action is invalid: {}'.format(execute_meta['action']))

        self.is_finish = execute_meta['finished']
        tmp = parse_body(input_stream, body_length)
        if len(tmp) > 0:
            self.lines.extend(tmp)
        return execute_meta

    def after_getinfo(self):
        return

    def init_env_by_getinfo(self):
        return

    def config_require_fields(self, require_fields=None):
        if require_fields is None:
            require_fields = []
        if require_fields is None or len(require_fields) == 0:
            require_fields = ['*']
        return require_fields

    def config_export_fields(self, export_fields=None):
        if export_fields is None:
            export_fields = []
        if export_fields is None or len(export_fields) == 0:
            export_fields = ['*']
        return export_fields

    def streaming_handle(self, lines):
        return lines, True
