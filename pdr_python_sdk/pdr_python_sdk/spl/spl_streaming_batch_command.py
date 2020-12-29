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

from .spl_packet_utils import *
from .spl_base_command import SplBaseCommand


class SplStreamingBatchCommand(SplBaseCommand):
    def process_data(self, argv=None, input_stream=sys.stdin.buffer, output_stream=sys.__stdout__.buffer):
        while True:
            execute_meta = self.process_protocol_execute(input_stream)
            resp = self.streaming_handle(self.lines)
            send_packet(output_stream, execute_meta, resp)
            self.lines = []
            if self.is_finish:
                break
