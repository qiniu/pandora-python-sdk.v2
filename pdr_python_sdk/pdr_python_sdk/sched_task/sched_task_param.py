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


class SchedTaskPacketBody(object):
    def __init__(self, metadata=None, params={}):
        self.__metadata = metadata
        self.__params = params

    def metadata(self):
        return self.__metadata

    def params(self):
        return self.__params

    def contains_metadata(self):
        return self.__metadata is not None

    def contains_params(self):
        return self.__params is not None


def parse_packet_body(body):
    return SchedTaskPacketBody(**json.loads(body))
