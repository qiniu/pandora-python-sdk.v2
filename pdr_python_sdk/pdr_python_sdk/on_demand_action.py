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
import os

from .common.logging import config_logging


class OnDemandAction(object):
    """
    One time action which process starts on new request and ends after handling it.
    """

    def on_request(self, argv=None, input_stream=sys.stdin.buffer, output_stream=sys.__stdout__.buffer):
        """
        method used to handle request.
        """
        raise NotImplemented('method [on_request()] is not yet implemented')


def run(clz, argv=None, input_stream=sys.stdin.buffer, output_stream=sys.__stdout__.buffer):

    # config logging
    config_logging()

    if argv is None:
        argv = sys.argv

    # add $APP_ROOT/bins/libs to path
    app_root_dir = argv[1]
    lib_name = os.path.sep.join([app_root_dir, 'bins', 'libs'])
    sys.path.insert(0, lib_name)

    logging.info('app root dir is: {}'.format(app_root_dir))
    logging.info('running script using class: [' + str(clz) + ']')

    try:
        clz().on_request(argv, input_stream, output_stream)
    except Exception as err:
        logging.exception(err)
