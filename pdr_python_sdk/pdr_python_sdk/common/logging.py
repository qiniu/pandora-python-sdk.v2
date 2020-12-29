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

import os
import logging


def config_logging(level=None, filename=None):
    if filename is None:
        log_dir = '../log'
        if not os.path.exists(log_dir):
            os.mkdir(log_dir)

        filename = '../log/phoenix_app.log'
        if not os.path.exists(filename):
            open(filename, 'w').close()

    if level is None:
        level = 'INFO'

    logging.basicConfig(filename=filename,
                        level=level,
                        format="%(asctime)s %(name)s %(levelname)s %(message)s",
                        datefmt='%Y-%m-%d  %H:%M:%S %a')
