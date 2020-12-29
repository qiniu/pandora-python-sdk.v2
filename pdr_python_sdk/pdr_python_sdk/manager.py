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

import time
import logging
import pandas
from pdr_python_sdk import client


class SearchManager(object):
    """
    Encapsulate Pandora Client Search SDK
    """

    def __init__(self, conn=None, **kwargs):
        if not conn:
            self.conn = client.connect(**kwargs)
        else:
            self.conn = conn

    def query_is_done(self, job_id):
        """
        Check if the query job is done

        :param job_id: query job id
        :return:  ``(Bool, Map)`` the job is done, and the last status
        """
        status = self.conn.get_query_status(job_id)
        if status['process'] == 1:
            return True, status
        else:
            return False, status

    @staticmethod
    def logging_job_status(job_id, status):
        logging.debug("Waiting for job {}, elapsed seconds {}, events scanned {}, result size {}  " \
                      .format(job_id, status['duration'], status['scanSize'], status['resultSize']))

    def query(self, spl, **kwargs):
        """
        Encapsulate spl query API, waiting for query result
        """
        verbose = kwargs.get("verbose", False)
        poll_interval = kwargs.get("poll_interval", 0.1)
        job = self.conn.create_analysis_job(spl, **kwargs)
        job_id = job['id']

        while True:
            done, status = self.query_is_done(job_id)
            if done:
                break

            if verbose:
                self.logging_job_status(job_id, status)
            time.sleep(poll_interval)

        if verbose:
            self.logging_job_status(job_id, status)

        return self.conn.get_query_results(job_id)

    def query_to_pandas(self, spl, **kwargs):
        results = self.query(spl, **kwargs)
        columns = list(map(lambda x: x['name'], results['fields']))
        return pandas.DataFrame.from_records(results['rows'], columns=columns)
