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
import numpy as np
import json
from pdr_python_sdk import client
from pdr_python_sdk import errors


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


class DataManager(object):
    """
    Encapsulate Pandora Client Data SDK
    """

    def __init__(self, conn=None, **kwargs):
        if not conn:
            self.conn = client.connect(**kwargs)
        else:
            self.conn = conn

    def create_repo_if_absent(self, repo_name, **kwargs):
        try:
            self.conn.create_repo(repo_name, **kwargs)
        except errors.BadRequest as err:
            if "仓库 '{}' 已存在".format(repo_name) not in err.args[0]:
                raise err

    def delete_repo_if_exists(self, repo_name):
        try:
            self.conn.delete_repo_by_name(repo_name)
        except errors.NotFound:
            pass

    def save_records_raw_json(self, records=None, try_create_repo=True, **kwargs):
        """
        Save records to pandora via raw json format

        :param records: list of records
        :type records: ``list``
        :param repo: repo name
        :type repo: ``str``
        :param sourcetype: source type name
        :type sourcetype: ``str``
        :param time_field: which field to record the event time, the field should be 13-digit timestamp
        :type time_field: ``str``
        """
        if records is None:
            return
        repo = kwargs.setdefault("repo", "default")
        kwargs.setdefault("sourcetype", "json")
        time_field = kwargs.get("time_field", None)
        data = []
        for r in records:
            d = {"raw": json.dumps(r)}
            if time_field and (time_field in r):
                t = r[time_field]
                if is_legal_timestamp(t):
                    d["timestamp"] = t
            data.append(d)
        if try_create_repo:
            self.create_repo_if_absent(repo, **kwargs)
        return self.conn.data_upload(data, **kwargs)

    def save_pandas_dataframe(self, df=None, **kwargs):
        """
        Save pandas dataframe to pandora
        """
        if df is None:
            return
        records = df.to_dict('records')
        return self.save_records_raw_json(records, **kwargs)

    def save_pandas_dataframe_splits(self, df=None, n=1, **kwargs):
        """
        Save pandas dataframe to pandora in n parts
        """
        if df is None:
            return
        if n <= 1:
            return self.save_pandas_dataframe(df, **kwargs)

        dfs = np.array_split(df, n)
        final_ret = {}
        for d in dfs:
            resp = self.save_pandas_dataframe(d, **kwargs)
            final_ret = merge_result(final_ret, resp)
        return final_ret


def merge_result(left, right):
    return {
        "total": left.get("total", 0) + right.get("total", 0),
        "success": left.get("success", 0) + right.get("success", 0),
        "failure": left.get("failure", 0) + right.get("failure", 0),
        "details": left.get("details", []) + right.get("details", []),
    }


def is_legal_timestamp(t):
    return isinstance(t, int) and 1e12 <= t < 1e13
