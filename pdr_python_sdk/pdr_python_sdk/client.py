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
import copy
import urllib3
import json
import time
from urllib.parse import urlencode
from pdr_python_sdk.errors import *
from pdr_python_sdk.entity import RepoConfigBuilder

# If you change these, update the docstring
# on _uri as well.
DEFAULT_HOST = "localhost"
DEFAULT_PORT = "8000"
DEFAULT_SCHEME = "http"
API_ROOT_PREFIX = "/api/v1"

# Request Header
DEFAULT_CONTENT_TYPE_KEY = 'Content-Type'
DEFAULT_CONTENT_TYPE_VALUE = 'application/json'
TOKEN_AUTH_HEADER_KEY = "Authorization"
TOKEN_SESSION_HEADER_KEY = "X-App-Session-Key"
DEFAULT_ENCODING = "utf-8"

# Request Path

# Data related
PATH_DATA_UPLOAD = "/data"

# SPL QUERY related
PATH_SPL_QUERY_JOB = "/jobs"
PATH_SPL_QUERY_JOB_STATUS = "/jobs/{}"
PATH_SPL_QUERY_JOB_EVENTS = "/jobs/{}/events"
PATH_SPL_QUERY_JOB_TIMELINE = "/jobs/{}/timeline"
PATH_SPL_QUERY_JOB_SUMMARY = "/jobs/{}/summary"
PATH_SPL_QUERY_JOB_RESULTS = "/jobs/{}/results"
PATH_SPL_QUERY_MAPPING = "/mapping"

# Repo related
PATH_REPOS = "/repos"
PATH_SINGLE_REPO = "/repos/{}"

# sourcetype
PATH_SOURCETYPE = "/sourcetype"
PATH_SINGLE_SOURCETYPE = "/sourcetype/{}"

# APP Upload
APP_IMPORT = "/apps/import"
APP_UNINSTALL = "/apps/{}/uninstall"
APP_ENABLE = "/apps/{}/enable"
APP_DISABLE = "/apps/{}/disable"
APP_CHUNK_SIZE = 5242880


def connect(**kwargs):
    return PandoraConnection(**kwargs)


class PandoraConnection(object):
    """
    Encapsulate connection pool, to make request to pandora server
    """

    def __init__(self, **kwargs):
        """
        Constructor of PandoraConnection, return a connection instance

        :param scheme: http or https
        :type scheme: ``string``
        :param host: the pandora server host
        :type host: ``string``
        :param port: the pandora server port
        :type port: ``integer``
        :param root_path: the pandora api root path, default is /api/v1
        :type root_path: ``string``
        :param token: required if using token to access
        :type token: ``string``
        :param session_key: required if using session_key to access (mostly in app)
        :type session_key: ``string``
        :param headers: additional headers to access
        :type headers: ``map``
        """
        self.scheme = kwargs.get("scheme", DEFAULT_SCHEME)
        self.host = kwargs.get("host", DEFAULT_HOST)
        self.port = kwargs.get("port", None)
        self.root_path = kwargs.get("root_path", API_ROOT_PREFIX)
        if not self.port:
            self.root_url = urllib3.util.Url(
                host=self.host, scheme=self.scheme, path=self.root_path)
        else:
            self.root_url = urllib3.util.Url(
                host=self.host, scheme=self.scheme, port=self.port, path=self.root_path)

        self.token = kwargs.get("token", "")
        self.session_key = kwargs.get("session_key", "")

        self.additional_headers = kwargs.get("headers", {})
        if DEFAULT_CONTENT_TYPE_KEY not in self.additional_headers:
            self.additional_headers[DEFAULT_CONTENT_TYPE_KEY] = DEFAULT_CONTENT_TYPE_VALUE
        if len(self.token) > 0 and TOKEN_AUTH_HEADER_KEY not in self.additional_headers:
            self.additional_headers[TOKEN_AUTH_HEADER_KEY] = self.token
        if len(self.session_key) > 0 and TOKEN_SESSION_HEADER_KEY not in self.additional_headers:
            self.additional_headers[TOKEN_SESSION_HEADER_KEY] = self.session_key
        self.http = urllib3.PoolManager(headers=self.additional_headers)

    def request(self, method, subpath, fields=None, headers=None, **urlopen_kw):
        """
        Base request method, encapsulate `PoolManager.request` basic request.
        """
        resp = self.http.request(
            method, self.root_url.url + subpath, fields=fields, headers=headers, **urlopen_kw)
        if resp.status >= 500:
            raise ServerError("""Server Error Status: {}, Error Reason: {}, Body: {}""".format(
                resp.status, resp.reason, resp.data.decode(DEFAULT_ENCODING)))

        if resp.status == 404:
            raise NotFound("""Not Found Error Status: {}, Error Reason: {}, Body: {}""".format(
                resp.status, resp.reason, resp.data.decode(DEFAULT_ENCODING)))

        if resp.status == 413:
            raise RequestTooLarge("""Request Too Large Status: {}, Error Reason: {}, BodySize: {}""".format(
                resp.status, resp.reason, len(urlopen_kw.get("body", b""))))

        if resp.status >= 400:
            raise BadRequest("""Bad Request Error Status: {}, Error Reason: {}, Body: {}""".format(
                resp.status, resp.reason, resp.data.decode(DEFAULT_ENCODING)))

        return resp

    def request_json(self, method, subpath, fields=None, headers=None, **urlopen_kw):
        resp = self.request(method, subpath, fields=fields,
                            headers=headers, **urlopen_kw)
        return decode_json(resp.data)

    def post(self, subpath, body, fields=None):
        encoded_data = encode_json(body)
        if fields is not None:
            return self.request_json('POST', subpath + '?' + urlencode(fields), body=encoded_data)
        else:
            return self.request_json('POST', subpath, body=encoded_data)

    def get(self, subpath, fields=None):
        return self.request_json('GET', subpath, fields)

    def delete(self, subpath, fields=None):
        return self.request_json('DELETE', subpath, fields)

    def put(self, subpath, body, fields=None):
        encoded_data = encode_json(body)
        if fields is not None:
            return self.request_json('PUT', subpath + '?' + urlencode(fields), body=encoded_data)
        else:
            return self.request_json('PUT', subpath, body=encoded_data)

    def data_upload(self, data=[], **kwargs):
        """
        Data upload function, **kwargs are optional, they are encoded in the url query params

        :param repo: The repo name.
        :type repo: ``string``
        :param host: The host of data comes from.
        :type host: ``string``
        :param sourcetype: The sourcetype name.
        :type sourcetype: ``string``
        :param origin: The data origin, such as source file name etc.
        :type origin: ``string``
        :param collectTime: The collect time of the data
        :type collectTime: ``integer`` of 13 digit integer.
        :returns: response data.
        """
        return self.post(PATH_DATA_UPLOAD, data, fields=kwargs)

    def data_upload_strictly(self, data=[], **kwargs):
        """
        Encapsulate ``data_upload`` function, and check if the data is legal
        """
        repo = kwargs.get("repo", None)
        sourcetype = kwargs.get("sourcetype", None)
        i = 0
        for d in data:
            if 'raw' not in d:
                raise IllegalArgument(
                    "The {}th data does not have raw field".format(i))
            if not repo and 'repo' not in d:
                raise IllegalArgument(
                    "The {}th data does not have repo field".format(i))
            if not sourcetype and 'sourcetype' not in d:
                raise IllegalArgument(
                    "The {}th data does not have sourcetype field".format(i))
            i += 1
        return self.data_upload(data, **kwargs)

    def create_job(self, req_body):
        """
        The lowest level of job creation api. The req_body is like:
        >>>{
        >>>    "query": " repo=default ",
        >>>    "startTime": 0,
        >>>    "endTime": time.time() * 1000,
        >>>    "mode": "fast",
        >>>    "preview": False,
        >>>    "collectSize": 1000,
        >>>    "action": "result"
        >>>}

        :param req_body: the final request body
        :return: query info
        """
        return self.post(PATH_SPL_QUERY_JOB, req_body)

    def create_analysis_job(self, spl, **kwargs):
        """
        Create an analysis job, without events/timeline/summary result.
        This kind of jobs is more efficient and useful for scientific analysis.
        This job should use ``get_query_results`` API to fetch analysis result.
        """
        kwargs["preview"] = False
        return self.create_query_job(spl, **kwargs)

    def create_query_job(self, spl, **kwargs):
        """
        SPL Query API. ``create_query`` create a common spl query job.


        :param spl: The spl query command, such as `* | stats count()`
        :type spl: ``string``
        :param start: The search start time.
        :type start: ``integer`` of 13 digit integer.
        :param end: The search end time.
        :type end: ``integer`` of 13 digit integer.
        :param mode: The searach mode: legal options are ``fast``, ``smart`` and ``detailed``.
        :type mode: ``string``
        :param preview: When preview is True, then return intermediate results even if the query did not finish
        :type preview: ``boolean``
        :param collectSize: Limit the returned data size
        :type collectSize: ``integer``
        :param action: Limit the server background actions
        :type action: ``string``
        :returns: response data.
        """
        if len(spl) <= 0:
            raise IllegalArgument("SPL is required when create query job !")
        query = {
            "query": spl,
            "startTime": int(kwargs.get("start", 0)),
            "endTime": int(kwargs.get("end", time.time() * 1000)),
            "mode": kwargs.get("mode", "fast"),
            "preview": kwargs.get("preview", False),
            "collectSize": int(kwargs.get("collectSize", -1))
        }
        action = kwargs.get("action", None)
        if action:
            query["action"] = action
        return self.create_job(query)

    def get_query_status(self, query_id):
        return self.get(PATH_SPL_QUERY_JOB_STATUS.format(query_id))

    def get_query_results(self, query_id):
        return self.get(PATH_SPL_QUERY_JOB_RESULTS.format(query_id))

    def get_query_events(self, query_id):
        return self.get(PATH_SPL_QUERY_JOB_EVENTS.format(query_id))

    def get_query_timeline(self, query_id):
        return self.get(PATH_SPL_QUERY_JOB_TIMELINE.format(query_id))

    def get_query_summary(self, query_id):
        return self.get(PATH_SPL_QUERY_JOB_SUMMARY.format(query_id))

    def get_query_mapping(self, spl, **kwargs):
        """
        Get query result schema

        :param spl: query
        :param kwargs: start, end of query time
        :return:
        """
        if len(spl) <= 0:
            raise IllegalArgument("SPL is required when get query mapping !")
        return self.get(PATH_SPL_QUERY_MAPPING, fields={
            "query": spl,
            "startTime": kwargs.get("start", 0),
            "endTime": kwargs.get("end", int(time.time() * 1000))
        })

    def create_repo(self, repo_name, **kwargs):
        """
        Repo creation API with validation

        :param repo_name: The repositories' name
        :type repo_name: ``string``
        :param description: The repositories' description
        :type description: ``string``
        :param retention: The data's retention, -1 means forever
        :type retention: ``integer``
        :param lifeCycleEnable: Enable life cycle config
        :type lifeCycleEnable: ``boolean``
        :param lifeCycle: Life cycle policy, map with key hot, warm, cold maxages
        :type lifeCycle: ``map``
        :param rollover: Rollover policy, map with key shardMaxDocs, shardMaxSize, indexMaxAge
        :type rollover: ``map``
        :param replicas: Replicas policy, map with key repoReplicas, writeReplicas, writeShards, writeRefreshIntervalInSeconds
        :type replicas: ``map``
        """
        builder = RepoConfigBuilder(repo_name)
        builder.set_desc(kwargs.get("description", "")) \
            .set_retention(int(kwargs.get("retention", -1)))

        life_cycle_enable = kwargs.get("lifeCycleEnable", False)
        life_cycle = kwargs.get("lifeCycle", {})
        rollover = kwargs.get("rollover", {})
        replicas = kwargs.get("replicas", {})

        if len(rollover) > 0:
            builder.set_rollover(rollover)

        builder.set_repo_replicas(replicas.get("repoReplicas")) \
            .set_write_replicas(replicas.get("writeReplicas")) \
            .set_write_shards(replicas.get("writeShards")) \
            .set_write_refresh_interval(replicas.get("writeRefreshIntervalInSeconds"))

        if life_cycle_enable:
            builder.enable_lifecycle() \
                .init_lifecycle() \
                .set_lifecycle_ages(life_cycle)

        req_body = builder.build()
        return self.create_repo_by_body(repo_name, req_body)

    def get_repos(self, **page_params):
        """
        Get repo list

        :param sort: The sort column, updateTime by default
        :type sort: ``string``
        :param order: The order of data, asc or desc, desc by default
        :type order: ``string``
        :param pageNo: The page no, start from 1
        :type pageNo: ``integer``
        :param pageSize: The size of page, 10 by default
        :type pageSize: ``integer``
        :param prefix: The search keyword of repo name
        :type prefix: ``string``
        """
        return self.get(PATH_REPOS, page_params)

    def get_repo_by_name(self, repo_name):
        return self.get(PATH_SINGLE_REPO.format(repo_name))

    def delete_repo_by_name(self, repo_name):
        return self.delete(PATH_SINGLE_REPO.format(repo_name))

    def create_repo_by_body(self, repo_name, req_body):
        return self.post(PATH_SINGLE_REPO.format(repo_name), req_body)

    def update_repo_by_body(self, repo_name, req_body):
        return self.put(PATH_SINGLE_REPO.format(repo_name), req_body)

    def get_sourcetypes(self, **page_params):
        """
        Get sourcetype list

        :param sort: The sort column, updateTime by default
        :type sort: ``string``
        :param order: The order of data, asc or desc, desc by default
        :type order: ``string``
        :param pageNo: The page no, start from 1
        :type pageNo: ``integer``
        :param pageSize: The size of page, 10 by default
        :type pageSize: ``integer``
        :param prefix: The search keyword of repo name
        :type prefix: ``string``
        """
        return self.get(PATH_SOURCETYPE, page_params)

    def get_sourcetype_by_name(self, sourcetype_name):
        return self.get(PATH_SINGLE_SOURCETYPE.format(sourcetype_name))

    def create_sourcetype_by_body(self, sourcetype_name, req_body):
        return self.post(PATH_SINGLE_SOURCETYPE.format(sourcetype_name), req_body)

    def create_sourcetype(self, sourcetype_name, category="custom", description="", line_type="auto",
                          datetime_type="now", charset="utf-8", **kwargs):
        """
        :param sourcetype_name: The name of sourcetype
        :type sourcetype_name: ``string``
        :param category: the type of sourcetype, must in ["custom", "web", "application", "metrics", "os", "database" "others", "email", "login", "structured"]
        :type category: ``string``
        :param description: The description of sourcetype
        :type description : ``string``
        :param line_type: The line type of data must in ["auto"，"single"，"regex"]
        :type line_type : ``string``
        :param datetime_type: The datetime type of data must in ["auto"，"now"，"custom"]
        :type datetime_type : ``string``
        :param charset: The charset of data
        :type charset : ``string``
        """
        line_breaker = kwargs.get("line_breaker", "\r?\n")
        regex = kwargs.get("regex", "")
        datetime_format = kwargs.get("datetime_format", "")
        zone_id = kwargs.get("zone_id", "Asia/Shanghai")
        datetime_prefix = kwargs.get("datetime_prefix", "")
        max_datetime_length = kwargs.get("max_datetime_length", 100)
        field_discovery = kwargs.get("field_discovery", False)
        app = kwargs.get("app", "search")
        scope = kwargs.get("scope", "global")

        req_body = {
            "line": {
                "type": line_type,
                "lineBreaker": line_breaker,
                "regex": regex
            },
            "datetime": {
                "type": datetime_type,
                "dateTimeFormat": datetime_format,
                "zoneId": zone_id,
                "dateTimePrefix": datetime_prefix,
                "maxDateTimeLength": max_datetime_length
            },
            "description": description,
            "category": category,
            "advance": {
                "charset": charset,
                "fieldDiscovery": field_discovery
            },
            "app": app,
            "scope": scope
        }
        return self.create_sourcetype_by_body(sourcetype_name, req_body)

    def delete_sourcetype_by_name(self, sourcetype_name):
        return self.delete(PATH_SINGLE_SOURCETYPE.format(sourcetype_name))

    def is_exist_sourcetype(self, sourcetype_name):
        if self.get_sourcetype_by_name(sourcetype_name) == {}:
            return False
        return True

    def app_enable(self, app_name):
        """
        Enable App
        """
        return self.request(method="PUT", subpath=APP_ENABLE.format(app_name))

    def app_disable(self, app_name):
        """
        Disable App
        """
        return self.request(method="PUT", subpath=APP_DISABLE.format(app_name))

    def app_uninstall(self, app_name):
        """
        Uninstall App
        """
        return self.request(method="PUT", subpath=APP_UNINSTALL.format(app_name))

    def app_install(self, filename, overwrite=False):
        """
        Import app to Pandora
        """
        if not os.path.exists(filename):
            raise IllegalArgument(f"File not found: {filename}")
        filesize = os.path.getsize(filename)
        absname = os.path.abspath(filename)
        basename = os.path.basename(absname)
        per_chunk_size = APP_CHUNK_SIZE
        chunks, residual = filesize // per_chunk_size, filesize % per_chunk_size
        if residual > 0:
            chunks += 1
        with open(absname, 'rb') as f:
            for i in range(chunks):
                curr_chunk_size = per_chunk_size
                if i == chunks - 1:
                    curr_chunk_size = residual
                params = {
                    "resumableChunkNumber": i + 1,
                    "resumableChunkSize": per_chunk_size,
                    "resumableCurrentChunkSize": curr_chunk_size,
                    "resumableTotalSize": filesize,
                    "resumableType": "application/x-gzip",
                    "resumableIdentifier": f"{filesize}-{basename}",
                    "resumableFilename": basename,
                    "resumableRelativePath": basename,
                    "resumableTotalChunks": chunks,
                    "upgrade": str(overwrite).lower(),
                }
                fields = copy.copy(params)
                content = f.read(curr_chunk_size)
                fields["file"] = (basename, content, 'application/octet-stream')
                self.request(method="POST", subpath=APP_IMPORT + "?" + urlencode(params), fields=fields)
        return True


def encode_json(data):
    return json.dumps(data).encode(DEFAULT_ENCODING)


def decode_json(data_bytes):
    return json.loads(data_bytes.decode(DEFAULT_ENCODING))
