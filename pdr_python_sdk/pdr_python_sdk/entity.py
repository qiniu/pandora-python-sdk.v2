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

from pdr_python_sdk.errors import IllegalArgument


class RepoConfigBuilder(object):
    def __init__(self, repo_name):
        self.config = {}
        self.repo_name = repo_name

    def set_desc(self, desc):
        self.config["description"] = desc
        return self

    def set_retention(self, retention):
        self.config["retention"] = int(retention)
        return self

    def enable_lifecycle(self):
        self.config["lifecyclePolicyEnable"] = True
        return self

    def disable_lifecycle(self):
        self.config["lifecyclePolicyEnable"] = False
        return self

    def set_repo_replicas(self, num):
        self.config["repoReplicas"] = num
        return self

    def set_write_replicas(self, num):
        self.config["writeReplicas"] = num
        return self

    def set_write_shards(self, num):
        self.config["writeShards"] = num
        return self

    def set_write_refresh_interval(self, seconds):
        self.config["writeRefreshIntervalInSeconds"] = seconds
        return self

    def init_rollover(self):
        self.config["rollover"] = {}
        return self

    def set_rollover(self, rollover):
        self.config["rollover"] = rollover
        return self

    def init_lifecycle(self):
        self.config["lifecyclePolicy"] = {
            "repoName": self.repo_name,
            "phases": {
                "hot": {
                    "phaseName": "hot"
                },
                "warm": {
                    "phaseName": "warm"
                },
                "cold": {
                    "phaseName": "cold"
                }
            }
        }
        return self

    def set_lifecycle_ages(self, lifecycle):
        self.config["lifecyclePolicy"]["phases"]["hot"]["maxAge"] = lifecycle.get(
            "hot")
        self.config["lifecyclePolicy"]["phases"]["warm"]["maxAge"] = lifecycle.get(
            "warm")
        self.config["lifecyclePolicy"]["phases"]["cold"]["maxAge"] = lifecycle.get(
            "cold")
        return self

    def build(self):
        ok, reason = self.check()
        if not ok:
            raise IllegalArgument(reason)
        return self.config

    """"
    TODO validate the config
    """

    def check(self):
        return check_repo_config(self.config)


def check_repo_config(repo_config):
    if repo_config.get("lifecyclePolicyEnable"):
        if not repo_config.get("lifecyclePolicy"):
            return (False, "lifecyclePolicy should be provided when lifecyclePolicyEnable is True")
        if not repo_config["lifecyclePolicy"]["phases"]["hot"].get("maxAge"):
            return (False, "hot/wam/cold maxAge should be provided when lifecyclePolicyEnable is True")
        if not repo_config["lifecyclePolicy"]["phases"]["warm"].get("maxAge"):
            return (False, "hot/wam/cold maxAge should be provided when lifecyclePolicyEnable is True")
        if not repo_config["lifecyclePolicy"]["phases"]["cold"].get("maxAge"):
            return (False, "hot/wam/cold maxAge should be provided when lifecyclePolicyEnable is True")

    if not repo_config.get("lifecyclePolicyEnable"):
        if "retention" not in repo_config or not repo_config.get("retention"):
            return (False, "retention should be provided when lifecyclePolicyEnable is False")
        if repo_config.get("lifecyclePolicy"):
            return (False, "lifecyclePolicy should not be provided when lifecyclePolicyEnable is False")
    return (True, "")
