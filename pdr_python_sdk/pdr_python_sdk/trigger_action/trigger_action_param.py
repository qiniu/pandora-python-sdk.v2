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


class TriggerActionParam(object):
    def __init__(self, eventDisplayId, eventId, eventName, eventDescription, eventStatus,
                 eventSubject, alertLevel, eventTime, eventConditionRecord, alertSourceType,
                 alertName, alertDescription, alertDataSource, alertTimeRange, alertInterval,
                 alertExecuteCount, phoenixHost, userName, realUserName, params, additionContents):
        self.eventDisplayId = eventDisplayId
        self.eventId = eventId
        self.eventName = eventName
        self.eventDescription = eventDescription
        self.eventStatus = eventStatus
        self.eventSubject = eventSubject
        self.alertLevel = alertLevel
        self.eventTime = eventTime
        self.eventConditionRecord = eventConditionRecord
        self.alertSourceType = alertSourceType
        self.alertName = alertName
        self.alertDescription = alertDescription
        self.alertDataSource = alertDataSource
        self.alertTimeRange = alertTimeRange
        self.alertInterval = alertInterval
        self.alertExecuteCount = alertExecuteCount
        self.phoenixHost = phoenixHost
        self.userName = userName
        self.realUserName = realUserName
        if params is None:
            self.params = {}
        else:
            self.params = params
        self.additionContents = []
        if additionContents is not None:
            for additionContent in additionContents:
                self.additionContents.append(AdditionContent(**additionContent))


class AdditionContent(object):
    def __init__(self, name, spl, contents):
        self.name = name
        self.spl = spl
        self.contents = contents


def parse_packet_body(body):
    trigger_action_params = []
    for param in json.loads(body):
        trigger_action_params.append(TriggerActionParam(**param))
    return trigger_action_params
