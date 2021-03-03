import json
import sys
import unittest

from pdr_python_sdk.tools.mock_tools import mock_trigger_param
from pdr_python_sdk.on_demand_action import run
from pdr_python_sdk.trigger_action.on_demand_trigger_action import OnDemandTriggerAction

class TriggerActionExample(OnDemandTriggerAction):

    def do_handle_events(self, events):
        length = len(events)
        params = {}
        if length > 0:
            params = events[0].params
        return json.dumps(params)


class TestTriggerAction(unittest.TestCase):
    def test_runner(self):
        """
        Check the runner method
        """
        run(TriggerActionExample, ["/bin/test_trigger_action.py app_root/bin/libs", "app_root/bin/libs"], sys.stdin.buffer,
            sys.__stdout__.buffer)

    def test_mock_trigger_action(self):
        event = {
            "eventDisplayId": "1",
            "eventId": "asdf-1234-dfdfe",
            "eventName": "mock_event1",
            "eventDescription": "this is a mock event",
            "eventStatus": "created",
            "eventSubject": "hostname=localhost",
            "alertLevel": "警告",
            "eventTime": "2021-01-28 12:00:00",
            "eventConditionRecord": "field1:12, field2:13",
            "alertSourceType": "日志告警",
            "alertName": "mock_event1",
            "alertDescription": "this is a mock event",
            "alertDataSource": "|makeresults",
            "alertTimeRange": "从 5 分钟前到 0 分钟前",
            "alertInterval": "每 5 分钟",
            "alertExecuteCount": 1,
            "phoenixHost": "127.0.0.1:9999",
            "userName": "mock_user@qiniu.com",
            "realUserName": "mock_user@qiniu.com",
            "params": {
                "param1": "value1"
            },
            "additionContents": [
                {
                    "name": "附件内容1",
                    "spl": "|makeresults | eval x=10",
                    "contents": [
                        {
                            "x": 10
                        }
                    ]
                }
            ]
        }
        result = mock_trigger_param(TriggerActionExample, [event])
        self.assertEqual(result, {"param1": "value1"})


if __name__ == "__main__":
    unittest.main()
