import json
import sys
import unittest

from pdr_python_sdk.tools.mock_tools import mock_sched_task_param
from pdr_python_sdk.on_demand_action import run
from pdr_python_sdk.sched_task.on_demand_sched_task import OnDemandSchedTask


class SchedTaskExample(OnDemandSchedTask):

    def do_handle_task(self, params):
        return json.dumps({"status": "success", "data": params, "message": "successful"})


class TestSchedTaskExample(unittest.TestCase):
    def test_runner(self):
        """
        Check the runner method
        """
        run(SchedTaskExample, ["/bin/test_sched_task.py app_root/bin/libs", "app_root/bin/libs"],
            sys.stdin.buffer,
            sys.__stdout__.buffer)

    def test_mock_sched_task(self):
        params = {
            "key1": "value1"
        }
        result = mock_sched_task_param(SchedTaskExample, params)
        self.assertEqual(result.get("data"), params)


if __name__ == "__main__":
    unittest.main()
