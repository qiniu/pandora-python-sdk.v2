import sys
import os

# add library to python path , don't forget it
lib_name = 'libs'
sys.path.insert(0, os.path.sep.join([os.path.dirname(os.path.abspath(__file__)), lib_name]))

import logging
import json

from pdr_python_sdk.sched_task.on_demand_sched_task import OnDemandSchedTask
from pdr_python_sdk.on_demand_action import run


class SchedTaskExample(OnDemandSchedTask):

    def do_handle_init(self, packet):
        if packet.body().contains_metadata():
            metadata = packet.body().metadata()
            logging.info("metadata = %s", metadata)

    def do_handle_task(self, params):
        logging.info(params)
        return json.dumps({"status": "success", "data": {}, "message": "successful"})


if __name__ == '__main__':
    run(SchedTaskExample, sys.argv, sys.stdin.buffer, sys.stdout.buffer)
