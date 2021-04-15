import sys
import os
import logging

from pdr_python_sdk.trigger_action.on_demand_trigger_action import OnDemandTriggerAction
from pdr_python_sdk.on_demand_action import run

# add library to python path , don't forget it
lib_name = 'libs'
sys.path.insert(0, os.path.sep.join([os.path.dirname(os.path.abspath(__file__)), lib_name]))


class TriggerActionExample(OnDemandTriggerAction):

    def do_handle_init(self, packet):
        """
        :param packet:
        :return:
        """
        if packet.body().contains_metadata():
            metadata = packet.body().metadata()
            logging.info("metadata = %s", metadata)

    def do_handle_events(self, events):
        length = len(events)
        event_name = ""
        event_display_id = ""
        params = {}
        logging.info("event length = {0}".format(length))
        if length > 0:
            event_name = events[0].eventName
            event_display_id = events[0].eventDisplayId
            params = events[0].params

        content= "from hello trigger: eventLength={0}, event_name={1}, " \
               "event_display_id={2}, params={3}".format(length, event_name, event_display_id, params)
        # do send events to other platform
        return content


if __name__ == '__main__':
    run(TriggerActionExample, sys.argv, sys.stdin.buffer, sys.stdout.buffer)
