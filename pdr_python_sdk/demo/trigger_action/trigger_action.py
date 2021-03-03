import sys

from pdr_python_sdk.trigger_action.on_demand_trigger_action import OnDemandTriggerAction
from pdr_python_sdk.on_demand_action import run


class TriggerActionExample(OnDemandTriggerAction):

    def do_handle_event(self, events):
        length = 0
        if events is not None:
            length = len(events)
        event_name = ""
        event_display_id = ""
        params = {}
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
