#!/usr/bin/env python

from pdr_python_sdk.on_demand_action import run
from pdr_python_sdk.spl import *
import os,sys
import time


class GenerateHelloCommand(SplGeneratingCommand):
    def init_env_by_getinfo(self):
        self.args = self.metainfo["searchinfo"]["args"]
        self.count = int(self.args[0]) if (len(self.args) > 0) else 100

    def generate(self):
        lines = [{}] * self.count
        for i in range(1, self.count):
            text = 'Hello World %d' % i
            lines[i]["_time"] = time.time()
            lines[i]["event_no"] = i
            lines[i]["_raw"] = text
        return lines

if __name__ == "__main__":
    run(GenerateHelloCommand, sys.argv, sys.stdin.buffer, sys.stdout.buffer)