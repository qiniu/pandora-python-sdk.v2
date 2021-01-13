#!/usr/bin/env python

from pdr_python_sdk.on_demand_action import run
from pdr_python_sdk.spl import *


class ReplaceHelloCommand(SplStreamingCommand):
    def init_env_by_getinfo(self):
        self.args = self.metainfo["searchinfo"]["args"]
        if len(self.args) > 0:
            self.key = self.args[0]
        else:
            raise RuntimeError('Empty key! please specify replacehello with key')

    def streaming_handle(self, lines):
        for line in lines:
            if self.key in line:
                line[self.key] = 'hello'
        return lines

if __name__ == "__main__":
    run(ReplaceHelloCommand, sys.argv, sys.stdin.buffer, sys.stdout.buffer)