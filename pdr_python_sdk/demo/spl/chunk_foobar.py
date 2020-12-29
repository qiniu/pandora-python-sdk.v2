#!/usr/bin/env python

import sys

from pdr_python_sdk.spl import *
from pdr_python_sdk.on_demand_action import run

class Foobar(SplStreamingChunkCommand):
    def streaming_handle(self, lines):
        for line in lines:
            line['foo'] = 'bar'
        return lines


if __name__ == '__main__':
    run(Foobar, sys.argv, sys.stdin.buffer, sys.__stdout__.buffer)