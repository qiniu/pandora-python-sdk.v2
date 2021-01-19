#!/usr/bin/env python
import sys
import os

# add library to python path , don't forget it
lib_name = 'libs'
sys.path.insert(0, os.path.sep.join([os.path.dirname(os.path.abspath(__file__)), lib_name]))

from pdr_python_sdk.spl import *
from pdr_python_sdk.on_demand_action import run


class SplExample(SplStreamingBatchCommand):

    def streaming_handle(self, lines):
        """
        TODO: implement your own business logic

        :param lines: list of key-value dict
        :return:
        """
        return self.handle_lines(lines)

    @staticmethod
    def handle_lines(lines):
        for line in lines:
            line['number'] = 100
        return lines


if __name__ == '__main__':
    run(SplExample, sys.argv, sys.stdin.buffer, sys.stdout.buffer)
