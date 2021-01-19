from unittest import TestCase
from ..spl_example import SplExample


# TODO: implement your own unit test
class TestSplExample(TestCase):

    def test_handle_lines(self):
        lines = [{"a": 1}, {"a": 2}]
        new_lines = SplExample.handle_lines(lines)
        self.assertEqual(len(new_lines), 2)
        self.assertEqual(new_lines[0]["number"], 100)
