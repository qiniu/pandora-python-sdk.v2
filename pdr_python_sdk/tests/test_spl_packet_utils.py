import unittest
import io
from pdr_python_sdk.spl import *


class TestClientMethods(unittest.TestCase):

    def test_parse_head(self):
        meta_length, body_length = parse_head(input_stream=io.BytesIO(b"chunked 1.0,10,20\n"))
        self.assertEqual(meta_length, 10)
        self.assertEqual(body_length, 20)

    def test_parse_meta(self):
        metainfo = {
            "args": ["a=1", "b"],
            "dispatch_dir": "/a/b/c",
            "owner": "abc@def.com",
            "command": "sample a=1 b",
            "app": "YourApp",
            "session_key": "xxxxxxx",
            "username": "def@abc.com",
            "server_uri": "localhost:8080"
        }
        metabytes = json.dumps(metainfo).encode("utf-8")
        ret = parse_meta(io.BytesIO(metabytes), len(metabytes))
        for key in ret:
            self.assertEqual(ret[key], metainfo[key])

    def test_parse_body(self):
        body = b"a\tb\tc\n0,abc\t1,3\t2,4.5\n0,abcd\t1,4\t2,4.6"
        lines = parse_body(io.BytesIO(body), len(body))
        self.assertEqual(lines[0]["a"], "abc")
        self.assertEqual(lines[0]["b"], 3)
        self.assertAlmostEqual(lines[0]["c"], 4.5)

    def test_format_field(self):
        self.assertEqual(format_field("{},abc".format(TYPE_STRING)), "abc")
        self.assertEqual(format_field("{},2".format(TYPE_INT)), 2)
        self.assertAlmostEqual(format_field("{},3.0".format(TYPE_FLOAT)), 3.0)
        self.assertEqual(format_field("{},".format(TYPE_NULL)), "")
        self.assertEqual(format_field("{},true".format(TYPE_BOOLEAN)), True)
        self.assertEqual(format_field("{},false".format(TYPE_BOOLEAN)), False)
        self.assertEqual(format_field("{},[1,2,3,4,5]".format(TYPE_ARRAY)), "[1,2,3,4,5]")

    def test_send_packet(self):
        stream = io.BytesIO()
        metainfo = {
            "args": ["a=1", "b"],
            "dispatch_dir": "/a/b/c",
            "owner": "abc@def.com",
            "command": "sample a=1 b",
            "app": "YourApp",
            "session_key": "xxxxxxx",
            "username": "def@abc.com",
            "server_uri": "localhost:8080"
        }
        lines = [
            {
                "a": 1,
                "b": "ABC"
            },
            {
                "c": 1.0,
                "b": "DEF"
            },
        ]
        send_packet(output_stream=stream, meta_info=metainfo, lines=lines)
        expected_starts = b''.join([b'chunked 1.0,202,20\n{"args": ["a=1", "b"], "dispatch_dir": ',
                                    b'"/a/b/c", "owner": "abc@def.com", "command": "sample a=1 b", ',
                                    b'"app": "YourApp", "session_key": "xxxxxxx", "username": ',
                                    b'"def@abc.com", "server_uri": "localhost:8080"}'])
        self.assertTrue(stream.getvalue().startswith(expected_starts))

    def test_convert_body_to_str(self):
        lines = [
            {
                "a": 1,
                "b": "23"
            },
            {
                "c": 3.0,
                "b": "23"
            }
        ]
        self.assertEqual(len(convert_body_to_str(lines=lines)), len("a\tb\tc\n1\t23\n\t23\t3.0"))

    def test_encode_string(self):
        self.assertEqual(encode_string("\t\n"), "\\t\\n")

    def test_decode_string(self):
        self.assertEqual(decode_string("\\t\\n"), "\t\n")
