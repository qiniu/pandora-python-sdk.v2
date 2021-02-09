import unittest
import io
from pdr_python_sdk.spl.spl_packet_utils_v2 import *
import pyarrow as pa
import pandas as pd


class TestClientMethods(unittest.TestCase):

    def test_parse_head(self):
        meta_length, body_length = parse_head(input_stream=io.BytesIO(b"format: chunked 2.0,10,20\n"))
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
        data = [
            pa.array([1, 2, 3, 4])
        ]
        batch = pa.record_batch(data, names=["f0"])
        sink = pa.BufferOutputStream()
        writer = pa.ipc.new_stream(sink, batch.schema)
        writer.write_batch(batch)
        body = sink.getvalue().to_pybytes()

        df = parse_body(io.BytesIO(body), len(body))

    def test_parse_body_bench(self):
        large_data = [
            {
                "cnt": i,
                "_raw": "this is a log, it is a little long, but it's just a string, with no surprise",
                "_time": 1612774891 + i,
                "host": "192.168.1.1",
                "origin": "/path/to/your/log.txt",
                "sourcetype": "test_log",
            } for i in range(1000)
        ]
        large_df = pd.DataFrame.from_records(large_data)
        batch = pa.record_batch(large_df)
        sink = pa.BufferOutputStream()
        writer = pa.ipc.new_stream(sink, batch.schema)
        writer.write_batch(batch)
        body = sink.getvalue().to_pybytes()
        print(f"size is ======== {len(body)}")
        n = 10
        n = 5000
        import pyarrow.compute as pc
        for i in range(n):
            table = parse_body(io.BytesIO(body), len(body))

            # 向量化计算
            newtable = table.append_column("f1", pc.add(table.column("cnt"), 2))

            # 转成pandas dataframe
            # df = table.to_pandas()
