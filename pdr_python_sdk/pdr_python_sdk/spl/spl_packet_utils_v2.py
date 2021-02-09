"""
Copyright 2020 Qiniu Cloud (qiniu.com)
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
 http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import json
import sys
import pyarrow as pa
import pandas as pd

TYPE_STRING = 0
TYPE_INT = 1
TYPE_FLOAT = 2
TYPE_NULL = 3
TYPE_BOOLEAN = 4
TYPE_ARRAY = 5


def parse_head(input_stream=sys.stdin.buffer):
    """
    Parse the chunked protocol

    format: chunked 2.0,<meta_length>,<body_length>\n
    encoding utf-8
    """
    try:
        header = input_stream.readline().decode("utf-8")
    except Exception as error:
        raise RuntimeError('Failed to read spl protocol header: {}'.format(error))
    parts = str.split(header, ",")
    meta_length = int(parts[1])
    body_length = int(parts[2])

    return meta_length, body_length


def parse_meta(input_stream=sys.stdin.buffer, length=0):
    """
    Parse the chunked protocol

    format: {}
    encoding utf-8
    """
    try:
        meta_body = input_stream.read(length).decode("utf-8")
        metainfo = json.loads(meta_body)
    except Exception as error:
        raise RuntimeError('Failed to parser spl protocol meta: {}'.format(error))

    return metainfo


def send_packet(output_stream=sys.__stdout__.buffer, meta_info=None, df=None):
    """
    Send data in packets
    """
    if meta_info is None:
        meta_info = {}
    if df is None:
        df = pd.DataFrame()
    meta = json.dumps(meta_info).encode("utf-8")

    # body = convert_body_to_str(lines).encode("utf-8")
    sink = pa.BufferOutputStream()
    batch = pa.record_batch(df)
    writer = pa.ipc.new_stream(sink, batch.schema)
    writer.write_batch(batch)

    head = ('chunked 2.0,%s,%s\n' % (len(meta), len(sink.getvalue().size))).encode("utf-8")
    output_stream.write(head)
    output_stream.write(meta)

    if len(df) > 0:
        output_stream.write(sink.getvalue().to_pybytes())

    output_stream.flush()
    return


def parse_body(input_stream=sys.stdin.buffer, length=0):
    """
    Parse body into records
    """
    buf = pa.py_buffer(input_stream.read(length))
    return pa.ipc.open_stream(buf).read_all()
