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

TYPE_STRING = 0
TYPE_INT = 1
TYPE_FLOAT = 2
TYPE_NULL = 3
TYPE_BOOLEAN = 4
TYPE_ARRAY = 5


def parse_head(input_stream=sys.stdin.buffer):
    """
    Parse the chunked protocol

    format: chunked 1.0,<meta_length>,<body_length>\n
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


def decode_string(value):
    """
    Decode special character, \t \n
    """
    value = str.replace(value, '\\t', '\t')
    value = str.replace(value, '\\n', '\n')
    return value


def encode_string(value):
    """
    Encode special character, \t \n
    """
    value = str.replace(value, '\t', '\\t')
    value = str.replace(value, '\n', '\\n')
    return value


def convert_body_to_str(lines=[]):
    """
    Encode lines data to string
    """
    if lines is None:
        return ''

    if len(lines) == 0:
        return ''

    field_str = ""
    line_strs = []
    fields = []
    allfields = {}
    for line in lines:
        if not isinstance(line, dict):
            continue
        row = dict(line)
        line_str = ''
        if len(fields) != 0:
            for field in fields:
                if field not in row:
                    line_str += '\t'
                else:
                    line_str = line_str + encode_string(str(row[field])) + '\t'

        for key in row:
            if key in allfields:
                continue
            allfields[key] = 0
            fields.append(key)
            line_str = line_str + encode_string(str(row[key])) + '\t'

        line_strs.append(line_str[:len(line_str) - 1])

    for field in fields:
        field_str = field_str + field + '\t'

    field_str = field_str[:len(field_str) - 1]

    body = field_str
    for line_str in line_strs:
        body += '\n' + line_str

    return body


def format_field(part):
    """
    Parse field data value
    """
    type, value = str.split(part, ',', 1)
    if int(type) == TYPE_STRING:
        return decode_string(value)
    elif int(type) == TYPE_INT:
        return int(value)
    elif int(type) == TYPE_FLOAT:
        return float(value)
    elif int(type) == TYPE_NULL:
        return ''
    elif int(type) == TYPE_BOOLEAN:
        return str.lower(value) == 'true'
    elif int(type) == TYPE_ARRAY:
        return value
    return value


def send_packet(output_stream=sys.__stdout__.buffer, meta_info=None, lines=None):
    """
    Send data in packets
    """
    if meta_info is None:
        meta_info = {}
    if lines is None:
        lines = []
    meta = json.dumps(meta_info).encode("utf-8")

    body = convert_body_to_str(lines).encode("utf-8")

    head = ('chunked 1.0,%s,%s\n' % (len(meta), len(body))).encode("utf-8")
    output_stream.write(head)
    output_stream.write(meta)
    if len(body) > 0:
        output_stream.write(body)

    output_stream.flush()
    return


def parse_body(input_stream=sys.stdin.buffer, length=0):
    """
    Parse body into records
    """
    records = []
    if length <= 0:
        return records

    try:
        body = input_stream.read(length).decode("utf-8")
        rows = str.split(body, '\n')
        if len(rows) < 2:
            return records

        fields = str.split(rows[0], '\t')
        for row in rows[1:]:
            record = {}
            parts = str.split(row, '\t')
            for i in range(len(fields)):
                if i < len(parts):
                    record[fields[i]] = format_field(parts[i])
            records.append(record)

        return records
    except Exception as error:
        raise RuntimeError('Failed to parser spl protocol body: {}'.format(error))

