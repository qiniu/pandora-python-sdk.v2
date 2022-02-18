import sys
import logging
import time
from logging.handlers import RotatingFileHandler


TYPE_STRING = 0
TYPE_INT = 1
TYPE_FLOAT = 2
TYPE_NULL = 3
TYPE_BOOLEAN = 4
TYPE_ARRAY = 5

def parse_body(input_stream=sys.stdin.buffer, length=76388927):
    """
    Parse body into records
    """
    records = []
    if length <= 0:
        return records

    try:
        body = input_stream.read(length).decode("utf-8")
        logging.info("body len: {}".format(len(body)))
        rows = str.split(body, '\n')
        logging.info("rows len: {}".format(len(rows)))
        if len(rows) < 2:
            return records

        fields = str.split(rows[0], '\t')
        logging.info("fields len: {}".format(len(fields)))
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
    
    
def send_packet(output_stream=sys.__stdout__.buffer, lines=None):
    """
    Send data in packets
    """
    logging.info("lines len: {}".format(len(lines)))
    if lines is None:
        lines = []

    body = convert_body_to_str(lines).encode("utf-8")
    logging.info("after convert_body_to_str body len {}".format(len(body)))     

    if len(body) > 0:
        output_stream.write(body)

    output_stream.flush()
    return


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

def config_logging(level=None, filename=None, max_bytes=1024*1024*20, backup_count=5):
    filename = '/Users/lihong/Documents/custom_lh_test.log'
    if level is None:
        level = 'INFO'        

    handler = RotatingFileHandler(filename, maxBytes=max_bytes, backupCount=backup_count)

    logging.basicConfig(level=level,
                            handlers=[handler],
                        format="%(asctime)s %(name)s %(levelname)s %(message)s",
                        datefmt='%Y-%m-%d  %H:%M:%S %a')
    
    
if __name__ == "__main__":
    try:
        config_logging()
        logging.info("test main")
        records=parse_body()
        logging.info("after parse_body {}".format(int(round(time.time() * 1000)))) 

        send_packet(sys.__stdout__.buffer, records)
    except Exception as ex:
        logging.error("ex: {}".format(ex))
