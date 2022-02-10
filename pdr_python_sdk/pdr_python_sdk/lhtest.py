
import sys
import time
import pyarrow as pa
import pandas as pd
import logging
from logging.handlers import RotatingFileHandler

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
    body_length = int(parts[1])

    return body_length      

def parse_body_file():
    '''
    Parse body into records
    '''
    logging.info("parse_body_file")   
    with pa.OSFile('/Users/lihong/Documents/java.arrow', 'rb') as source:
        return pa.ipc.open_file(source).read_all()

def parse_body(input_stream=sys.stdin.buffer):
    '''
    Parse body into records
    '''
    return pa.ipc.open_stream(input_stream).read_all()


def write_body_stream(output_stream=sys.__stdout__.buffer, length=54):
    '''
    Parse body into records
    '''
    data = [
            pa.array([1, 2, 3, 4]),
            pa.array(['foo', 'bar', 'baz', None]),
            pa.array([True, None, False, True])
        ]

    batch = pa.record_batch(data, names=['f0', 'f1', 'f2'])      

    with pa.ipc.new_stream(output_stream, batch.schema) as writer:
        writer.write_batch(batch)
    logging.info("after write_body_stream")



def send_packet(output_stream=sys.__stdout__.buffer, df=None):
    """
    Send data in packets
    """
    schema = pa.Schema.from_pandas(df, preserve_index=False)
    batch = pa.Table.from_pandas(df, preserve_index=False) 

    logging.info("send_packet before new_stream")    
    writer = pa.ipc.new_stream(output_stream, schema)
    writer.write(batch)
    writer.close()
    logging.info("send_packet before output_stream flush")                 
    output_stream.flush()
    logging.info("send_packet stream df size: {}".format(df.size))         

'''
        sink = pa.OSFile('/Users/lihong/Documents/py.arrow', 'wb')
        logging.info("send_packet schema: {}".format(schema))        
        writer = pa.ipc.new_file(sink, schema)
        writer.write(batch)
        writer.close()


        with pa.OSFile('/Users/lihong/Documents/py.arrow', 'rb') as source:
                table = pa.ipc.open_file(source).read_all()
                logging.info("send_packet after parse_body")
                df_new = table.to_pandas()
                logging.info("send_packet df_new: {}".format(df_new)) 
''' 

def config_logging(level=None, filename=None, max_bytes=1024*1024*20, backup_count=5):
    filename = '/Users/lihong/Documents/lh_test.log'
    if level is None:
        level = 'INFO'        

    handler = RotatingFileHandler(filename, maxBytes=max_bytes, backupCount=backup_count)

    logging.basicConfig(level=level,
                            handlers=[handler],
                        format="%(asctime)s %(name)s %(levelname)s %(message)s",
                        datefmt='%Y-%m-%d  %H:%M:%S %a')

def write_file():
    BATCH_SIZE = 2
    NUM_BATCHES = 10
    schema = pa.schema([pa.field('nums', pa.int32())])
    with pa.OSFile('/Users/lihong/Documents/py.arrow', 'wb') as sink:
        with pa.ipc.new_file(sink, schema) as writer:
            for row in range(NUM_BATCHES):
                batch = pa.record_batch([pa.array(range(BATCH_SIZE), type=pa.int32())], schema)
                writer.write(batch)



if __name__ == "__main__":
    try:
        config_logging()
        logging.info("test main")
        table=parse_body()
        logging.info("after parse_body {} bytes {}".format(int(round(time.time() * 1000)), table.nbytes)) 

        df_new = table.to_pandas()
        logging.info("df_new {}".format(df_new.size))                 

        send_packet(sys.__stdout__.buffer, df_new)
    except Exception as ex:
        logging.error("ex: {}".format(ex))
