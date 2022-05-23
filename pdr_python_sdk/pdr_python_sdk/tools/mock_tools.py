import io
import json
import sys

from pdr_python_sdk.api import ApiRequestPacket, HTTP_CODE_KEY, ACTUAL_RESPONSE_KEY, HEADER_KEY
from pdr_python_sdk.on_demand_action import run
from pdr_python_sdk.trigger_action.trigger_action_param import TriggerActionParam


def gen_api_request_packet(opcode, body=""):
    """
    Generate api request packet
    """
    return "{}\n{}\n{}".format(opcode, len(bytearray(body, "utf-8")), body)


def mock_trigger_param(custom_trigger_cls, events=[], metadata=None):
    if events is None or len(events) == 0:
        return {}

    opcode = ApiRequestPacket.OPCODE_REQUEST_INIT | \
             ApiRequestPacket.OPCODE_REQUEST_DATA | \
             ApiRequestPacket.OPCODE_REQUEST_END

    packet = {
        "metadata": metadata,
        "triggerActionParams": events
    }
    body = json.dumps(packet)
    request_packet = gen_api_request_packet(opcode, body)

    in_stream = io.BytesIO(request_packet.encode("utf-8"))
    in_stream.seek(0)

    out_stream = io.BytesIO()
    run(custom_trigger_cls, sys.argv, in_stream, out_stream)
    out_stream.seek(0)

    # opcode
    int(out_stream.readline().decode("utf-8").strip())
    # body_length
    len_str = out_stream.readline().decode("utf-8")
    body_length = int(len_str)
    # response
    response_str = out_stream.read(body_length).decode("utf-8")
    body = json.loads(response_str)

    in_stream.close()
    out_stream.close()
    return body

def mock_sched_task_param(sched_task_cls, params=None, metadata=None):
    if params is None:
        return {}

    opcode = ApiRequestPacket.OPCODE_REQUEST_INIT | \
             ApiRequestPacket.OPCODE_REQUEST_DATA | \
             ApiRequestPacket.OPCODE_REQUEST_END

    packet = {
        "metadata": metadata,
        "params": params
    }
    body = json.dumps(packet)
    request_packet = gen_api_request_packet(opcode, body)

    in_stream = io.BytesIO(request_packet.encode("utf-8"))
    in_stream.seek(0)

    out_stream = io.BytesIO()
    run(sched_task_cls, sys.argv, in_stream, out_stream)
    out_stream.seek(0)

    # opcode
    int(out_stream.readline().decode("utf-8").strip())
    # body_length
    len_str = out_stream.readline().decode("utf-8")
    body_length = int(len_str)
    # response
    response_str = out_stream.read(body_length).decode("utf-8")
    body = json.loads(response_str)

    in_stream.close()
    out_stream.close()
    return body


def mock_api_request(custom_api_cls,
                     method="GET", header={}, param={}, body="", uuid="", path="", metadata=None):
    """
    Mock api request to test custom api

    metadata = {"server_uri": "http://localhost:xxxx/", "session_key":"xxxxx"}
    """
    request = {
        "method": method,
        "path": path,
        "requestBody": body,
        "uuid": uuid,
    }

    if len(param) > 0:
        param["param"] = param
    if len(header) > 0:
        param["header"] = header

    opcode = ApiRequestPacket.OPCODE_REQUEST_INIT | \
             ApiRequestPacket.OPCODE_REQUEST_DATA | \
             ApiRequestPacket.OPCODE_REQUEST_END

    full_request = {"request": request}
    if metadata:
        full_request["metadata"] = metadata

    body = json.dumps(full_request)
    request_packet = gen_api_request_packet(opcode, body)

    in_stream = io.BytesIO(request_packet.encode("utf-8"))
    in_stream.seek(0)

    out_stream = io.BytesIO()
    run(custom_api_cls, sys.argv, in_stream, out_stream)
    out_stream.seek(0)

    # opcode
    int(out_stream.readline().decode("utf-8").strip())
    # body_length
    len_str = out_stream.readline().decode("utf-8")
    body_length = int(len_str)
    # response
    response_str = out_stream.read(body_length).decode("utf-8")
    body = json.loads(response_str)

    in_stream.close()
    out_stream.close()
    return body[HTTP_CODE_KEY], body[ACTUAL_RESPONSE_KEY], body[HEADER_KEY]
