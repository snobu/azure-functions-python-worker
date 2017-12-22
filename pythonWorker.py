#!/usr/bin/env python3

#
# Python 3.x language worker for Azure Functions (v2 / beta / .NET Core runtime)
#

import sys
import trace
import colorama
colorama.init(autoreset=False)
import threading
import time
import argparse
import grpc
from gen import FunctionRpc_pb2
from gen import FunctionRpc_pb2_grpc

class Iterator9000(object):

    def __init__(self):
        self._lock = threading.Lock()
        self._responses_so_far = []
        send_start_stream()

    def __iter__(self):
        return self

    def _next(self):
        # some logic that depends upon what responses have been seen
        # before returning the next request message
        return worker_init_response

    def __next__(self):
        return self._next()

    def add_response(self, response):
        with self._lock:
            self._responses.append(response)


def read_cmdline_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--debug", action="store_true", dest="debug",
                        help="Enable verbose output", default=False)
    parser.add_argument("--host", dest="host", required=True,
                        help="RPC server host")
    parser.add_argument("--port", dest="port", required=True,
                        type=int, help="RPC server port")
    parser.add_argument("--workerId", dest="workerId", required=True,
                        help="RPC server workerId")
    parser.add_argument("--requestId", dest="requestId", required=True,
                        help="Request ID")
    args = parser.parse_args()
    if args.debug:
        print('[PyWORKER] Called with: ' + str(args))

    return args


args = read_cmdline_args()

start_stream_msg = FunctionRpc_pb2.StreamingMessage(
        request_id = args.requestId,
        start_stream = FunctionRpc_pb2.StartStream(worker_id = args.workerId)
)

worker_init_response = FunctionRpc_pb2.StreamingMessage(
        request_id = args.requestId,
        worker_init_response = FunctionRpc_pb2.WorkerInitResponse(result = FunctionRpc_pb2.StatusResult(status = 1))
)


def run():
    conn_str = args.host + ':' + str(args.port)
    channel = grpc.insecure_channel(conn_str)
    try:
        # Our host could be in a different castle,
        # also cloud ravens are slow and raven SLA is poor,
        # so let's wait for some seconds
        grpc.channel_ready_future(channel).result(timeout=5)
    except grpc.FutureTimeoutError:
        sys.exit('[PyWORKER] Error connecting to server')
    else:
        stub = FunctionRpc_pb2_grpc.FunctionRpcStub(channel)
        print('[PyWORKER] Connected to gRPC server.')
    
    req_iterator = Iterator9000()
    responses = stub.EventStream(req_iterator)
    for response in responses:
        print(f'[PyWORKER] Received message: {response}')
        req_iterator.add_response(response)


def send_start_stream():
    yield start_stream_msg # should we yield here or return?? Not even Bitcoin Jesus knows.


if __name__ == '__main__':
    print('[PyWORKER] Starting Python worker...')
    run()