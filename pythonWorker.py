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

#-----------------------------------
#def run():
    # args = read_cmdline_args()
    # conn_str = args.host + ':' + str(args.port)
    # channel = grpc.insecure_channel(conn_str)
    # try:
    #     # Our host could be in a different castle,
    #     # also cloud ravens are slow and raven SLA is poor,
    #     # so let's wait for some seconds
    #     grpc.channel_ready_future(channel).result(timeout=5)
    # except grpc.FutureTimeoutError:
    #     sys.exit('[PyWORKER] Error connecting to server')
    # else:
    #     stub = FunctionRpc_pb2_grpc.FunctionRpcStub(channel)
    #     print('[PyWORKER] Connected to gRPC server.')

    # # -- rock solid so far ^

    # def stream():
    #     yield FunctionRpc_pb2.StreamingMessage(
    #         request_id = args.requestId,
    #         start_stream = FunctionRpc_pb2.StartStream(worker_id = args.workerId)
    #     )

    # def handle_worker_init_request():
    #     yield FunctionRpc_pb2.StreamingMessage(
    #         request_id = args.requestId,
    #         worker_init_response = FunctionRpc_pb2.WorkerInitResponse(result = FunctionRpc_pb2.StatusResult(status = 1))
    #     )

    # msg_stream = stub.EventStream(stream())

    # def read_incoming():
    #     rpc_msg = next(msg_stream)
    #     print(f'[PyWORKER] received: {rpc_msg}')
    #     if rpc_msg.worker_init_request:
    #         print('-----------GOT INIT REQ-----------')
    #         stub.EventStream(handle_worker_init_request())

    # thread = threading.Thread(target=read_incoming)
    # thread.daemon = True
    # thread.start()

    # while 1:
    #     time.sleep(1)
#----------------

args = read_cmdline_args()

start_stream_msg = FunctionRpc_pb2.StreamingMessage(
        request_id = args.requestId,
        start_stream = FunctionRpc_pb2.StartStream(worker_id = args.workerId)
)

worker_init_response = FunctionRpc_pb2.StreamingMessage(
        request_id = args.requestId,
        worker_init_response = FunctionRpc_pb2.WorkerInitResponse(result = FunctionRpc_pb2.StatusResult(status = 1))
)

def SomeIterator():
    yield start_stream_msg
    time.sleep(1)
    yield worker_init_response
    while 1:
        time.sleep(1)

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
        send_message(stub)

def send_message(stub):
    #yield start_stream_msg
    responses = stub.EventStream(SomeIterator())
    for response in responses:
        print('[PyWORKER] host_chat - GOT MESSAGE')
        print(f'[PyWORKER] Received message: {response}')
        if response.worker_init_request:
            print(f'Got worker_init_request')
            yield worker_init_response


if __name__ == '__main__':
    print('[PyWORKER] Starting Python worker...')
    run()
