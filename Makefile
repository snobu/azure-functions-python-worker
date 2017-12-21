.PHONY: proto

proto:
	python3 -m grpc_tools.protoc -I protos --python_out=gen/ --grpc_python_out=gen/ protos/FunctionRpc.proto
