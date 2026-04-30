API_DIR = protobuf

gen:
	protoc --proto_path=$(API_DIR)/$* \
	       --go_out=$(API_DIR)/$* --go_opt=paths=source_relative \
	       --go-grpc_out=$(API_DIR)/$* --go-grpc_opt=paths=source_relative \
	       $(API_DIR)/*.proto