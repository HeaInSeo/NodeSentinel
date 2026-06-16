.PHONY: proto

PROTOC    ?= protoc
PROTO_SRC ?= ./protos

# ── proto 코드 생성 ───────────────────────────────────────────────────────────
# NodeVault와 동일한 컨벤션: .pb.go / _grpc.pb.go를 .proto와 같은 디렉토리에 생성.
proto:
	$(PROTOC) --proto_path=$(PROTO_SRC) \
	  --go_out=$(PROTO_SRC) --go_opt=paths=source_relative \
	  --go-grpc_out=$(PROTO_SRC) --go-grpc_opt=paths=source_relative \
	  $(shell find $(PROTO_SRC) -name '*.proto')
