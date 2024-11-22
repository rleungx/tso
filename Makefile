# Makefile

# Define variables
PROTOC = protoc
PROTOC_GEN_GO = protoc-gen-go
PROTOC_GEN_GO_GRPC = protoc-gen-go-grpc
PROTO_FILES = $(wildcard proto/*.proto) # All proto files
OUT_DIR = proto # Output directory

# Default target
all: install generate build

# Add build target
build:
	go build -o bin/tso-server main.go

# Add tidy target
tidy:
	go mod tidy

# Generate gRPC code
generate: $(PROTO_FILES)
	$(PROTOC) --go_out=$(OUT_DIR) --go-grpc_out=$(OUT_DIR) $^

# Clean generated files
clean:
	@find $(OUT_DIR) -name '*.go' -exec rm -f {} + 2>/dev/null || true

# Install dependencies
install:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

.PHONY: all build tidy generate clean install
