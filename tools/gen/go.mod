module github.com/0xPolygon/sequence-store-proto/tools/gen

go 1.24.0

tool (
	google.golang.org/grpc/cmd/protoc-gen-go-grpc
	google.golang.org/protobuf/cmd/protoc-gen-go
)

require (
	google.golang.org/grpc/cmd/protoc-gen-go-grpc v1.6.0 // indirect
	google.golang.org/protobuf v1.36.10 // indirect
)
