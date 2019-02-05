PROTOC_GEN_TS_PATH := ${HOME}/src/3p/ts-protoc-gen/bazel-bin/bin/protoc-gen-ts
GOOGLEAPIS := ${HOME}/src/3p/googleapis
PROTOBUF := ${HOME}/src/3p/protobuf/src

go_out := ../go/gen/proto

dolt_srcs := \
  services/dolt/v1alpha1/credentials.proto \
  services/dolt/v1alpha1/doltblockstore.proto \

go_files = $(patsubst %.proto, %.pb.go, $(dolt_srcs))

artifacts = \
    $(addprefix $(go_out)/, $(go_files)) \

all: $(artifacts)

$(go_out)/%.pb.go \
: %.proto
	protoc \
	    -I . \
	    -I $(GOOGLEAPIS) \
	    -I $(PROTOBUF) \
	    --go_out=plugins=grpc:$(go_out) \
	    ./$*.proto

.PHONY: clean

clean:
	rm -f $(artifacts)
