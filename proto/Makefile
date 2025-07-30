comma := ,

THIRD_PARTY_ROOT := ./third_party

GOOGLEAPIS := $(THIRD_PARTY_ROOT)/googleapis
PROTOBUF := $(THIRD_PARTY_ROOT)/protobuf/src

PROTOC_GEN_GO := third_party/protobuf-go/._protoc-gen-go
PROTOC_GEN_GO_GRPC := third_party/grpc-go/cmd/protoc-gen-go-grpc/._protoc-gen-go-grpc
PROTOC := $(THIRD_PARTY_ROOT)/protobuf/bazel-bin/protoc
PROTOC_FLAGS = --plugin=protoc-gen-go=$(PROTOC_GEN_GO) --plugin=protoc-gen-go-grpc=$(PROTOC_GEN_GO_GRPC) -I . -I $(GOOGLEAPIS) -I $(PROTOBUF)

pbgo_out := ../go/gen/proto

REMOTESAPI_protos := \
  dolt/services/remotesapi/v1alpha1/chunkstore.proto \
  dolt/services/remotesapi/v1alpha1/credentials.proto
REMOTESAPI_pbgo_pkg_path := dolt/services/remotesapi/v1alpha1

REPLICATIONAPI_protos := \
  dolt/services/replicationapi/v1alpha1/replication.proto
REPLICATIONAPI_pbgo_pkg_path := dolt/services/replicationapi/v1alpha1

nonservice_protos := \
  dolt/services/eventsapi/v1alpha1/event_constants.proto

PBGO_pkgs := \
  CLIENTEVENTS \
  REMOTESAPI \
  REPLICATIONAPI

all:

$(PROTOC_GEN_GO_GRPC):
	@echo "ERROR: Could not find $(PROTOC_GEN_GO_GRPC)"
	@echo "ERROR: Run "'`'"go build -o ._protoc-gen-go-grpc ."'`'" in third_party/grpc-go/cmd/protoc-gen-go-grpc to build protoc-gen-go-grpc."
	@exit 1

$(PROTOC_GEN_GO):
	@echo "ERROR: Could not find $(PROTOC_GEN_GO)"
	@echo "ERROR: Run "'`'"go build -o ._protoc-gen-go ./cmd/protoc-gen-go"'`'" in third_party/protobuf-go to build protoc-gen-go."
	@exit 1

$(PROTOC):
	@echo "ERROR: Could not find $(PROTOC)"
	@echo "ERROR: Run "'`'"bazel build //:protoc"'`'" in $(THIRD_PARTY_ROOT)/protobuf to build protoc."
	@exit 1

.PHONY: clean

clean:
	@rm -f $(ALL_OUTPUTS)

define PROTOC_template # 1=proto, 2=outputs, 3=flags
$(2) : $(1) $(PROTOC) $(PROTOC_GEN_GO) $(PROTOC_GEN_GO_GRPC)
	@$$(PROTOC) $$(PROTOC_FLAGS) $(3) $(1)
ALL_OUTPUTS += $(2)
all: $(2)
endef

define PBGO_output # 1=proto
$(patsubst %.proto,%.pb.go,$(1)) \
$(if $(findstring $(1),$(nonservice_protos)),,$(patsubst %.proto,%_grpc.pb.go,$(1)))
endef

define PBGO_template # 1=proto, 2=output pkg path, 3=output base
$(call PROTOC_template,\
  $(1),\
  $(addprefix $(3)/$(2)/,$(notdir $(call PBGO_output,$(1)))),\
	--go_out=paths=source_relative:$(3) \
        --go-grpc_out=paths=source_relative:$(3))
endef

$(foreach p,$(PBGO_pkgs),\
  $(foreach f,$($(p)_protos),\
    $(eval \
      $(call PBGO_template,$(f),$($(p)_pbgo_pkg_path),$(pbgo_out)))))
