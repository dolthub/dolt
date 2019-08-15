THIRD_PARTY_ROOT := ./third_party

GOOGLEAPIS := $(THIRD_PARTY_ROOT)/googleapis
PROTOBUF := $(THIRD_PARTY_ROOT)/protobuf/src

PROTOC_GEN_GO := third_party/golang-protobuf/._protoc-gen-go
PROTOC := $(THIRD_PARTY_ROOT)/protobuf/bazel-bin/protoc
PROTOC_FLAGS = --plugin=protoc-gen-go=$(PROTOC_GEN_GO) -I . -I $(GOOGLEAPIS) -I $(PROTOBUF)

pbgo_out := ../go/gen/proto

REMOTESAPI_protos := \
  dolt/services/remotesapi/v1alpha1/chunkstore.proto \
  dolt/services/remotesapi/v1alpha1/credentials.proto
REMOTESAPI_pbgo_pkg_path := dolt/services/remotesapi_v1alpha1

PBGO_pkgs := REMOTESAPI

all:

$(PROTOC_GEN_GO):
	@echo "ERROR: Could not find $(PROTOC_GEN_GO)"
	@echo "ERROR: Run "'`'"go build -o ._protoc-gen-go ./protoc-gen-go"'`'" in third_party/golang-protobuf to build protoc-gen-go."
	@exit 1

$(PROTOC):
	@echo "ERROR: Could not find $(PROTOC)"
	@echo "ERROR: Run "'`'"bazel build //:protoc"'`'" in $(THIRD_PARTY_ROOT)/protobuf to build protoc."
	@exit 1

.PHONY: clean

clean:
	@rm -f $(ALL_OUTPUTS)

define PROTOC_template # 1=proto, 2=outputs, 3=flags
$(2) : $(1) $(PROTOC) $(PROTOC_GEN_GO)
	@$$(PROTOC) $$(PROTOC_FLAGS) $(3) $(1)
ALL_OUTPUTS += $(2)
all: $(2)
endef

define PBGO_output # 1=proto
$(patsubst %.proto,%.pb.go,$(1))
endef

define PBGO_template # 1=proto, 2=output pkg path, 3=output base
$(call PROTOC_template,\
  $(1),\
  $(addprefix $(3)/$(2)/,$(notdir $(call PBGO_output,$(1)))),\
	--go_out=plugins=grpc:$(3))
endef

$(foreach p,$(PBGO_pkgs),\
  $(foreach f,$($(p)_protos),\
    $(eval \
      $(call PBGO_template,$(f),$($(p)_pbgo_pkg_path),$(pbgo_out)))))
