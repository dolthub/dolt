GOOGLEAPIS := ${HOME}/src/3p/googleapis
PROTOBUF := ${HOME}/src/3p/protobuf/src

PROTOC = protoc
PROTOC_FLAGS = -I . -I $(GOOGLEAPIS) -I $(PROTOBUF)

go_out := ../go/gen/proto

REMOTESAPI_protos := \
  dolt/services/remotesapi/v1alpha1/chunkstore.proto \
  dolt/services/remotesapi/v1alpha1/credentials.proto
REMOTESAPI_go_pkg_path = dolt/services/remotesapi_v1alpha1

GO_pkgs = REMOTESAPI

all:

.PHONY: clean

clean:
	@rm -f $(ALL_OUTPUTS)

define PROTO_template # 1=proto, 2=outputs, 3=flags
$(2) : $(1)
	@$$(PROTOC) $$(PROTOC_FLAGS) $(3) $(1)
ALL_OUTPUTS += $(2)
all: $(2)
endef

define GO_outputs # 1=output base, 2=output pkg path, 3=proto
$(1)/$(2)/$(patsubst %.proto,%.pb.go,$(notdir $(3)))
endef

define GO_template # 1=proto, 2=output pkg path, 3=output base
$(call PROTO_template,$(1),$(call GO_outputs,$(3),$(2),$(1)),--go_out=plugins=grpc:$(3))
endef

$(foreach p,$(GO_pkgs),\
  $(foreach f,$($(p)_protos),\
    $(eval \
      $(call GO_template,$(f),$($(p)_go_pkg_path),$(go_out)))))
