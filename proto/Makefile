GOOGLEAPIS := ${HOME}/src/3p/googleapis
PROTOBUF := ${HOME}/src/3p/protobuf/src

go_out := ../go/gen/proto

REMOTESAPI_protos := \
  dolt/services/remotesapi/v1alpha1/chunkstore.proto \
  dolt/services/remotesapi/v1alpha1/credentials.proto
REMOTESAPI_go_pkg_path = dolt/services/remotesapi_v1alpha1

GO_pkgs = REMOTESAPI

all:

.PHONY: clean

clean:
	rm -f $(ALL_OUTPUTS)

define protoc_base
endef

define GO_template # 1=proto, 2=output pkg path, 3=output base
$(call GO_filename,$(3),$(2),$(1)): $(1)
	protoc \
	    -I . \
	    -I $(GOOGLEAPIS) \
	    -I $(PROTOBUF) \
			--go_out=plugins=grpc:$(3) \
	    $(1)
ALL_OUTPUTS += $(call GO_filename,$(3),$(2),$(1))
all: $(call GO_filename,$(3),$(2),$(1))
endef

define GO_filename # 1=output base, 2=output pkg path, 3=proto
$(1)/$(2)/$(patsubst %.proto,%.pb.go,$(notdir $(3)))
endef

$(foreach p,$(GO_pkgs),\
  $(foreach f,$($(p)_protos),\
    $(eval \
      $(call GO_template,$(f),$($(p)_go_pkg_path),$(go_out)))))
