gx-path = gx/ipfs/$(shell gx deps find $(1))/$(1)

gx-deps:
	gx install --global
.PHONY: gx-deps

ifneq ($(IPFS_GX_USE_GLOBAL),1)
gx-deps: bin/gx bin/gx-go
endif

DEPS_GO += gx-deps
