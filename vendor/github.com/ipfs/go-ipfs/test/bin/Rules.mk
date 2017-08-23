include mk/header.mk

TGTS_$(d) :=

$(d)/random: Godeps/_workspace/src/github.com/jbenet/go-random/random
	$(go-build)
TGTS_$(d) += $(d)/random

$(d)/random-files: Godeps/_workspace/src/github.com/jbenet/go-random-files/random-files
	$(go-build)
TGTS_$(d) += $(d)/random-files

$(d)/pollEndpoint: thirdparty/pollEndpoint
	$(go-build)
TGTS_$(d) += $(d)/pollEndpoint

$(d)/go-sleep: test/dependencies/go-sleep
	$(go-build)
TGTS_$(d) += $(d)/go-sleep

$(d)/go-timeout: test/dependencies/go-timeout
	$(go-build)
TGTS_$(d) += $(d)/go-timeout

$(d)/ma-pipe-unidir: test/dependencies/ma-pipe-unidir
	$(go-build)
TGTS_$(d) += $(d)/ma-pipe-unidir

TGTS_GX_$(d) := hang-fds iptb
TGTS_GX_$(d) := $(addprefix $(d)/,$(TGTS_GX_$(d)))

$(TGTS_GX_$(d)):
	go build -i $(go-flags-with-tags) -o "$@" "$(call gx-path,$(notdir $@))"

TGTS_$(d) += $(TGTS_GX_$(d))

# multihash is special
$(d)/multihash:
	go build -i $(go-flags-with-tags) -o "$@" "gx/ipfs/$(shell gx deps find go-multihash)/go-multihash/multihash"
TGTS_$(d) += $(d)/multihash

$(TGTS_$(d)): $$(DEPS_GO)

CLEAN += $(TGTS_$(d))

PATH := $(realpath $(d)):$(PATH)

include mk/footer.mk
