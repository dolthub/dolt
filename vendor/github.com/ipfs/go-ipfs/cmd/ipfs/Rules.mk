include mk/header.mk
IPFS_BIN_$(d) := $(call go-curr-pkg-tgt)

TGT_BIN += $(IPFS_BIN_$(d))
CLEAN += $(IPFS_BIN_$(d))

PATH := $(realpath $(d)):$(PATH)

# disabled for now
# depend on *.pb.go files in the repo as Order Only (as they shouldn't be rebuilt if exist)
# DPES_OO_$(d) := diagnostics/pb/diagnostics.pb.go exchange/bitswap/message/pb/message.pb.go
# DEPS_OO_$(d) += merkledag/pb/merkledag.pb.go namesys/pb/namesys.pb.go
# DEPS_OO_$(d) += pin/internal/pb/header.pb.go unixfs/pb/unixfs.pb.go

$(d)_flags =-ldflags="-X "github.com/ipfs/go-ipfs/repo/config".CurrentCommit=$(shell git rev-parse --short HEAD)" 

$(IPFS_BIN_$(d)): GOFLAGS += $(cmd/ipfs_flags)

# uses second expansion to collect all $(DEPS_GO)
$(IPFS_BIN_$(d)): $(d) $$(DEPS_GO) ALWAYS #| $(DEPS_OO_$(d))
	$(go-build)


$(d)-install: GOFLAGS += $(cmd/ipfs_flags)
$(d)-install: $(d) $$(DEPS_GO) ALWAYS 
	go install $(go-flags-with-tags) ./cmd/ipfs
.PHONY: $(d)-install

COVER_BIN_$(d) := $(d)/ipfs-test-cover
CLEAN += $(COVER_BIN_$(d))

$(COVER_BIN_$(d)): GOTAGS += testrunmain
$(COVER_BIN_$(d)): $(d) $$(DEPS_GO) ALWAYS
	$(eval TMP_PKGS := $(shell go list -f '{{range .Deps}}{{.}} {{end}}' $(go-flags-with-tags) ./cmd/ipfs | sed 's/ /\n/g' | grep ipfs/go-ipfs | grep -v ipfs/go-ipfs/Godeps) $(call go-pkg-name,$<))
	$(eval TMP_LIST := $(call join-with,$(comma),$(TMP_PKGS)))
	@echo go test $@ -c -covermode atomic -coverpkg ... $(go-flags-with-tags) ./$(@D) # for info
	@go test -o $@ -c -covermode atomic -coverpkg $(TMP_LIST) $(go-flags-with-tags) ./$(@D) 2>&1 | (grep -v 'warning: no packages being tested' || true)

include mk/footer.mk
