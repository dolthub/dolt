include mk/header.mk

$(d)/coverage_deps: $$(DEPS_GO)
	rm -rf $(@D)/unitcover && mkdir $(@D)/unitcover
	rm -rf $(@D)/sharnesscover && mkdir $(@D)/sharnesscover
ifneq ($(IPFS_SKIP_COVER_BINS),1)
	go get -u github.com/Kubuxu/gocovmerge
	go get -u golang.org/x/tools/cmd/cover
endif
.PHONY: $(d)/coverage_deps

# unit tests coverage
UTESTS_$(d) := $(shell go list -f '{{if (len .TestGoFiles)}}{{.ImportPath}}{{end}}' $(go-flags-with-tags) ./... | grep -v go-ipfs/vendor | grep -v go-ipfs/Godeps)

UCOVER_$(d) := $(addsuffix .coverprofile,$(addprefix $(d)/unitcover/, $(subst /,_,$(UTESTS_$(d)))))

$(UCOVER_$(d)): $(d)/coverage_deps ALWAYS
	$(eval TMP_PKG := $(subst _,/,$(basename $(@F))))
	$(eval TMP_DEPS := $(shell go list -f '{{range .Deps}}{{.}} {{end}}' $(go-flags-with-tags) $(TMP_PKG) | sed 's/ /\n/g' | grep ipfs/go-ipfs | grep -v ipfs/go-ipfs/Godeps) $(TMP_PKG))
	$(eval TMP_DEPS_LIST := $(call join-with,$(comma),$(TMP_DEPS)))
	go test $(go-flags-with-tags) $(GOTFLAGS) -covermode=atomic -coverpkg=$(TMP_DEPS_LIST) -coverprofile=$@ $(TMP_PKG)


$(d)/unit_tests.coverprofile: $(UCOVER_$(d))
	gocovmerge $^ > $@

TGTS_$(d) := $(d)/unit_tests.coverprofile


# sharness tests coverage
$(d)/ipfs: GOTAGS += testrunmain
$(d)/ipfs: $(d)/main
	$(go-build)

CLEAN += $(d)/ipfs

ifneq ($(filter coverage%,$(MAKECMDGOALS)),)
	# this is quite hacky but it is best way I could fiture out
	DEPS_test/sharness += cmd/ipfs/ipfs-test-cover $(d)/coverage_deps $(d)/ipfs
endif

export IPFS_COVER_DIR:= $(realpath $(d))/sharnesscover/

$(d)/sharness_tests.coverprofile: export TEST_NO_PLUGIN=1
$(d)/sharness_tests.coverprofile: $(d)/ipfs cmd/ipfs/ipfs-test-cover $(d)/coverage_deps test_sharness_short
	(cd $(@D)/sharnesscover && find . -type f | gocovmerge -list -) > $@


PATH := $(realpath $(d)):$(PATH)

TGTS_$(d) += $(d)/sharness_tests.coverprofile

CLEAN += $(TGTS_$(d))
COVERAGE += $(TGTS_$(d))

include mk/footer.mk
