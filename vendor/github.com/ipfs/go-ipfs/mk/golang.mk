# golang utilities
GO_MIN_VERSION = 1.8

# pre-definitions
GOCC ?= go
GOTAGS ?=
GOFLAGS ?=
GOTFLAGS ?=

DEPS_GO :=
TEST_GO :=
CHECK_GO :=

go-pkg-name=$(shell $(GOCC) list $(go-tags) github.com/ipfs/go-ipfs/$(1))
go-main-name=$(notdir $(call go-pkg-name,$(1)))$(?exe)
go-curr-pkg-tgt=$(d)/$(call go-main-name,$(d))
go-pkgs-novendor=$(shell $(GOCC) list github.com/ipfs/go-ipfs/... | grep -v /Godeps/)

go-tags=$(if $(GOTAGS), -tags="$(call join-with,$(space),$(GOTAGS))")
go-flags-with-tags=$(GOFLAGS)$(go-tags)

define go-build
$(GOCC) build -i $(go-flags-with-tags) -o "$@" "$(call go-pkg-name,$<)"
endef

test_go_short: GOTFLAGS += -test.short
test_go_short: test_go_expensive
.PHONY: test_go_short

test_go_race: GOTFLAGS += -race
test_go_race: test_go_expensive
.PHONY: test_go_race

test_go_expensive: $$(DEPS_GO)
	$(GOCC) test $(go-flags-with-tags) $(GOTFLAGS) ./...
.PHONY: test_go_expensive
TEST_GO += test_go_expensive

test_go_fmt:
	bin/test-go-fmt
.PHONY: test_go_fmt
TEST_GO += test_go_fmt

test_go_megacheck:
	@$(GOCC) get honnef.co/go/tools/cmd/megacheck
	@for pkg in $(go-pkgs-novendor); do megacheck "$$pkg"; done
.PHONY: megacheck

test_go: $(TEST_GO)

check_go_version:
	bin/check_go_version $(GO_MIN_VERSION)
.PHONY: check_go_version
DEPS_GO += check_go_version

check_go_path:
	bin/check_go_path $(realpath $(shell pwd)) $(realpath $(addsuffix /src/github.com/ipfs/go-ipfs,$(subst $(PATH_SEP),$(space),$(GOPATH))))
.PHONY: check_go_path
DEPS_GO += check_go_path

TEST += $(TEST_GO)
TEST_SHORT += test_go_fmt test_go_short
