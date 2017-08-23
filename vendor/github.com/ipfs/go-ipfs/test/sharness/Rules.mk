include mk/header.mk

SHARNESS_$(d) = $(d)/lib/sharness/sharness.sh

T_$(d) = $(sort $(wildcard $(d)/t[0-9][0-9][0-9][0-9]-*.sh))

DEPS_$(d) := test/bin/random test/bin/multihash test/bin/pollEndpoint \
	   test/bin/iptb test/bin/go-sleep test/bin/random-files \
	   test/bin/go-timeout test/bin/hang-fds test/bin/ma-pipe-unidir
DEPS_$(d) += cmd/ipfs/ipfs
DEPS_$(d) += $(d)/clean-test-results
DEPS_$(d) += $(SHARNESS_$(d))

ifeq ($(OS),Linux)
PLUGINS_DIR_$(d) := $(d)/plugins/
ORGIN_PLUGINS_$(d) := $(plugin/plugins_plugins_so)
PLUGINS_$(d) := $(addprefix $(PLUGINS_DIR_$(d)),$(notdir $(ORGIN_PLUGINS_$(d))))

$(PLUGINS_$(d)): $(ORGIN_PLUGINS_$(d))
	@mkdir -p $(@D)
	cp -f plugin/plugins/$(@F) $@

ifneq ($(TEST_NO_PLUGIN),1)
DEPS_$(d) += $(PLUGINS_$(d))
endif
endif

export MAKE_SKIP_PATH=1

$(T_$(d)): $$(DEPS_$(d)) # use second expansion so coverage can inject dependency
	@echo "*** $@ ***"
	@(cd $(@D) && ./$(@F)) 2>&1
.PHONY: $(T_$(d))

$(d)/aggregate: $(T_$(d))
	@echo "*** $@ ***"
	@(cd $(@D) && ./lib/test-aggregate-results.sh)
.PHONY: $(d)/aggregate

$(d)/clean-test-results:
	rm -rf $(@D)/test-results
.PHONY: $(d)/clean-test-results

CLEAN += $(wildcard $(d)/test-results/*)

$(SHARNESS_$(d)): $(d) ALWAYS
	@clonedir=$(dir $(@D)) $</lib/install-sharness.sh

$(d)/deps: $(SHARNESS_$(d)) $$(DEPS_$(d)) # use second expansion so coverage can inject dependency
.PHONY: $(d)/deps

test_sharness_deps: $(d)/deps
.PHONY: test_sharness_deps

test_sharness_short: $(d)/aggregate
.PHONY: test_sharness_short


test_sharness_expensive: export TEST_EXPENSIVE=1
test_sharness_expensive: test_sharness_short
.PHONY: test_sharness_expensive

test_sharness_race: GOFLAGS += -race
.PHONY: test_sharness_race

TEST += test_sharness_expensive
TEST_SHORT += test_sharness_short


include mk/footer.mk
