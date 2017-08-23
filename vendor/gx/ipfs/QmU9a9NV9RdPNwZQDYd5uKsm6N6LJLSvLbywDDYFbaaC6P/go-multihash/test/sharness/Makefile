# Run tests
#
# Copyright (c) 2014 Christian Couder
# MIT Licensed; see the LICENSE file in this repository.
#

# NOTE: run with TEST_VERBOSE=1 for verbose sharness tests.

T = $(sort $(wildcard t[0-9][0-9][0-9][0-9]-*.sh))
BINS = bin/multihash
SHARNESS = lib/sharness/sharness.sh

all: clean deps $(T) aggregate

clean:
	@echo "*** $@ ***"
	-rm -rf test-results

$(T):
	@echo "*** $@ ***"
	./$@

aggregate:
	@echo "*** $@ ***"
	lib/test-aggregate-results.sh

deps: $(SHARNESS) $(BINS)

$(SHARNESS):
	@echo "*** installing $@ ***"
	lib/install-sharness.sh

bin/%:
	@echo "*** installing $@ ***"
	cd .. && make $@

.PHONY: all clean $(T) aggregate
