# default target is to run all tests
all: aggregate

SH := $(wildcard t[0-9][0-9][0-9][0-9]-*.sh)

.DEFAULT $(SH): ALWAYS
	$(MAKE) -C ../.. test/sharness/$@

ALWAYS:
.PHONY: ALWAYS
