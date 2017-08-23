BINS = bin/multihash
MULTIHASH_ROOT = ../
MULTIHASH_CMD = ../multihash

all: deps

deps: bins

clean:
	rm $(BINS)

bins: $(BINS)

bin/multihash: $(MULTIHASH_ROOT)/**/*.go
	go build -o bin/multihash $(MULTIHASH_CMD)

test: test_expensive

test_expensive:
	cd sharness && make TEST_EXPENSIVE=1

test_cheap:
	cd sharness && make

.PHONY: all clean
