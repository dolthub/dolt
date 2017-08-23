# Test framework for go-ipfs
#
# Copyright (c) 2014 Christian Couder
# MIT Licensed; see the LICENSE file in this repository.
#
# We are using sharness (https://github.com/mlafeldt/sharness)
# which was extracted from the Git test framework.

# Use the multihash tool to test against

# Add current directory to path, for multihash tool.
PATH=$(pwd)/bin:${PATH}

# Set sharness verbosity. we set the env var directly as
# it's too late to pass in --verbose, and --verbose is harder
# to pass through in some cases.
test "$TEST_VERBOSE" = 1 && verbose=t

# assert the `multihash` we're using is the right one.
if test `which multihash` != $(pwd)/bin/multihash; then
	echo >&2 "Cannot find the tests' local multihash tool."
	echo >&2 "Please check test and multihash tool installation."
	exit 1
fi

SHARNESS_LIB="lib/sharness/sharness.sh"

. "$SHARNESS_LIB" || {
	echo >&2 "Cannot source: $SHARNESS_LIB"
	echo >&2 "Please check Sharness installation."
	exit 1
}

# Please put go-multihash specific shell functions below

for hashbin in sha1sum shasum; do
	if type "$hashbin"; then
		export SHASUMBIN="$hashbin" &&
		test_set_prereq SHASUM &&
		break
	fi
done

